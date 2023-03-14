/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	otrace "go.opencensus.io/trace"

	"github.com/dgraph-io/badger/v3/options"
	"github.com/dgraph-io/badger/v3/pb"
	"github.com/dgraph-io/badger/v3/table"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/dgraph-io/ristretto/z"
	"github.com/pkg/errors"
)

type levelsController struct {
	nextFileID uint64 // Atomic
	l0stallsMs int64  // Atomic

	// The following are initialized once and const.
	levels []*levelHandler
	kv     *DB

	cstatus compactStatus
}

// revertToManifest checks that all necessary table files exist and removes all table files not
// referenced by the manifest. idMap is a set of table file id's that were read from the directory
// listing.
// revertToManifest 检查所有必要的表文件是否存在，并删除所有非清单引用的表文件。idMap 是一组从目录列表中读取的表文件的 ID。
func revertToManifest(kv *DB, mf *Manifest, idMap map[uint64]struct{}) error {
	// 1. Check all files in manifest exist.
	for id := range mf.Tables {
		if _, ok := idMap[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}

	// 2. Delete files that shouldn't exist.
	for id := range idMap {
		if _, ok := mf.Tables[id]; !ok {
			kv.opt.Debugf("Table file %d not referenced in MANIFEST\n", id)
			filename := table.NewFilename(id, kv.opt.Dir)
			if err := os.Remove(filename); err != nil {
				return y.Wrapf(err, "While removing table %d", id)
			}
		}
	}

	return nil
}

func newLevelsController(db *DB, mf *Manifest) (*levelsController, error) {
	y.AssertTrue(db.opt.NumLevelZeroTablesStall > db.opt.NumLevelZeroTables)
	s := &levelsController{
		kv:     db,
		levels: make([]*levelHandler, db.opt.MaxLevels),
	}
	s.cstatus.tables = make(map[uint64]struct{})
	s.cstatus.levels = make([]*levelCompactStatus, db.opt.MaxLevels)

	for i := 0; i < db.opt.MaxLevels; i++ {
		s.levels[i] = newLevelHandler(db, i)
		//new返回的是指针
		s.cstatus.levels[i] = new(levelCompactStatus)
	}

	if db.opt.InMemory {
		return s, nil
	}
	// Compare manifest against directory, check for existent/non-existent files, and remove.
	//// 将MANIFEST与目录进行比较，检查存在/不存在的文件，并删除。
	if err := revertToManifest(db, mf, getIDMap(db.opt.Dir)); err != nil {
		return nil, err
	}

	var mu sync.Mutex
	//  创建一个tables的二维数组
	tables := make([][]*table.Table, db.opt.MaxLevels)
	var maxFileID uint64

	// We found that using 3 goroutines allows disk throughput to be utilized to its max.
	// Disk utilization is the main thing we should focus on, while trying to read the data. That's
	// the one factor that remains constant between HDD and SSD.
	//我们发现，使用3个goroutines可以使磁盘的吞吐量得到最大的利用。
	// 在试图读取数据时，磁盘利用率是我们应该关注的主要问题。这就是HDD和SSD之间保持不变的一个因素。
	throttle := y.NewThrottle(3)

	start := time.Now()
	var numOpened int32
	//　Ticker是一个周期触发定时的计时器，它会按照一个时间间隔往channel发送系统当前时间，而channel的接收者可以以固定的时间间隔从channel中读取事件。
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()
	// 根据manfist里的记录，初始化每一层的table对象
	for fileID, tf := range mf.Tables {
		fname := table.NewFilename(fileID, db.opt.Dir)
		select {
		case <-tick.C:
			db.opt.Infof("%d tables out of %d opened in %s\n", atomic.LoadInt32(&numOpened),
				len(mf.Tables), time.Since(start).Round(time.Millisecond))
		default:
		}
		if err := throttle.Do(); err != nil {
			closeAllTables(tables)
			return nil, err
		}
		if fileID > maxFileID {
			maxFileID = fileID
		}
		go func(fname string, tf TableManifest) {
			var rerr error
			defer func() {
				throttle.Done(rerr)
				atomic.AddInt32(&numOpened, 1)
			}()
			dk, err := db.registry.DataKey(tf.KeyID)
			if err != nil {
				rerr = y.Wrapf(err, "Error while reading datakey")
				return
			}
			topt := buildTableOptions(db)
			// Explicitly set Compression and DataKey based on how the table was generated.
			topt.Compression = tf.Compression
			topt.DataKey = dk

			mf, err := z.OpenMmapFile(fname, db.opt.getFileFlags(), 0)
			if err != nil {
				rerr = y.Wrapf(err, "Opening file: %q", fname)
				return
			}
			t, err := table.OpenTable(mf, topt)
			if err != nil {
				if strings.HasPrefix(err.Error(), "CHECKSUM_MISMATCH:") {
					db.opt.Errorf(err.Error())
					db.opt.Errorf("Ignoring table %s", mf.Fd.Name())
					// Do not set rerr. We will continue without this table.
				} else {
					rerr = y.Wrapf(err, "Opening table: %q", fname)
				}
				return
			}

			mu.Lock()
			tables[tf.Level] = append(tables[tf.Level], t)
			mu.Unlock()
		}(fname, tf)
	}
	if err := throttle.Finish(); err != nil {
		closeAllTables(tables)
		return nil, err
	}
	db.opt.Infof("All %d tables opened in %s\n", atomic.LoadInt32(&numOpened),
		time.Since(start).Round(time.Millisecond))
	s.nextFileID = maxFileID + 1
	// 根据manfist里的记录，初始化每一层的table对象
	// 如果是L0层则按fid排序，否则按每个表中最小key进行排序

	for i, tbls := range tables {
		s.levels[i].initTables(tbls)
	}

	// Make sure key ranges do not overlap etc.
	if err := s.validate(); err != nil {
		_ = s.cleanupLevels()
		return nil, y.Wrap(err, "Level validation")
	}

	// Sync directory (because we have at least removed some files, or previously created the
	// manifest file).
	//同步目录
	if err := syncDir(db.opt.Dir); err != nil {
		_ = s.close()
		return nil, err
	}

	return s, nil
}

// Closes the tables, for cleanup in newLevelsController.  (We Close() instead of using DecrRef()
// because that would delete the underlying files.)  We ignore errors, which is OK because tables
// are read-only.
func closeAllTables(tables [][]*table.Table) {
	for _, tableSlice := range tables {
		for _, table := range tableSlice {
			_ = table.Close(-1)
		}
	}
}

func (s *levelsController) cleanupLevels() error {
	var firstErr error
	for _, l := range s.levels {
		if err := l.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// dropTree picks all tables from all levels, creates a manifest changeset,
// applies it, and then decrements the refs of these tables, which would result
// in their deletion.
func (s *levelsController) dropTree() (int, error) {
	// First pick all tables, so we can create a manifest changelog.
	var all []*table.Table
	for _, l := range s.levels {
		l.RLock()
		all = append(all, l.tables...)
		l.RUnlock()
	}
	if len(all) == 0 {
		return 0, nil
	}

	// Generate the manifest changes.
	changes := []*pb.ManifestChange{}
	for _, table := range all {
		// Add a delete change only if the table is not in memory.
		if !table.IsInmemory {
			changes = append(changes, newDeleteChange(table.ID()))
		}
	}
	changeSet := pb.ManifestChangeSet{Changes: changes}
	if err := s.kv.manifest.addChanges(changeSet.Changes); err != nil {
		return 0, err
	}

	// Now that manifest has been successfully written, we can delete the tables.
	for _, l := range s.levels {
		l.Lock()
		l.totalSize = 0
		l.tables = l.tables[:0]
		l.Unlock()
	}
	for _, table := range all {
		if err := table.DecrRef(); err != nil {
			return 0, err
		}
	}
	return len(all), nil
}

// dropPrefix runs a L0->L1 compaction, and then runs same level compaction on the rest of the
// levels. For L0->L1 compaction, it runs compactions normally, but skips over
// all the keys with the provided prefix.
// For Li->Li compactions, it picks up the tables which would have the prefix. The
// tables who only have keys with this prefix are quickly dropped. The ones which have other keys
// are run through MergeIterator and compacted to create new tables. All the mechanisms of
// compactions apply, i.e. level sizes and MANIFEST are updated as in the normal flow.
func (s *levelsController) dropPrefixes(prefixes [][]byte) error {
	opt := s.kv.opt
	// Iterate levels in the reverse order because if we were to iterate from
	// lower level (say level 0) to a higher level (say level 3) we could have
	// a state in which level 0 is compacted and an older version of a key exists in lower level.
	// At this point, if someone creates an iterator, they would see an old
	// value for a key from lower levels. Iterating in reverse order ensures we
	// drop the oldest data first so that lookups never return stale data.
	for i := len(s.levels) - 1; i >= 0; i-- {
		l := s.levels[i]

		l.RLock()
		if l.level == 0 {
			size := len(l.tables)
			l.RUnlock()

			if size > 0 {
				cp := compactionPriority{
					level: 0,
					score: 1.74,
					// A unique number greater than 1.0 does two things. Helps identify this
					// function in logs, and forces a compaction.
					dropPrefixes: prefixes,
				}
				if err := s.doCompact(174, cp); err != nil {
					opt.Warningf("While compacting level 0: %v", err)
					return nil
				}
			}
			continue
		}

		// Build a list of compaction tableGroups affecting all the prefixes we
		// need to drop. We need to build tableGroups that satisfy the invariant that
		// bottom tables are consecutive.
		// tableGroup contains groups of consecutive tables.
		var tableGroups [][]*table.Table
		var tableGroup []*table.Table

		finishGroup := func() {
			if len(tableGroup) > 0 {
				tableGroups = append(tableGroups, tableGroup)
				tableGroup = nil
			}
		}

		for _, table := range l.tables {
			if containsAnyPrefixes(table, prefixes) {
				tableGroup = append(tableGroup, table)
			} else {
				finishGroup()
			}
		}
		finishGroup()

		l.RUnlock()

		if len(tableGroups) == 0 {
			continue
		}
		_, span := otrace.StartSpan(context.Background(), "Badger.Compaction")
		span.Annotatef(nil, "Compaction level: %v", l.level)
		span.Annotatef(nil, "Drop Prefixes: %v", prefixes)
		defer span.End()
		opt.Infof("Dropping prefix at level %d (%d tableGroups)", l.level, len(tableGroups))
		for _, operation := range tableGroups {
			cd := compactDef{
				span:         span,
				thisLevel:    l,
				nextLevel:    l,
				top:          nil,
				bot:          operation,
				dropPrefixes: prefixes,
				t:            s.levelTargets(),
			}
			cd.t.baseLevel = l.level
			if err := s.runCompactDef(-1, l.level, cd); err != nil {
				opt.Warningf("While running compact def: %+v. Error: %v", cd, err)
				return err
			}
		}
	}
	return nil
}

func (s *levelsController) startCompact(lc *z.Closer) {
	n := s.kv.opt.NumCompactors
	lc.AddRunning(n - 1)
	//  1. 启动4个协程进行压缩
	for i := 0; i < n; i++ {
		go s.runCompactor(i, lc)
	}
}

type targets struct {
	baseLevel int
	targetSz  []int64
	fileSz    []int64
}

// levelTargets calculates the targets for levels in the LSM tree. The idea comes from Dynamic Level
// Sizes ( https://rocksdb.org/blog/2015/07/23/dynamic-level.html ) in RocksDB. The sizes of levels
// are calculated based on the size of the lowest level, typically L6. So, if L6 size is 1GB, then
// L5 target size is 100MB, L4 target size is 10MB and so on.
//
// L0 files don't automatically go to L1. Instead, they get compacted to Lbase, where Lbase is
// chosen based on the first level which is non-empty from top (check L1 through L6). For an empty
// DB, that would be L6.  So, L0 compactions go to L6, then L5, L4 and so on.
//
// Lbase is advanced to the upper levels when its target size exceeds BaseLevelSize. For
// example, when L6 reaches 1.1GB, then L4 target sizes becomes 11MB, thus exceeding the
// BaseLevelSize of 10MB. L3 would then become the new Lbase, with a target size of 1MB <
// BaseLevelSize.
//levelTargets计算LSM树中各层的目标。这个想法来自于RocksDB中的Dynamic Level Sizes ( https://rocksdb.org/blog/2015/07/23/dynamic-level.html )。层的大小是根据最低层的大小计算的，通常是L6。所以，如果L6的大小是1GB，那么L5的目标大小是100MB，L4的目标大小是10MB，以此类推。
//
// L0文件不会自动转到L1。相反，它们被压缩到Lbase，其中Lbase的选择是基于从顶部开始非空的第一层（检查L1到L6）。对于一个空的数据库，这将是L6。 所以，L0压缩到L6，然后是L5，L4，以此类推。
//
//当Lbase的目标大小超过BaseLevelSize时，它就会被推进到上层。例如，当L6达到1.1GB时，那么L4的目标大小就会变成11MB，从而超过BaseLevelSize的10MB。然后，L3将成为新的Lbase，其目标大小为1MB < BaseLevelSize。
func (s *levelsController) levelTargets() targets {
	adjust := func(sz int64) int64 {
		if sz < s.kv.opt.BaseLevelSize {
			return s.kv.opt.BaseLevelSize
		}
		return sz
	}

	t := targets{
		targetSz: make([]int64, len(s.levels)),
		fileSz:   make([]int64, len(s.levels)),
	}
	// DB size is the size of the last level.
	dbSize := s.lastLevel().getTotalSize()
	for i := len(s.levels) - 1; i > 0; i-- {
		ltarget := adjust(dbSize)
		t.targetSz[i] = ltarget
		if t.baseLevel == 0 && ltarget <= s.kv.opt.BaseLevelSize {
			t.baseLevel = i
		}
		dbSize /= int64(s.kv.opt.LevelSizeMultiplier)
	}

	tsz := s.kv.opt.BaseTableSize
	for i := 0; i < len(s.levels); i++ {
		if i == 0 {
			// Use MemTableSize for Level 0. Because at Level 0, we stop compactions based on the
			// number of tables, not the size of the level. So, having a 1:1 size ratio between
			// memtable size and the size of L0 files is better than churning out 32 files per
			// memtable (assuming 64MB MemTableSize and 2MB BaseTableSize).
			t.fileSz[i] = s.kv.opt.MemTableSize
		} else if i <= t.baseLevel {
			t.fileSz[i] = tsz
		} else {
			tsz *= int64(s.kv.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}

	// Bring the base level down to the last empty level.
	for i := t.baseLevel + 1; i < len(s.levels)-1; i++ {
		if s.levels[i].getTotalSize() > 0 {
			break
		}
		t.baseLevel = i
	}

	// If the base level is empty and the next level size is less than the
	// target size, pick the next level as the base level.
	b := t.baseLevel
	lvl := s.levels
	if b < len(lvl)-1 && lvl[b].getTotalSize() == 0 && lvl[b+1].getTotalSize() < t.targetSz[b+1] {
		t.baseLevel++
	}
	return t
}

func (s *levelsController) runCompactor(id int, lc *z.Closer) {
	defer lc.Done()
	//1. 随机延迟1s执行
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	case <-lc.HasBeenClosed():
		randomDelay.Stop()
		return
	}

	moveL0toFront := func(prios []compactionPriority) []compactionPriority {
		idx := -1
		for i, p := range prios {
			if p.level == 0 {
				idx = i
				break
			}
		}
		// If idx == -1, we didn't find L0.
		// If idx == 0, then we don't need to do anything. L0 is already at the front.
		if idx > 0 {
			out := append([]compactionPriority{}, prios[idx])
			out = append(out, prios[:idx]...)
			out = append(out, prios[idx+1:]...)
			return out
		}
		return prios
	}

	run := func(p compactionPriority) bool {
		err := s.doCompact(id, p)
		switch err {
		case nil:
			return true
		case errFillTables:
			// pass
		default:
			s.kv.opt.Warningf("While running doCompact: %v\n", err)
		}
		return false
	}
	runOnce := func() bool {
		prios := s.pickCompactLevels()
		if id == 0 {
			// Worker ID zero prefers to compact L0 always.
			//工作者ID为0的人更愿意永远压缩L0。
			prios = moveL0toFront(prios)
		}
		for _, p := range prios {
			if id == 0 && p.level == 0 {
				// Allow worker zero to run level 0, irrespective of its adjusted score.
			} else if p.adjusted < 1.0 {
				break
			}
			if run(p) {
				return true
			}
		}

		return false
	}

	tryLmaxToLmaxCompaction := func() {
		p := compactionPriority{
			level: s.lastLevel().level,
			t:     s.levelTargets(),
		}
		run(p)

	}
	count := 0
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		// Can add a done channel or other stuff.
		case <-ticker.C:
			count++
			// Each ticker is 50ms so 50*200=10seconds.
			//    2. 每10s 2号协程序 执行一次
			if s.kv.opt.LmaxCompaction && id == 2 && count >= 200 {
				tryLmaxToLmaxCompaction()
				count = 0
			} else {
				runOnce()
			}
		case <-lc.HasBeenClosed():
			return
		}
	}
}

type compactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	t            targets
}

func (s *levelsController) lastLevel() *levelHandler {
	return s.levels[len(s.levels)-1]
}

// pickCompactLevel determines which level to compact.
// Based on: https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
func (s *levelsController) pickCompactLevels() (prios []compactionPriority) {
	t := s.levelTargets()
	addPriority := func(level int, score float64) {
		pri := compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			t:        t,
		}
		prios = append(prios, pri)
	}

	// Add L0 priority based on the number of tables.
	addPriority(0, float64(s.levels[0].numTables())/float64(s.kv.opt.NumLevelZeroTables))

	// All other levels use size to calculate priority.
	for i := 1; i < len(s.levels); i++ {
		// Don't consider those tables that are already being compacted right now.
		delSize := s.cstatus.delSize(i)

		l := s.levels[i]
		sz := l.getTotalSize() - delSize
		addPriority(i, float64(sz)/float64(t.targetSz[i]))
	}
	y.AssertTrue(len(prios) == len(s.levels))

	// The following code is borrowed from PebbleDB and results in healthier LSM tree structure.
	// If Li-1 has score > 1.0, then we'll divide Li-1 score by Li. If Li score is >= 1.0, then Li-1
	// score is reduced, which means we'll prioritize the compaction of lower levels (L5, L4 and so
	// on) over the higher levels (L0, L1 and so on). On the other hand, if Li score is < 1.0, then
	// we'll increase the priority of Li-1.
	// Overall what this means is, if the bottom level is already overflowing, then de-prioritize
	// compaction of the above level. If the bottom level is not full, then increase the priority of
	// above level.
	var prevLevel int
	for level := t.baseLevel; level < len(s.levels); level++ {
		if prios[prevLevel].adjusted >= 1 {
			// Avoid absurdly large scores by placing a floor on the score that we'll
			// adjust a level by. The value of 0.01 was chosen somewhat arbitrarily
			const minScore = 0.01
			if prios[level].score >= minScore {
				prios[prevLevel].adjusted /= prios[level].adjusted
			} else {
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	// Pick all the levels whose original score is >= 1.0, irrespective of their adjusted score.
	// We'll still sort them by their adjusted score below. Having both these scores allows us to
	// make better decisions about compacting L0. If we see a score >= 1.0, we can do L0->L0
	// compactions. If the adjusted score >= 1.0, then we can do L0->Lbase compactions.
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	// Sort by the adjusted score.
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}

// checkOverlap checks if the given tables overlap with any level from the given "lev" onwards.
func (s *levelsController) checkOverlap(tables []*table.Table, lev int) bool {
	kr := getKeyRange(tables...)
	for i, lh := range s.levels {
		if i < lev { // Skip upper levels.
			continue
		}
		lh.RLock()
		left, right := lh.overlappingTables(levelHandlerRLocked{}, kr)
		lh.RUnlock()
		if right-left > 0 {
			return true
		}
	}
	return false
}

// subcompact runs a single sub-compaction, iterating over the specified key-range only.
//
// We use splits to do a single compaction concurrently. If we have >= 3 tables
// involved in the bottom level during compaction, we choose key ranges to
// split the main compaction up into sub-compactions. Each sub-compaction runs
// concurrently, only iterating over the provided key range, generating tables.
// This speeds up the compaction significantly.
func (s *levelsController) subcompact(it y.Iterator, kr keyRange, cd compactDef,
	inflightBuilders *y.Throttle, res chan<- *table.Table) {

	// Check overlap of the top level with the levels which are not being
	// compacted in this compaction.
	hasOverlap := s.checkOverlap(cd.allTables(), cd.nextLevel.level+1)

	// Pick a discard ts, so we can discard versions below this ts. We should
	// never discard any versions starting from above this timestamp, because
	// that would affect the snapshot view guarantee provided by transactions.
	discardTs := s.kv.orc.discardAtOrBelow()

	// Try to collect stats so that we can inform value log about GC. That would help us find which
	// value log file should be GCed.
	discardStats := make(map[uint32]int64)
	updateStats := func(vs y.ValueStruct) {
		// We don't need to store/update discard stats when badger is running in Disk-less mode.
		if s.kv.opt.InMemory {
			return
		}
		if vs.Meta&bitValuePointer > 0 {
			var vp valuePointer
			vp.Decode(vs.Value)
			discardStats[vp.Fid] += int64(vp.Len)
		}
	}

	// exceedsAllowedOverlap returns true if the given key range would overlap with more than 10
	// tables from level below nextLevel (nextLevel+1). This helps avoid generating tables at Li
	// with huge overlaps with Li+1.
	exceedsAllowedOverlap := func(kr keyRange) bool {
		n2n := cd.nextLevel.level + 1
		if n2n <= 1 || n2n >= len(s.levels) {
			return false
		}
		n2nl := s.levels[n2n]
		n2nl.RLock()
		defer n2nl.RUnlock()

		l, r := n2nl.overlappingTables(levelHandlerRLocked{}, kr)
		return r-l >= 10
	}

	var (
		lastKey, skipKey       []byte
		numBuilds, numVersions int
		// Denotes if the first key is a series of duplicate keys had
		// "DiscardEarlierVersions" set
		firstKeyHasDiscardSet bool
	)

	addKeys := func(builder *table.Builder) {
		timeStart := time.Now()
		var numKeys, numSkips uint64
		var rangeCheck int
		var tableKr keyRange
		for ; it.Valid(); it.Next() {
			// See if we need to skip the prefix.
			if len(cd.dropPrefixes) > 0 && hasAnyPrefixes(it.Key(), cd.dropPrefixes) {
				numSkips++
				updateStats(it.Value())
				continue
			}

			// See if we need to skip this key.
			if len(skipKey) > 0 {
				if y.SameKey(it.Key(), skipKey) {
					numSkips++
					updateStats(it.Value())
					continue
				} else {
					skipKey = skipKey[:0]
				}
			}

			if !y.SameKey(it.Key(), lastKey) {
				firstKeyHasDiscardSet = false
				if len(kr.right) > 0 && y.CompareKeys(it.Key(), kr.right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					// Only break if we are on a different key, and have reached capacity. We want
					// to ensure that all versions of the key are stored in the same sstable, and
					// not divided across multiple tables at the same level.
					break
				}
				lastKey = y.SafeCopy(lastKey, it.Key())
				numVersions = 0
				firstKeyHasDiscardSet = it.Value().Meta&BitDiscardEarlierVersions > 0

				if len(tableKr.left) == 0 {
					tableKr.left = y.SafeCopy(tableKr.left, it.Key())
				}
				tableKr.right = lastKey

				rangeCheck++
				if rangeCheck%5000 == 0 {
					// This table's range exceeds the allowed range overlap with the level after
					// next. So, we stop writing to this table. If we don't do this, then we end up
					// doing very expensive compactions involving too many tables. To amortize the
					// cost of this check, we do it only every N keys.
					if exceedsAllowedOverlap(tableKr) {
						// s.kv.opt.Debugf("L%d -> L%d Breaking due to exceedsAllowedOverlap with
						// kr: %s\n", cd.thisLevel.level, cd.nextLevel.level, tableKr)
						break
					}
				}
			}

			vs := it.Value()
			version := y.ParseTs(it.Key())

			isExpired := isDeletedOrExpired(vs.Meta, vs.ExpiresAt)

			// Do not discard entries inserted by merge operator. These entries will be
			// discarded once they're merged
			if version <= discardTs && vs.Meta&bitMergeEntry == 0 {
				// Keep track of the number of versions encountered for this key. Only consider the
				// versions which are below the minReadTs, otherwise, we might end up discarding the
				// only valid version for a running transaction.
				numVersions++
				// Keep the current version and discard all the next versions if
				// - The `discardEarlierVersions` bit is set OR
				// - We've already processed `NumVersionsToKeep` number of versions
				// (including the current item being processed)
				lastValidVersion := vs.Meta&BitDiscardEarlierVersions > 0 ||
					numVersions == s.kv.opt.NumVersionsToKeep

				if isExpired || lastValidVersion {
					// If this version of the key is deleted or expired, skip all the rest of the
					// versions. Ensure that we're only removing versions below readTs.
					skipKey = y.SafeCopy(skipKey, it.Key())

					switch {
					// Add the key to the table only if it has not expired.
					// We don't want to add the deleted/expired keys.
					case !isExpired && lastValidVersion:
						// Add this key. We have set skipKey, so the following key versions
						// would be skipped.
					case hasOverlap:
						// If this key range has overlap with lower levels, then keep the deletion
						// marker with the latest version, discarding the rest. We have set skipKey,
						// so the following key versions would be skipped.
					default:
						// If no overlap, we can skip all the versions, by continuing here.
						numSkips++
						updateStats(vs)
						continue // Skip adding this key.
					}
				}
			}
			numKeys++
			var vp valuePointer
			if vs.Meta&bitValuePointer > 0 {
				vp.Decode(vs.Value)
			}
			switch {
			case firstKeyHasDiscardSet:
				// This key is same as the last key which had "DiscardEarlierVersions" set. The
				// the next compactions will drop this key if its ts >
				// discardTs (of the next compaction).
				builder.AddStaleKey(it.Key(), vs, vp.Len)
			case isExpired:
				// If the key is expired, the next compaction will drop it if
				// its ts > discardTs (of the next compaction).
				builder.AddStaleKey(it.Key(), vs, vp.Len)
			default:
				builder.Add(it.Key(), vs, vp.Len)
			}
		}
		s.kv.opt.Debugf("[%d] LOG Compact. Added %d keys. Skipped %d keys. Iteration took: %v",
			cd.compactorId, numKeys, numSkips, time.Since(timeStart).Round(time.Millisecond))
	} // End of function: addKeys

	if len(kr.left) > 0 {
		it.Seek(kr.left)
	} else {
		it.Rewind()
	}
	for it.Valid() {
		if len(kr.right) > 0 && y.CompareKeys(it.Key(), kr.right) >= 0 {
			break
		}

		bopts := buildTableOptions(s.kv)
		// Set TableSize to the target file size for that level.
		bopts.TableSize = uint64(cd.t.fileSz[cd.nextLevel.level])
		builder := table.NewTableBuilder(bopts)

		// This would do the iteration and add keys to builder.
		addKeys(builder)

		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		if builder.Empty() {
			// Cleanup builder resources:
			builder.Finish()
			builder.Close()
			continue
		}
		numBuilds++
		if err := inflightBuilders.Do(); err != nil {
			// Can't return from here, until I decrRef all the tables that I built so far.
			break
		}
		go func(builder *table.Builder, fileID uint64) {
			var err error
			defer inflightBuilders.Done(err)
			defer builder.Close()

			var tbl *table.Table
			if s.kv.opt.InMemory {
				tbl, err = table.OpenInMemoryTable(builder.Finish(), fileID, &bopts)
			} else {
				fname := table.NewFilename(fileID, s.kv.opt.Dir)
				tbl, err = table.CreateTable(fname, builder)
			}

			// If we couldn't build the table, return fast.
			if err != nil {
				return
			}
			res <- tbl
		}(builder, s.reserveFileID())
	}
	//          4. 将discard写入统计中
	s.kv.vlog.updateDiscardStats(discardStats)
	s.kv.opt.Debugf("Discard stats: %v", discardStats)
}

// compactBuildTables merges topTables and botTables to form a list of new tables.
func (s *levelsController) compactBuildTables(
	lev int, cd compactDef) ([]*table.Table, func() error, error) {

	topTables := cd.top
	botTables := cd.bot

	numTables := int64(len(topTables) + len(botTables))
	y.NumCompactionTablesAdd(s.kv.opt.MetricsEnabled, numTables)
	defer y.NumCompactionTablesAdd(s.kv.opt.MetricsEnabled, -numTables)

	cd.span.Annotatef(nil, "Top tables count: %v Bottom tables count: %v",
		len(topTables), len(botTables))

	keepTable := func(t *table.Table) bool {
		for _, prefix := range cd.dropPrefixes {
			if bytes.HasPrefix(t.Smallest(), prefix) &&
				bytes.HasPrefix(t.Biggest(), prefix) {
				// All the keys in this table have the dropPrefix. So, this
				// table does not need to be in the iterator and can be
				// dropped immediately.
				// 这个表中的所有键都有 dropPrefix。所以，这个
				// 表不需要在迭代器中，并且可以
				// 立即删除。
				return false
			}
		}
		return true
	}
	var valid []*table.Table
	for _, table := range botTables {
		if keepTable(table) {
			valid = append(valid, table)
		}
	}
	//合并的表创建一个迭代器
	newIterator := func() []y.Iterator {
		// Create iterators across all the tables involved first.
		var iters []y.Iterator
		switch {
		case lev == 0:
			iters = append(iters, iteratorsReversed(topTables, table.NOCACHE)...)
		case len(topTables) > 0:
			y.AssertTrue(len(topTables) == 1)
			iters = []y.Iterator{topTables[0].NewIterator(table.NOCACHE)}
		}
		// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
		return append(iters, table.NewConcatIterator(valid, table.NOCACHE))
	}

	res := make(chan *table.Table, 3)
	inflightBuilders := y.NewThrottle(8 + len(cd.splits))
	for _, kr := range cd.splits {
		// Initiate Do here so we can register the goroutines for buildTables too.
		if err := inflightBuilders.Do(); err != nil {
			s.kv.opt.Errorf("cannot start subcompaction: %+v", err)
			return nil, nil, err
		}
		go func(kr keyRange) {
			defer inflightBuilders.Done(nil)
			it := table.NewMergeIterator(newIterator(), false)
			defer it.Close()
			// 使用迭代器遍历所有合并的表
			s.subcompact(it, kr, cd, inflightBuilders, res)
		}(kr)
	}

	var newTables []*table.Table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			//  3. 然后写入buffer注意分割
			newTables = append(newTables, t)
		}
	}()

	// Wait for all table builders to finish and also for newTables accumulator to finish.

	// 等待所有表的建造者完成，也等待newTables累积器完成。
	err := inflightBuilders.Finish()
	close(res)
	wg.Wait() // Wait for all tables to be picked up.

	if err == nil {
		// Ensure created files' directory entries are visible.  We don't mind the extra latency
		// from not doing this ASAP after all file creation has finished because this is a
		// background operation.
		err = s.kv.syncDir(s.kv.opt.Dir)
	}

	if err != nil {
		// An error happened.  Delete all the newly created table files (by calling DecrRef
		// -- we're the only holders of a ref).
		_ = decrRefs(newTables)
		return nil, nil, y.Wrapf(err, "while running compactions for: %+v", cd)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return y.CompareKeys(newTables[i].Biggest(), newTables[j].Biggest()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

func buildChangeSet(cd *compactDef, newTables []*table.Table) pb.ManifestChangeSet {
	changes := []*pb.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes,
			newCreateChange(table.ID(), cd.nextLevel.level, table.KeyID(), table.CompressionType()))
	}
	for _, table := range cd.top {
		// Add a delete change only if the table is not in memory.
		if !table.IsInmemory {
			changes = append(changes, newDeleteChange(table.ID()))
		}
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.ID()))
	}
	return pb.ManifestChangeSet{Changes: changes}
}

func hasAnyPrefixes(s []byte, listOfPrefixes [][]byte) bool {
	for _, prefix := range listOfPrefixes {
		if bytes.HasPrefix(s, prefix) {
			return true
		}
	}

	return false
}

func containsPrefix(table *table.Table, prefix []byte) bool {
	smallValue := table.Smallest()
	largeValue := table.Biggest()
	if bytes.HasPrefix(smallValue, prefix) {
		return true
	}
	if bytes.HasPrefix(largeValue, prefix) {
		return true
	}
	isPresent := func() bool {
		ti := table.NewIterator(0)
		defer ti.Close()
		// In table iterator's Seek, we assume that key has version in last 8 bytes. We set
		// version=0 (ts=math.MaxUint64), so that we don't skip the key prefixed with prefix.
		ti.Seek(y.KeyWithTs(prefix, math.MaxUint64))
		if bytes.HasPrefix(ti.Key(), prefix) {
			return true
		}
		return false
	}

	if bytes.Compare(prefix, smallValue) > 0 &&
		bytes.Compare(prefix, largeValue) < 0 {
		// There may be a case when table contains [0x0000,...., 0xffff]. If we are searching for
		// k=0x0011, we should not directly infer that k is present. It may not be present.
		if !isPresent() {
			return false
		}
		return true
	}

	return false
}

func containsAnyPrefixes(table *table.Table, listOfPrefixes [][]byte) bool {
	for _, prefix := range listOfPrefixes {
		if containsPrefix(table, prefix) {
			return true
		}
	}

	return false
}

type compactDef struct {
	span *otrace.Span

	compactorId int
	t           targets
	p           compactionPriority
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*table.Table
	bot []*table.Table

	thisRange keyRange
	nextRange keyRange
	splits    []keyRange

	thisSize int64

	dropPrefixes [][]byte
}

// addSplits can allow us to run multiple sub-compactions in parallel across the split key ranges.
//// addSplits可以让我们在分裂的关键范围内并行地运行多个子压缩。
func (s *levelsController) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// Let's say we have 10 tables in cd.bot and min width = 3. Then, we'll pick
	// 0, 1, 2 (pick), 3, 4, 5 (pick), 6, 7, 8 (pick), 9 (pick, because last table).
	// This gives us 4 picks for 10 tables.
	// In an edge case, 142 tables in bottom led to 48 splits. That's too many splits, because it
	// then uses up a lot of memory for table builder.
	// We should keep it so we have at max 5 splits.
	//	// 假设我们在cd.bot中有10个表，最小宽度=3。那么，我们将挑选
	// 0，1，2（选），3，4，5（选），6，7，8（选），9（选，因为是最后一个表）。
	// 这给我们提供了10张桌子的4种选择。
	// 在一个边缘案例中，底部的142张桌子导致了48个分牌。这是太多的分牌，因为它
	// 那么就会占用大量的内存用于建表。
	// 我们应该保持它，使我们最多只有5个分叉。
	width := int(math.Ceil(float64(len(cd.bot)) / 5.0))
	if width < 3 {
		width = 3
	}
	skr := cd.thisRange
	skr.extend(cd.nextRange)

	addRange := func(right []byte) {
		skr.right = y.Copy(right)
		cd.splits = append(cd.splits, skr)

		skr.left = skr.right
	}

	for i, t := range cd.bot {
		// last entry in bottom table.
		if i == len(cd.bot)-1 {
			addRange([]byte{})
			return
		}
		if i%width == width-1 {
			// Right should always have ts=maxUint64 otherwise we'll lose keys
			// in subcompaction. Consider the following.
			// Top table is [A1...C3(deleted)]
			// bot table is [B1....C2]
			// This will generate splits like [A1 ... C2] . Notice that we
			// dropped the C3 which is the last key of the top table.
			// See TestCompaction/with_split test.
			right := y.KeyWithTs(y.ParseKey(t.Biggest()), math.MaxUint64)
			addRange(right)
		}
	}
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.nextLevel.RUnlock()
	cd.thisLevel.RUnlock()
}

func (cd *compactDef) allTables() []*table.Table {
	ret := make([]*table.Table, 0, len(cd.top)+len(cd.bot))
	ret = append(ret, cd.top...)
	ret = append(ret, cd.bot...)
	return ret
}

func (s *levelsController) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorId != 0 {
		// Only compactor zero can work on this.
		return false
	}

	cd.nextLevel = s.levels[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	// Because this level and next level are both level 0, we should NOT acquire
	// the read lock twice, because it can result in a deadlock. So, we don't
	// call compactDef.lockLevels, instead locking the level only once and
	// directly here.
	//
	// As per godocs on RWMutex:
	// If a goroutine holds a RWMutex for reading and another goroutine might
	// call Lock, no goroutine should expect to be able to acquire a read lock
	// until the initial read lock is released. In particular, this prohibits
	// recursive read locking. This is to ensure that the lock eventually
	// becomes available; a blocked Lock call excludes new readers from
	// acquiring the lock.
	y.AssertTrue(cd.thisLevel.level == 0)
	y.AssertTrue(cd.nextLevel.level == 0)
	s.levels[0].RLock()
	defer s.levels[0].RUnlock()

	s.cstatus.Lock()
	defer s.cstatus.Unlock()

	top := cd.thisLevel.tables
	var out []*table.Table
	now := time.Now()
	for _, t := range top {
		if t.Size() >= 2*cd.t.fileSz[0] {
			// This file is already big, don't include it.
			continue
		}
		if now.Sub(t.CreatedAt) < 10*time.Second {
			// Just created it 10s ago. Don't pick for compaction.
			continue
		}
		// 3. 如果当前表在进行压缩则直接忽略执行
		if _, beingCompacted := s.cstatus.tables[t.ID()]; beingCompacted {
			continue
		}
		//          2. 然后将其加入compactStatus 的tables 的map中
		out = append(out, t)
	}

	if len(out) < 4 {
		// If we don't have enough tables to merge in L0, don't do it.
		return false
	}
	cd.thisRange = infRange
	cd.top = out

	// Avoid any other L0 -> Lbase from happening, while this is going on.
	thisLevel := s.cstatus.levels[cd.thisLevel.level]
	thisLevel.ranges = append(thisLevel.ranges, infRange)
	for _, t := range out {
		s.cstatus.tables[t.ID()] = struct{}{}
	}

	// For L0->L0 compaction, we set the target file size to max, so the output is always one file.
	// This significantly decreases the L0 table stalls and improves the performance.
	cd.t.fileSz[0] = math.MaxUint32
	return true
}

func (s *levelsController) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.level == 0 {
		panic("Base level can't be zero.")
	}
	// We keep cd.p.adjusted > 0.0 here to allow functions in db.go to artificially trigger
	// L0->Lbase compactions. Those functions wouldn't be setting the adjusted score.
	//	// 我们在这里保持cd.p.adjusted > 0.0，以允许db.go中的函数人为地触发 L0->Lbase的压缩。这些函数不会设置调整后的分数。
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		// Do not compact to Lbase if adjusted score is less than 1.0.
		return false
	}
	//1. 对压缩过程上锁
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}

	var out []*table.Table
	if len(cd.dropPrefixes) > 0 {
		// Use all tables if drop prefix is set. We don't want to compact only a
		// sub-range. We want to compact all the tables.
		out = top

	} else {
		var kr keyRange
		// cd.top[0] is the oldest file. So we start from the oldest file first.
		for _, t := range top {
			dkr := getKeyRange(t)
			if kr.overlapsWith(dkr) {
				out = append(out, t)
				kr.extend(dkr)
			} else {
				break
			}
		}
	}
	cd.thisRange = getKeyRange(out...)
	//  2. 计算L0层中存在数据重合的sstable
	cd.top = out
	//   3. 计算Lbase层与L0层的sstable 列表重合范围有重合的sstable
	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	//  4. 将这些tables 全部加入compactStatus 的tables 的map中
	cd.bot = make([]*table.Table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])

	if len(cd.bot) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}
	return s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0 would try to fill tables from L0 to be compacted with Lbase. If
// it can not do that, it would try to compact tables from L0 -> L0.
//
// Say L0 has 10 tables.
// fillTablesL0ToLbase picks up 5 tables to compact from L0 -> L5.
// Next call to fillTablesL0 would run L0ToLbase again, which fails this time.
// So, instead, we run fillTablesL0ToL0, which picks up rest of the 5 tables to
// be compacted within L0. Additionally, it would set the compaction range in
// cstatus to inf, so no other L0 -> Lbase compactions can happen.
// Thus, L0 -> L0 must finish for the next L0 -> Lbase to begin.

// fillTablesL0将尝试从L0填充表，以便用Lbase压实。如果它不能做到这一点，它将尝试从L0->L0压缩表。
//
// 假设L0有10个表。
// fillTablesL0ToLbase从L0->L5拾取5个表来压缩。下一次调用 fillTablesL0 将再次运行 L0ToLbase，这次失败了。
//因此，我们改为运行 fillTablesL0ToL0，它在 L0 中拾取其余的 5 个表来进行压缩。
//此外，它将把cstatus中的压实范围设置为inf，所以没有其他L0->Lbase的压实可以发生。
//因此，L0->L0必须完成，下一个L0->Lbase才能开始。
func (s *levelsController) fillTablesL0(cd *compactDef) bool {
	//1. 先尝试L0到Lbase的压缩
	if ok := s.fillTablesL0ToLbase(cd); ok {
		return true
	}
	// 2. 失败了会尝试l0到l0的压缩过程
	return s.fillTablesL0ToL0(cd)
}

// sortByStaleData sorts tables based on the amount of stale data they have.
// This is useful in removing tombstones.
func (s *levelsController) sortByStaleDataSize(tables []*table.Table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})
}

// sortByHeuristic sorts tables in increasing order of MaxVersion, so we
// compact older tables first.
func (s *levelsController) sortByHeuristic(tables []*table.Table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	// Sort tables by max version. This is what RocksDB does.
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].MaxVersion() < tables[j].MaxVersion()
	})
}

// This function should be called with lock on levels.
func (s *levelsController) fillMaxLevelTables(tables []*table.Table, cd *compactDef) bool {
	sortedTables := make([]*table.Table, len(tables))
	copy(sortedTables, tables)
	s.sortByStaleDataSize(sortedTables, cd)

	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 {
		// This is a maxLevel to maxLevel compaction and we don't have any stale data.
		return false
	}
	cd.bot = []*table.Table{}
	collectBotTables := func(t *table.Table, needSz int64) {
		totalSize := t.Size()

		j := sort.Search(len(tables), func(i int) bool {
			return y.CompareKeys(tables[i].Smallest(), t.Smallest()) >= 0
		})
		y.AssertTrue(tables[j].ID() == t.ID())
		j++
		// Collect tables until we reach the the required size.
		for j < len(tables) {
			newT := tables[j]
			totalSize += newT.Size()

			if totalSize >= needSz {
				break
			}
			cd.bot = append(cd.bot, newT)
			cd.nextRange.extend(getKeyRange(newT))
			j++
		}
	}
	now := time.Now()
	for _, t := range sortedTables {
		// If the maxVersion is above the discardTs, we won't clean anything in
		// the compaction. So skip this table.
		if t.MaxVersion() > s.kv.orc.discardAtOrBelow() {
			continue
		}
		if now.Sub(t.CreatedAt) < time.Hour {
			// Just created it an hour ago. Don't pick for compaction.
			continue
		}
		// If the stale data size is less than 10 MB, it might not be worth
		// rewriting the table. Skip it.
		if t.StaleDataSize() < 10<<20 {
			continue
		}

		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// Set the next range as the same as the current range. If we don't do
		// this, we won't be able to run more than one max level compactions.
		cd.nextRange = cd.thisRange
		// If we're already compacting this range, don't do anything.
		if s.cstatus.overlapsWith(cd.thisLevel.level, cd.thisRange) {
			continue
		}

		// Found a valid table!
		cd.top = []*table.Table{t}

		needFileSz := cd.t.fileSz[cd.thisLevel.level]
		// The table size is what we want so no need to collect more tables.
		if t.Size() >= needFileSz {
			break
		}
		// TableSize is less than what we want. Collect more tables for compaction.
		// If the level has multiple small tables, we collect all of them
		// together to form a bigger table.
		collectBotTables(t, needFileSz)
		if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			cd.bot = cd.bot[:0]
			cd.nextRange = keyRange{}
			continue
		}
		return true
	}
	if len(cd.top) == 0 {
		return false
	}

	return s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

func (s *levelsController) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	tables := make([]*table.Table, len(cd.thisLevel.tables))
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}
	// We're doing a maxLevel to maxLevel compaction. Pick tables based on the stale data size.
	if cd.thisLevel.isLastLevel() {
		return s.fillMaxLevelTables(tables, cd)
	}
	// We pick tables, so we compact older tables first. This is similar to
	// kOldestLargestSeqFirst in RocksDB.
	s.sortByHeuristic(tables, cd)

	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// If we're already compacting this range, don't do anything.
		if s.cstatus.overlapsWith(cd.thisLevel.level, cd.thisRange) {
			continue
		}
		cd.top = []*table.Table{t}
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)

		cd.bot = make([]*table.Table, right-left)
		copy(cd.bot, cd.nextLevel.tables[left:right])

		if len(cd.bot) == 0 {
			cd.bot = []*table.Table{}
			cd.nextRange = cd.thisRange
			if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(cd.bot...)

		if s.cstatus.overlapsWith(cd.nextLevel.level, cd.nextRange) {
			continue
		}
		if !s.cstatus.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true
	}
	return false
}

//3. 运行压缩任务
func (s *levelsController) runCompactDef(id, l int, cd compactDef) (err error) {
	if len(cd.t.fileSz) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	y.AssertTrue(len(cd.splits) == 0)
	if thisLevel.level == nextLevel.level {
		// don't do anything for L0 -> L0 and Lmax -> Lmax.
	} else {
		//1. 对合并的table 合理的拆分为多个可并行的子压缩
		s.addSplits(&cd)
	}
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	// Table should never be moved directly between levels, always be rewritten to allow discarding
	// invalid versions.
	// 表不应该在各级之间直接移动，总是被重写以允许丢弃 无效的版本。
	newTables, decr, err := s.compactBuildTables(l, cd)
	if err != nil {
		return err
	}
	defer func() {
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	// 将其写入MANIFEST文件
	changeSet := buildChangeSet(&cd, newTables)
	// 将其写入MANIFEST文件
	// We write to the manifest _before_ we delete files (and after we created files)
	if err := s.kv.manifest.addChanges(changeSet.Changes); err != nil {
		return err
	}

	// See comment earlier in this function about the ordering of these ops, and the order in which
	// we access levels when reading.
	// 对新表与旧表进行替换
	if err := nextLevel.replaceTables(cd.bot, newTables); err != nil {
		return err
	}
	// 删除旧表
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	// Note: For level 0, while doCompact is running, it is possible that new tables are added.
	// However, the tables are added only to the end, so it is ok to just delete the first table.

	from := append(tablesToString(cd.top), tablesToString(cd.bot)...)
	to := tablesToString(newTables)
	// 打印日志
	if dur := time.Since(timeStart); dur > 2*time.Second {
		var expensive string
		if dur > time.Second {
			expensive = " [E]"
		}
		s.kv.opt.Infof("[%d]%s LOG Compact %d->%d (%d, %d -> %d tables with %d splits)."+
			" [%s] -> [%s], took %v\n",
			id, expensive, thisLevel.level, nextLevel.level, len(cd.top), len(cd.bot),
			len(newTables), len(cd.splits), strings.Join(from, " "), strings.Join(to, " "),
			dur.Round(time.Millisecond))
	}

	if cd.thisLevel.level != 0 && len(newTables) > 2*s.kv.opt.LevelSizeMultiplier {
		s.kv.opt.Debugf("This Range (numTables: %d)\nLeft:\n%s\nRight:\n%s\n",
			len(cd.top), hex.Dump(cd.thisRange.left), hex.Dump(cd.thisRange.right))
		s.kv.opt.Debugf("Next Range (numTables: %d)\nLeft:\n%s\nRight:\n%s\n",
			len(cd.bot), hex.Dump(cd.nextRange.left), hex.Dump(cd.nextRange.right))
	}
	return nil
}

func tablesToString(tables []*table.Table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.ID()))
	}
	res = append(res, ".")
	return res
}

var errFillTables = errors.New("Unable to fill tables")

// doCompact picks some table on level l and compacts it away to the next level.
func (s *levelsController) doCompact(id int, p compactionPriority) error {
	l := p.level
	y.AssertTrue(l < s.kv.opt.MaxLevels) // Sanity check.
	if p.t.baseLevel == 0 {
		//	1. 计算目标层，其原理是保证每一层的存储大小为上一层的一个量级
		p.t = s.levelTargets()
	}

	_, span := otrace.StartSpan(context.Background(), "Badger.Compaction")
	defer span.End()
	// 1. 创建一个压缩任务
	cd := compactDef{
		compactorId:  id,
		span:         span,
		p:            p,
		t:            p.t,
		thisLevel:    s.levels[l],
		dropPrefixes: p.dropPrefixes,
	}

	// While picking tables to be compacted, both levels' tables are expected to
	// remain unchanged.
	// 在挑选要压缩的表格时，两级的表格都要保持不变。
	if l == 0 {
		//  2. 如果是0压缩，那就执行l0的特殊压缩策略
		cd.nextLevel = s.levels[p.t.baseLevel]
		if !s.fillTablesL0(&cd) {
			return errFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		// We're not compacting the last level so pick the next level.
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = s.levels[l+1]
		}
		if !s.fillTables(&cd) {
			return errFillTables
		}
	}
	defer s.cstatus.delete(cd) // Remove the ranges from compaction status.

	span.Annotatef(nil, "Compaction: %+v", cd)
	//      3. 运行压缩任务
	if err := s.runCompactDef(id, l, cd); err != nil {
		// This compaction couldn't be done successfully.
		s.kv.opt.Warningf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	s.kv.opt.Debugf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.level)
	return nil
}

func (s *levelsController) addLevel0Table(t *table.Table) error {
	// Add table to manifest file only if it is not opened in memory. We don't want to add a table
	// to the manifest file if it exists only in memory.
	if !t.IsInmemory {
		// We update the manifest _before_ the table becomes part of a levelHandler, because at that
		// point it could get used in some compaction.  This ensures the manifest file gets updated in
		// the proper order. (That means this update happens before that of some compaction which
		// deletes the table.)
		err := s.kv.manifest.addChanges([]*pb.ManifestChange{
			newCreateChange(t.ID(), 0, t.KeyID(), t.CompressionType()),
		})
		if err != nil {
			return err
		}
	}

	for !s.levels[0].tryAddLevel0Table(t) {
		// Before we unstall, we need to make sure that level 0 is healthy.
		timeStart := time.Now()
		for s.levels[0].numTables() >= s.kv.opt.NumLevelZeroTablesStall {
			time.Sleep(10 * time.Millisecond)
		}
		dur := time.Since(timeStart)
		if dur > time.Second {
			s.kv.opt.Infof("L0 was stalled for %s\n", dur.Round(time.Millisecond))
		}
		atomic.AddInt64(&s.l0stallsMs, int64(dur.Round(time.Millisecond)))
	}

	return nil
}

func (s *levelsController) close() error {
	err := s.cleanupLevels()
	return y.Wrap(err, "levelsController.Close")
}

// get searches for a given key in all the levels of the LSM tree. It returns
// key version <= the expected version (maxVs). If not found, it returns an empty
// y.ValueStruct.
//  get在LSM树的所有层次中搜索一个给定的键。它返回键的版本<=预期版本（maxVs）。如果没有找到，它会返回一个空的 y.ValueStruct.
func (s *levelsController) get(key []byte, maxVs y.ValueStruct, startLevel int) (
	y.ValueStruct, error) {
	if s.kv.IsClosed() {
		return y.ValueStruct{}, ErrDBClosed
	}
	// It's important that we iterate the levels from 0 on upward. The reason is, if we iterated
	// in opposite order, or in parallel (naively calling all the h.RLock() in some order) we could
	// read level L's tables post-compaction and level L+1's tables pre-compaction. (If we do
	// parallelize this, we will need to call the h.RLock() function by increasing order of level
	// number.)
	//// 我们从0开始向上迭代层次是很重要的。原因是，如果我们以相反的顺序迭代，或者以并行的方式（天真地以某种顺序调用所有的h.RLock()），
	//我们可以在压缩后读取L级的表，在压缩前读取L+1级的表。(如果我们真的将其并行化，我们将需要按级别的增加顺序调用h.RLock()函数。
	//号码递增调用h.RLock()函数）。
	version := y.ParseTs(key)
	//  逐层遍历
	for _, h := range s.levels {
		//  从某一层获取数据
		// Ignore all levels below startLevel. This is useful for GC when L0 is kept in memory.
		// 忽略所有低于startLevel的级别。当L0被保存在内存中时，这对GC很有用。
		if h.level < startLevel {
			continue
		}
		// 从某一层获取数据
		vs, err := h.get(key) // Calls h.RLock() and h.RUnlock().
		if err != nil {
			return y.ValueStruct{}, y.Wrapf(err, "get key: %q", key)
		}
		if vs.Value == nil && vs.Meta == 0 {
			continue
		}
		if vs.Version == version {
			return vs, nil
		}
		if maxVs.Version < vs.Version {
			maxVs = vs
		}
	}
	return maxVs, nil
}

func iteratorsReversed(th []*table.Table, opt int) []y.Iterator {
	out := make([]y.Iterator, 0, len(th))
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator(opt))
	}
	return out
}

// getTables return tables from all levels. It would call IncrRef on all returned tables.
func (s *levelsController) getTables(opt *IteratorOptions) [][]*table.Table {
	res := make([][]*table.Table, 0, len(s.levels))
	for _, level := range s.levels {
		res = append(res, level.getTables(opt))
	}
	return res
}

// iterators returns an array of iterators, for merging.
// Note: This obtains references for the table handlers. Remember to close these iterators.
//  为每个level创建一个迭代器
func (s *levelsController) iterators(opt *IteratorOptions) []y.Iterator {
	// Just like with get, it's important we iterate the levels from 0 on upward, to avoid missing
	// data when there's a compaction.
	itrs := make([]y.Iterator, 0, len(s.levels))
	for _, level := range s.levels {
		itrs = append(itrs, level.iterators(opt)...)
	}
	return itrs
}

// TableInfo represents the information about a table.
type TableInfo struct {
	ID               uint64
	Level            int
	Left             []byte
	Right            []byte
	KeyCount         uint32 // Number of keys in the table
	OnDiskSize       uint32
	StaleDataSize    uint32
	UncompressedSize uint32
	MaxVersion       uint64
	IndexSz          int
	BloomFilterSize  int
}

func (s *levelsController) getTableInfo() (result []TableInfo) {
	for _, l := range s.levels {
		l.RLock()
		for _, t := range l.tables {
			info := TableInfo{
				ID:               t.ID(),
				Level:            l.level,
				Left:             t.Smallest(),
				Right:            t.Biggest(),
				KeyCount:         t.KeyCount(),
				OnDiskSize:       t.OnDiskSize(),
				StaleDataSize:    t.StaleDataSize(),
				IndexSz:          t.IndexSize(),
				BloomFilterSize:  t.BloomFilterSize(),
				UncompressedSize: t.UncompressedSize(),
				MaxVersion:       t.MaxVersion(),
			}
			result = append(result, info)
		}
		l.RUnlock()
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Level != result[j].Level {
			return result[i].Level < result[j].Level
		}
		return result[i].ID < result[j].ID
	})
	return
}

type LevelInfo struct {
	Level          int
	NumTables      int
	Size           int64
	TargetSize     int64
	TargetFileSize int64
	IsBaseLevel    bool
	Score          float64
	Adjusted       float64
	StaleDatSize   int64
}

func (s *levelsController) getLevelInfo() []LevelInfo {
	t := s.levelTargets()
	prios := s.pickCompactLevels()
	result := make([]LevelInfo, len(s.levels))
	for i, l := range s.levels {
		l.RLock()
		result[i].Level = i
		result[i].Size = l.totalSize
		result[i].NumTables = len(l.tables)
		result[i].StaleDatSize = l.totalStaleSize

		l.RUnlock()

		result[i].TargetSize = t.targetSz[i]
		result[i].TargetFileSize = t.fileSz[i]
		result[i].IsBaseLevel = t.baseLevel == i
	}
	for _, p := range prios {
		result[p.level].Score = p.score
		result[p.level].Adjusted = p.adjusted
	}
	return result
}

// verifyChecksum verifies checksum for all tables on all levels.
func (s *levelsController) verifyChecksum() error {
	var tables []*table.Table
	for _, l := range s.levels {
		l.RLock()
		tables = tables[:0]
		for _, t := range l.tables {
			tables = append(tables, t)
			t.IncrRef()
		}
		l.RUnlock()

		for _, t := range tables {
			errChkVerify := t.VerifyChecksum()
			if err := t.DecrRef(); err != nil {
				s.kv.opt.Errorf("unable to decrease reference of table: %s while "+
					"verifying checksum with error: %s", t.Filename(), err)
			}

			if errChkVerify != nil {
				return errChkVerify
			}
		}
	}

	return nil
}

// Returns the sorted list of splits for all the levels and tables based
// on the block offsets.
func (s *levelsController) keySplits(numPerTable int, prefix []byte) []string {
	splits := make([]string, 0)
	for _, l := range s.levels {
		l.RLock()
		for _, t := range l.tables {
			tableSplits := t.KeySplits(numPerTable, prefix)
			splits = append(splits, tableSplits...)
		}
		l.RUnlock()
	}
	sort.Strings(splits)
	return splits
}

// AddTable builds the table from the KV.value options passed through the KV.Key.
func (lc *levelsController) AddTable(
	kv *pb.KV, lev int, dk *pb.DataKey, change *pb.ManifestChange) error {
	// TODO: Encryption / Decryption might be required for the table, if the sender and receiver
	// don't have same encryption mode. See if inplace encryption/decryption can be done.
	// Tables are sent in the sorted order, so no need to sort them here.
	encrypted := len(lc.kv.opt.EncryptionKey) > 0
	y.AssertTrue((dk != nil && encrypted) || (dk == nil && !encrypted))
	// The keyId is zero if there is no encryption.
	opts := buildTableOptions(lc.kv)
	opts.Compression = options.CompressionType(change.Compression)
	opts.DataKey = dk

	fileID := lc.reserveFileID()
	fname := table.NewFilename(fileID, lc.kv.opt.Dir)

	// kv.Value is owned by the z.buffer. Ensure that we copy this buffer.
	var tbl *table.Table
	var err error
	if lc.kv.opt.InMemory {
		if tbl, err = table.OpenInMemoryTable(y.Copy(kv.Value), fileID, &opts); err != nil {
			return errors.Wrap(err, "while creating in-memory table from buffer")
		}
	} else {
		if tbl, err = table.CreateTableFromBuffer(fname, kv.Value, opts); err != nil {
			return errors.Wrap(err, "while creating table from buffer")
		}
	}

	lc.levels[lev].addTable(tbl)
	// Release the ref held by OpenTable. addTable would add a reference.
	_ = tbl.DecrRef()

	change.Id = fileID
	change.Level = uint32(lev)
	if dk != nil {
		change.KeyId = dk.KeyId
	}
	// We use the same data KeyId. So, change.KeyId remains the same.
	y.AssertTrue(change.Op == pb.ManifestChange_CREATE)
	return lc.kv.manifest.addChanges([]*pb.ManifestChange{change})
}
