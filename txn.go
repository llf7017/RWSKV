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
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v3/y"
	"github.com/dgraph-io/ristretto/z"
	"github.com/pkg/errors"
)

/*管理全局时间戳，以及最近提交的事务列表，用于在新的事务提交中对事务开始与提交时间戳中间提交过的事务范围进行冲突检查，
乃至当前活跃的事务的最小时间戳，用于清理旧事务信息*/
type oracle struct {
	isManaged       bool // Does not change value, so no locking required.
	detectConflicts bool // Determines if the txns should be checked for conflicts.决定txn是否需要进行冲突检测

	sync.Mutex // For nextTxnTs and commits.
	// writeChLock lock is for ensuring that transactions go to the write
	// channel in the same order as their commit timestamps.
	// writeChLock是为了确保事务以与提交时间戳相同的顺序进入write channel。
	writeChLock sync.Mutex
	nextTxnTs   uint64

	// Used to block NewTransaction, so all previous commits are visible to a new read.
	//用来阻止NewTransaction，所以所有之前的提交对新的读取是可见的。
	txnMark *y.WaterMark

	// Either of these is used to determine which versions can be permanently
	// discarded during compaction.
	//其中任何一个都被用来确定哪些版本可以在compaction过程中被永久丢弃。
	discardTs uint64       // Used by ManagedDB.
	readMark  *y.WaterMark // Used by DB.

	// committedTxns contains all committed writes (contains fingerprints
	// of keys written and their latest commit counter).
	// committedTxns包含所有已提交的写入内容（包含写入的键的 fingerprints和其最新的commit数量）。
	committedTxns []committedTxn
	lastCleanupTs uint64

	// closer is used to stop watermarks.
	closer *z.Closer
}

type committedTxn struct {
	ts uint64
	// ConflictKeys Keeps track of the entries written at timestamp ts.
	conflictKeys map[uint64]struct{}
}

func newOracle(opt Options) *oracle {
	orc := &oracle{
		isManaged:       opt.managedTxns,
		detectConflicts: opt.DetectConflicts,
		// We're not initializing nextTxnTs and readOnlyTs. It would be done after replay in Open.
		//我们没有初始化 nextTxnTs 和 readOnlyTs。这将在Open中的重放之后完成。
		// WaterMarks must be 64-bit aligned for atomic package, hence we must use pointers here.
		// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
		readMark: &y.WaterMark{Name: "badger.PendingReads"},
		txnMark:  &y.WaterMark{Name: "badger.TxnTimestamp"},
		closer:   z.NewCloser(2),
	}
	orc.readMark.Init(orc.closer)
	orc.txnMark.Init(orc.closer)
	return orc
}

func (o *oracle) Stop() {
	o.closer.SignalAndWait()
}

//
func (o *oracle) readTs() uint64 {
	if o.isManaged {
		panic("ReadTs should not be retrieved for managed DB")
	}

	var readTs uint64
	o.Lock()
	// 从 nextReadTnx中获取一个时间戳的值
	readTs = o.nextTxnTs - 1
	// 然后标记为开始,记录最后的事物时间戳后发送给一个markChan
	o.readMark.Begin(readTs)
	o.Unlock()

	// Wait for all txns which have no conflicts, have been assigned a commit
	// timestamp and are going through the write to value log and LSM tree
	// process. Not waiting here could mean that some txns which have been
	// committed would not be read.
	// 等待所有没有冲突、已经被分配了提交时间戳并且正在将提交的数据写到值value log和LSM树过程的txns结束。
	// 不在这里等待可能意味着一些已经提交的txns 不被读取。
	y.Check(o.txnMark.WaitForMark(context.Background(), readTs))
	return readTs
}

func (o *oracle) nextTs() uint64 {
	o.Lock()
	defer o.Unlock()
	return o.nextTxnTs
}

func (o *oracle) incrementNextTs() {
	o.Lock()
	defer o.Unlock()
	o.nextTxnTs++
}

// Any deleted or invalid versions at or below ts would be discarded during
// compaction to reclaim disk space in LSM tree and thence value log.
func (o *oracle) setDiscardTs(ts uint64) {
	o.Lock()
	defer o.Unlock()
	o.discardTs = ts
	o.cleanupCommittedTransactions()
}

func (o *oracle) discardAtOrBelow() uint64 {
	if o.isManaged {
		o.Lock()
		defer o.Unlock()
		return o.discardTs
	}
	return o.readMark.DoneUntil()
}

// hasConflict must be called while having a lock.
//检查是否有冲突，对即将写入的数据是否已经被更新
func (o *oracle) hasConflict(txn *Txn) bool {
	if len(txn.reads) == 0 {
		return false
	}
	for _, committedTxn := range o.committedTxns {
		// If the committedTxn.ts is less than txn.readTs that implies that the
		// committedTxn finished before the current transaction started.
		// We don't need to check for conflict in that case.
		// This change assumes linearizability. Lack of linearizability could
		// cause the read ts of a new txn to be lower than the commit ts of
		// a txn before it (@mrjn).
		// 如果 commitTxn.ts 小于 txn.readTs，这意味着 commitTxn 在当前事务开始之前完成。
		// 在这种情况下，我们不需要检查冲突。此更改假定线性化。 缺乏线性化可能会导致新 txn 的读取 ts 低于它之前的 txn 的提交 ts 。
		// 如果读时间戳大于已提交的时间戳则忽略
		if committedTxn.ts <= txn.readTs {
			continue
		}
		//时间戳小于已提交的时间戳则判断是否存在读后写的情况存在就冲突
		for _, ro := range txn.reads {
			if _, has := committedTxn.conflictKeys[ro]; has {
				return true
			}
		}
	}

	return false
}

// 创建一个提交时间戳
func (o *oracle) newCommitTs(txn *Txn) (uint64, bool) {
	o.Lock()
	defer o.Unlock()
	// 是否存在冲突
	if o.hasConflict(txn) {
		return 0, true
	}

	var ts uint64
	if !o.isManaged {
		o.doneRead(txn)
		// 清理已经提交的事务的记录数组
		o.cleanupCommittedTransactions()

		// This is the general case, when user doesn't specify the read and commit ts.
		ts = o.nextTxnTs
		o.nextTxnTs++
		// 标记当前的事务开始进行提交，分配了一个新的提交时间戳
		o.txnMark.Begin(ts)

	} else {
		// If commitTs is set, use it instead.
		ts = txn.commitTs
	}

	y.AssertTrue(ts >= o.lastCleanupTs)
	// 5. 将当前提交事务的时间戳和冲突的key组成对象记录在已提交事务的数组中
	if o.detectConflicts {
		// We should ensure that txns are not added to o.committedTxns slice when
		// conflict detection is disabled otherwise this slice would keep growing.
		o.committedTxns = append(o.committedTxns, committedTxn{
			ts:           ts,
			conflictKeys: txn.conflictKeys,
		})
	}

	return ts, false
}

// 标记readTs时间戳已经完成
// 拿到当前事务的readTs标记其为完成
func (o *oracle) doneRead(txn *Txn) {
	if !txn.doneRead {
		txn.doneRead = true
		o.readMark.Done(txn.readTs)
	}
}

//清理掉已经commit的Txn
func (o *oracle) cleanupCommittedTransactions() { // Must be called under o.Lock
	if !o.detectConflicts {
		// When detectConflicts is set to false, we do not store any
		// committedTxns and so there's nothing to clean up.
		return
	}
	// Same logic as discardAtOrBelow but unlocked
	var maxReadTs uint64
	if o.isManaged {
		maxReadTs = o.discardTs
	} else {
		maxReadTs = o.readMark.DoneUntil()
	}

	y.AssertTrue(maxReadTs >= o.lastCleanupTs)

	// do not run clean up if the maxReadTs (read timestamp of the
	// oldest transaction that is still in flight) has not increased
	// 如果 maxReadTs（仍在运行的最旧事务的读取时间戳）没有增加，则不运行清理
	if maxReadTs == o.lastCleanupTs {
		return
	}
	o.lastCleanupTs = maxReadTs

	tmp := o.committedTxns[:0]
	for _, txn := range o.committedTxns {
		if txn.ts <= maxReadTs {
			continue
		}
		tmp = append(tmp, txn)
	}
	o.committedTxns = tmp
}

func (o *oracle) doneCommit(cts uint64) {
	if o.isManaged {
		// No need to update anything.
		return
	}
	o.txnMark.Done(cts)
}

// Txn represents a Badger transaction.
type Txn struct {
	readTs   uint64
	commitTs uint64
	size     int64
	count    int64
	db       *DB

	reads []uint64 // contains fingerprints of keys read. //在该txn下的所有读操作，都会记录在reads下
	// contains fingerprints of keys written. This is used for conflict detection.
	//包含所写钥匙的指纹。这是用来检测冲突的。
	conflictKeys map[uint64]struct{}
	readsLock    sync.Mutex // guards the reads slice. See addReadKey.

	//对于还未进行冲突检测的写入，都会存入pendingwtites,确保冲突检测没问题后，再刷盘
	pendingWrites map[string]*Entry // cache stores any writes done by txn.

	duplicateWrites []*Entry // Used in managed mode to store duplicate entries.

	numIterators int32
	discarded    bool
	doneRead     bool
	update       bool // update is used to conditionally keep track of reads.
}

type pendingWritesIterator struct {
	entries  []*Entry
	nextIdx  int
	readTs   uint64
	reversed bool
}

func (pi *pendingWritesIterator) Next() {
	pi.nextIdx++
}

func (pi *pendingWritesIterator) Rewind() {
	pi.nextIdx = 0
}

func (pi *pendingWritesIterator) Seek(key []byte) {
	key = y.ParseKey(key)
	pi.nextIdx = sort.Search(len(pi.entries), func(idx int) bool {
		cmp := bytes.Compare(pi.entries[idx].Key, key)
		if !pi.reversed {
			return cmp >= 0
		}
		return cmp <= 0
	})
}

func (pi *pendingWritesIterator) Key() []byte {
	y.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return y.KeyWithTs(entry.Key, pi.readTs)
}

func (pi *pendingWritesIterator) Value() y.ValueStruct {
	y.AssertTrue(pi.Valid())
	entry := pi.entries[pi.nextIdx]
	return y.ValueStruct{
		Value:     entry.Value,
		Meta:      entry.meta,
		UserMeta:  entry.UserMeta,
		ExpiresAt: entry.ExpiresAt,
		Version:   pi.readTs,
	}
}

func (pi *pendingWritesIterator) Valid() bool {
	return pi.nextIdx < len(pi.entries)
}

func (pi *pendingWritesIterator) Close() error {
	return nil
}

//  创建一个对PendingWrites数组的迭代器，并指定是正向还是反向
func (txn *Txn) newPendingWritesIterator(reversed bool) *pendingWritesIterator {
	//  如果不是读写事务，或者PendingWrites长度为0，则直接返回
	if !txn.update || len(txn.pendingWrites) == 0 {
		return nil
	}
	// 否则对PendingWrites数组返回排序的切片，并返回迭代器
	entries := make([]*Entry, 0, len(txn.pendingWrites))
	for _, e := range txn.pendingWrites {
		entries = append(entries, e)
	}
	// Number of pending writes per transaction shouldn't be too big in general.
	sort.Slice(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if !reversed {
			return cmp < 0
		}
		return cmp > 0
	})
	return &pendingWritesIterator{
		readTs:   txn.readTs,
		entries:  entries,
		reversed: reversed,
	}
}

//设置对应txn的大小
func (txn *Txn) checkSize(e *Entry) error {
	count := txn.count + 1
	// Extra bytes for the version in key.
	size := txn.size + e.estimateSizeAndSetThreshold(txn.db.valueThreshold()) + 10
	if count >= txn.db.opt.maxBatchCount || size >= txn.db.opt.maxBatchSize {
		return ErrTxnTooBig
	}
	txn.count, txn.size = count, size
	return nil
}

func exceedsSize(prefix string, max int64, key []byte) error {
	return errors.Errorf("%s with size %d exceeded %d limit. %s:\n%s",
		prefix, len(key), max, prefix, hex.Dump(key[:1<<10]))
}

const maxKeySize = 65000
const maxValSize = 1 << 20

func ValidEntry(db *DB, key, val []byte) error {
	switch {
	case len(key) == 0:
		return ErrEmptyKey
	case bytes.HasPrefix(key, badgerPrefix):
		return ErrInvalidKey
	case len(key) > maxKeySize:
		// Key length can't be more than uint16, as determined by table::header.  To
		// keep things safe and allow badger move prefix and a timestamp suffix, let's
		// cut it down to 65000, instead of using 65536.
		return exceedsSize("Key", maxKeySize, key)
	case int64(len(val)) > maxValSize:
		return exceedsSize("Value", maxValSize, val)
	}
	if err := db.isBanned(key); err != nil {
		return err
	}
	return nil
}

//将KV对先存到Txn中，后续完成后再写入数据库
func (txn *Txn) modify(e *Entry) error {
	switch {
	case !txn.update:
		return ErrReadOnlyTxn
	case txn.discarded:
		return ErrDiscardedTxn
	case len(e.Key) == 0:
		return ErrEmptyKey
	//如果以badgerPrefix开头的，都是invalid key
	case bytes.HasPrefix(e.Key, badgerPrefix):
		return ErrInvalidKey
	case len(e.Key) > maxKeySize:
		// Key length can't be more than uint16, as determined by table::header.  To
		// keep things safe and allow badger move prefix and a timestamp suffix, let's
		// cut it down to 65000, instead of using 65536.
		// 密钥长度不能超过 uint16，由 table::header 确定。 为了保证安全并允许prefix移动前缀和时间戳后缀，让我们将其减少到 65000，而不是使用 65536。
		return exceedsSize("Key", maxKeySize, e.Key)
	case int64(len(e.Value)) > txn.db.opt.ValueLogFileSize:
		return exceedsSize("Value", txn.db.opt.ValueLogFileSize, e.Value)
	case txn.db.opt.InMemory && int64(len(e.Value)) > txn.db.valueThreshold():
		return exceedsSize("Value", txn.db.valueThreshold(), e.Value)
	}

	if err := txn.db.isBanned(e.Key); err != nil {
		return err
	}
	if err := txn.checkSize(e); err != nil {
		return err
	}

	// The txn.conflictKeys is used for conflict detection. If conflict detection
	// is disabled, we don't need to store key hashes in this map.
	// txn.conflictKeys 用于冲突检测。 如果禁用冲突检测，我们不需要在此映射中存储密钥哈希。
	if txn.db.opt.DetectConflicts {
		// 按照key计算一个hash值，然后加入冲突检查map中

		fp := z.MemHash(e.Key) // Avoid dealing with byte arrays.
		txn.conflictKeys[fp] = struct{}{}
	}
	// If a duplicate entry was inserted in managed mode, move it to the duplicate writes slice.
	// Add the entry to duplicateWrites only if both the entries have different versions. For
	// same versions, we will overwrite the existing entry.
	// 如果这个key 在当前事务中被写入过，并且与之前的版本不同，则计入重复写入的数组中
	if oldEntry, ok := txn.pendingWrites[string(e.Key)]; ok && oldEntry.version != e.version {
		txn.duplicateWrites = append(txn.duplicateWrites, oldEntry)
	}
	//  将该key的写入操作记录到pending数组里
	txn.pendingWrites[string(e.Key)] = e
	return nil
}

// Set adds a key-value pair to the database.
// It will return ErrReadOnlyTxn if update flag was set to false when creating the transaction.
//
// The current transaction keeps a reference to the key and val byte slice
// arguments. Users must not modify key and val until the end of the transaction.
// Set将一个键值对添加到数据库中。如果在创建事务时更新标志被设置为false，它将返回ErrReadOnlyTxn。
//
// 当前事务保持对key和val字节片参数的引用。在事务结束之前，用户不得修改key和val。
func (txn *Txn) Set(key, val []byte) error {
	return txn.SetEntry(NewEntry(key, val))
}

// SetEntry takes an Entry struct and adds the key-value pair in the struct,
// along with other metadata to the database.
//
// The current transaction keeps a reference to the entry passed in argument.
// Users must not modify the entry until the end of the transaction.
// SetEntry接收一个Entry结构，并将该结构中的键值对与其他元数据一起添加到数据库。
//
// 当前事务保留对参数中传递的条目的引用。 在事务结束前，用户不得修改该条目。
func (txn *Txn) SetEntry(e *Entry) error {
	return txn.modify(e)
}

// Delete deletes a key.
//
// This is done by adding a delete marker for the key at commit timestamp.  Any
// reads happening before this timestamp would be unaffected. Any reads after
// this commit would see the deletion.
//
// The current transaction keeps a reference to the key byte slice argument.
// Users must not modify the key until the end of the transaction.
func (txn *Txn) Delete(key []byte) error {
	e := &Entry{
		Key:  key,
		meta: bitDelete,
	}
	return txn.modify(e)
}

// Get looks for key and returns corresponding Item.
// If key is not found, ErrKeyNotFound is returned.
// Get 查找 key 并返回相应的 Item。 如果没有找到 key，则返回 ErrKeyNotFound
func (txn *Txn) Get(key []byte) (item *Item, rerr error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	} else if txn.discarded {
		return nil, ErrDiscardedTxn
	}
	//检查key是否是被禁止使用的
	if err := txn.db.isBanned(key); err != nil {
		return nil, err
	}
	// 创建一个itme对象
	item = new(Item)
	// 如果是一个读写事务 则从pending数组中找是否有刚刚被修改的key
	if txn.update {
		if e, has := txn.pendingWrites[string(key)]; has && bytes.Equal(key, e.Key) {
			if isDeletedOrExpired(e.meta, e.ExpiresAt) {
				return nil, ErrKeyNotFound
			}
			// Fulfill from cache.
			//直接从缓存中读取数据
			item.meta = e.meta
			item.val = e.Value
			item.userMeta = e.UserMeta
			item.key = key
			item.status = prefetched
			item.version = txn.readTs
			item.expiresAt = e.ExpiresAt
			// We probably don't need to set db on item here.
			return item, nil
		}
		// Only track reads if this is update txn. No need to track read if txn serviced it
		// internally.
		// 仅在读写事务中采取跟踪读取操作, 记录当前的读取的key到reads数组中
		txn.addReadKey(key)
	}
	//  以readTs拼接时间戳版本号
	seek := y.KeyWithTs(key, txn.readTs)
	//从db中查找数据
	vs, err := txn.db.get(seek)
	if err != nil {
		return nil, y.Wrapf(err, "DB::Get key: %q", key)
	}
	if vs.Value == nil && vs.Meta == 0 {
		return nil, ErrKeyNotFound
	}
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return nil, ErrKeyNotFound
	}

	item.key = key
	item.version = vs.Version
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.vptr = y.SafeCopy(item.vptr, vs.Value)
	item.txn = txn
	item.expiresAt = vs.ExpiresAt
	return item, nil
}

//将要查询的key添加到reads队列中
func (txn *Txn) addReadKey(key []byte) {
	if txn.update {
		fp := z.MemHash(key)

		// Because of the possibility of multiple iterators it is now possible
		// for multiple threads within a read-write transaction to read keys at
		// the same time. The reads slice is not currently thread-safe and
		// needs to be locked whenever we mark a key as read.
		//// 由于存在多个迭代器的可能性，现在读写事务中的多个线程可以同时读取键。 读取切片目前不是线程安全的，每当我们将键标记为已读时都需要锁定。
		txn.readsLock.Lock()
		txn.reads = append(txn.reads, fp)
		txn.readsLock.Unlock()
	}
}

// Discard discards a created transaction. This method is very important and must be called. Commit
// method calls this internally, however, calling this multiple times doesn't cause any issues. So,
// this can safely be called via a defer right when transaction is created.
//
// NOTE: If any operations are run on a discarded transaction, ErrDiscardedTxn is returned.
// Discard 丢弃一个已创建的事务。这个方法非常重要，必须被调用。Commit方法在内部调用这个方法，然而，多次调用这个方法并不会导致任何问题。
// 所以，这个方法可以安全地在事务创建时通过defer来调用。
//
// 注意：如果在一个被丢弃的事务上运行任何操作，将返回ErrDiscardedTxn。
func (txn *Txn) Discard() {
	if txn.discarded { // Avoid a re-run.
		return
	}
	if atomic.LoadInt32(&txn.numIterators) > 0 {
		panic("Unclosed iterator at time of Txn.Discard.")
	}
	txn.discarded = true
	if !txn.db.orc.isManaged {
		txn.db.orc.doneRead(txn)
	}
}

//提交txn，将数据写入到存储引擎中去
func (txn *Txn) commitAndSend() (func() error, error) {
	orc := txn.db.orc
	// Ensure that the order in which we get the commit timestamp is the same as
	// the order in which we push these updates to the write channel. So, we
	// acquire a writeChLock before getting a commit timestamp, and only release
	// it after pushing the entries to it.
	// 确保我们获取提交时间戳的顺序与我们将这些更新推送到写入通道的顺序相同。
	//因此，我们在获取提交时间戳之前获取 writeChLock，并且仅在将条目推送到它之后才释放它。
	orc.writeChLock.Lock()
	defer orc.writeChLock.Unlock()

	// 创建一个提交时间戳
	commitTs, conflict := orc.newCommitTs(txn)
	if conflict {
		return nil, ErrConflict
	}

	keepTogether := true
	setVersion := func(e *Entry) {
		if e.version == 0 {
			e.version = commitTs
		} else {
			keepTogether = false
		}
	}
	// 遍历每个在当前事务中写入的key，为其分配唯一的版本号
	for _, e := range txn.pendingWrites {
		setVersion(e)
	}
	// The duplicateWrites slice will be non-empty only if there are duplicate
	// entries with different versions..
	//    4. 遍历pending数组处理每一个实体kv对象
	for _, e := range txn.duplicateWrites {
		setVersion(e)
	}

	entries := make([]*Entry, 0, len(txn.pendingWrites)+len(txn.duplicateWrites)+1)

	processEntry := func(e *Entry) {
		// Suffix the keys with commit ts, so the key versions are sorted in
		// descending order of commit timestamp.
		e.Key = y.KeyWithTs(e.Key, e.version)
		// Add bitTxn only if these entries are part of a transaction. We
		// support SetEntryAt(..) in managed mode which means a single
		// transaction can have entries with different timestamps. If entries
		// in a single transaction have different timestamps, we don't add the
		// transaction markers.
		// 仅当这些条目是事务的一部分时才添加 bitTxn。 我们在托管模式下支持 SetEntryAt(..)，
		// 这意味着单个事务可以具有不同时间戳的条目。 如果单个事务中的条目具有不同的时间戳，我们不会添加事务标记。
		if keepTogether {
			e.meta |= bitTxn
		}
		entries = append(entries, e)
	}

	// The following debug information is what led to determining the cause of
	// bank txn violation bug, and it took a whole bunch of effort to narrow it
	// down to here. So, keep this around for at least a couple of months.
	// var b strings.Builder
	// fmt.Fprintf(&b, "Read: %d. Commit: %d. reads: %v. writes: %v. Keys: ",
	// 	txn.readTs, commitTs, txn.reads, txn.conflictKeys)
	for _, e := range txn.pendingWrites {
		processEntry(e)
	}
	for _, e := range txn.duplicateWrites {
		processEntry(e)
	}

	if keepTogether {
		// CommitTs should not be zero if we're inserting transaction markers.
		y.AssertTrue(commitTs != 0)
		e := &Entry{
			Key:   y.KeyWithTs(txnKey, commitTs),
			Value: []byte(strconv.FormatUint(commitTs, 10)),
			meta:  bitFinTxn,
		}
		entries = append(entries, e)
	}
	//将entries发送到write channal
	req, err := txn.db.sendToWriteCh(entries)
	if err != nil {
		orc.doneCommit(commitTs)
		return nil, err
	}
	ret := func() error {
		//  等待批量写请求处理完成
		err := req.Wait()
		// Wait before marking commitTs as done.
		// We can't defer doneCommit above, because it is being called from a
		// callback here.
		// 标记事务已经提交完成

		orc.doneCommit(commitTs)
		return err
	}
	//	3.  返回一个回调函数
	return ret, nil
}

//检查自 txn 启动后读取的行是否已更新。 如果是，则返回 ErrConflict。
func (txn *Txn) commitPrecheck() error {
	if txn.discarded {
		return errors.New("Trying to commit a discarded txn")
	}
	keepTogether := true
	for _, e := range txn.pendingWrites {
		if e.version != 0 {
			keepTogether = false
		}
	}

	// If keepTogether is True, it implies transaction markers will be added.
	// In that case, commitTs should not be never be zero. This might happen if
	// someone uses txn.Commit instead of txn.CommitAt in managed mode.  This
	// should happen only in managed mode. In normal mode, keepTogether will
	// always be true.
	if keepTogether && txn.db.opt.managedTxns && txn.commitTs == 0 {
		return errors.New("CommitTs cannot be zero. Please use commitAt instead")
	}
	return nil
}

// Commit commits the transaction, following these steps:
//
// 1. If there are no writes, return immediately.
//
// 2. Check if read rows were updated since txn started. If so, return ErrConflict.
//
// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
//
// 4. Batch up all writes, write them to value log and LSM tree.
//
// 5. If callback is provided, Badger will return immediately after checking
// for conflicts. Writes to the database will happen in the background.  If
// there is a conflict, an error will be returned and the callback will not
// run. If there are no conflicts, the callback will be called in the
// background upon successful completion of writes or any error during write.
//
// If error is nil, the transaction is successfully committed. In case of a non-nil error, the LSM
// tree won't be updated, so there's no need for any rollback.
// Commit 按照以下步骤提交事务：
//
// 1. 如果没有写入，立即返回。
//
// 2. 检查自 txn 启动后读取的行是否已更新。 如果是，则返回 ErrConflict。
//
// 3. 如果没有冲突，则生成提交时间戳并更新写入行的提交 ts。
//
// 4. 将所有写入批量化，写入值日志和 LSM 树。
//
// 5. 如果提供了回调，Badger 会在检查后立即返回
// 冲突。 对数据库的写入将在后台进行。 如果
// 有冲突，会返回错误，回调不会
// 跑。 如果没有冲突，回调将在
// 成功完成写入或写入期间出现任何错误的背景。
//
// 如果error为nil，则事务提交成功。 如果出现非零错误，LSM
// 树不会被更新，所以不需要任何回滚。
func (txn *Txn) Commit() error {
	// txn.conflictKeys can be zero if conflict detection is turned off. So we
	// should check txn.pendingWrites.
	//  1. 检查 pending数组是否为空，如果为空则直接返回
	if len(txn.pendingWrites) == 0 {
		return nil // Nothing to do.
	}
	// Precheck before discarding txn.
	//  2. 事务提交的前置检查
	if err := txn.commitPrecheck(); err != nil {
		return err
	}
	defer txn.Discard()
	//  3. 提交并发送日志返回一个回调函数进行等待

	txnCb, err := txn.commitAndSend()
	if err != nil {
		return err
	}
	// If batchSet failed, LSM would not have been updated. So, no need to rollback anything.

	// TODO: What if some of the txns successfully make it to value log, but others fail.
	// Nothing gets updated to LSM, until a restart happens.
	return txnCb()
}

type txnCb struct {
	commit func() error
	user   func(error)
	err    error
}

func runTxnCallback(cb *txnCb) {
	switch {
	case cb == nil:
		panic("txn callback is nil")
	case cb.user == nil:
		panic("Must have caught a nil callback for txn.CommitWith")
	case cb.err != nil:
		cb.user(cb.err)
	case cb.commit != nil:
		err := cb.commit()
		cb.user(err)
	default:
		cb.user(nil)
	}
}

// CommitWith acts like Commit, but takes a callback, which gets run via a
// goroutine to avoid blocking this function. The callback is guaranteed to run,
// so it is safe to increment sync.WaitGroup before calling CommitWith, and
// decrementing it in the callback; to block until all callbacks are run.
func (txn *Txn) CommitWith(cb func(error)) {
	if cb == nil {
		panic("Nil callback provided to CommitWith")
	}

	if len(txn.pendingWrites) == 0 {
		// Do not run these callbacks from here, because the CommitWith and the
		// callback might be acquiring the same locks. Instead run the callback
		// from another goroutine.
		go runTxnCallback(&txnCb{user: cb, err: nil})
		return
	}

	// Precheck before discarding txn.
	if err := txn.commitPrecheck(); err != nil {
		cb(err)
		return
	}

	defer txn.Discard()

	commitCb, err := txn.commitAndSend()
	if err != nil {
		go runTxnCallback(&txnCb{user: cb, err: err})
		return
	}

	go runTxnCallback(&txnCb{user: cb, commit: commitCb})
}

// ReadTs returns the read timestamp of the transaction.
func (txn *Txn) ReadTs() uint64 {
	return txn.readTs
}

// Discarded returns the discarded value of the transaction.
func (txn *Txn) Discarded() bool {
	return txn.discarded
}

// NewTransaction creates a new transaction. Badger supports concurrent execution of transactions,
// providing serializable snapshot isolation, avoiding write skews. Badger achieves this by tracking
// the keys read and at Commit time, ensuring that these read keys weren't concurrently modified by
// another transaction.
//NewTransaction创建一个新的事务。Badger支持事务的并发执行，提供可序列化的快照隔离，避免写错。
//Badger通过跟踪读取的键值，并在提交时间确保这些读取的键值没有被另一个事务同时修改，来实现这一目标。
//
// For read-only transactions, set update to false. In this mode, we don't track the rows read for
// any changes. Thus, any long running iterations done in this mode wouldn't pay this overhead.
// 对于只读事务，将 update 设置为 false。在这种模式下，我们不会跟踪所读的行的任何变化。
//因此，在这种模式下进行的任何长期运行的迭代都不会支付这种开销。

// Running transactions concurrently is OK. However, a transaction itself isn't thread safe, and
// should only be run serially. It doesn't matter if a transaction is created by one goroutine and
// passed down to other, as long as the Txn APIs are called serially.
///并行运行事务是可以的。然而，事务本身并不是线程安全的，它只应该被连续运行。
//如果一个事务是由一个goroutine创建并传递给其他goroutine的，这并不重要，只要Txn APIs被连续调用即可。
//
// When you create a new transaction, it is absolutely essential to call
// Discard(). This should be done irrespective of what the update param is set
// to. Commit API internally runs Discard, but running it twice wouldn't cause
// any issues.
// 当你创建一个新事务时，绝对有必要调用Discard()。无论更新参数被设置为什么，都应该这样做。
// Commit API在内部运行Discard，但运行两次并不会导致任何问题。
//  txn := db.NewTransaction(false)
//  defer txn.Discard()
//  // Call various APIs.
func (db *DB) NewTransaction(update bool) *Txn {
	return db.newTransaction(update, false)
}

func (db *DB) newTransaction(update, isManaged bool) *Txn {
	if db.opt.ReadOnly && update {
		// DB is read-only, force read-only transaction.
		update = false
	}

	txn := &Txn{
		update: update,
		db:     db,
		count:  1,                       // One extra entry for BitFin.
		size:   int64(len(txnKey) + 10), // Some buffer for the extra entry.
	}
	// 创建一个存储哪些key存在冲突的记录map
	if update {
		if db.opt.DetectConflicts {
			txn.conflictKeys = make(map[uint64]struct{})
		}
		//  记录在当前事务上发生写入的key的列表
		txn.pendingWrites = make(map[string]*Entry)
	}
	if !isManaged {
		// 分配一个事务读取时间戳
		txn.readTs = db.orc.readTs()
	}
	return txn
}

// View executes a function creating and managing a read-only transaction for the user. Error
// returned by the function is relayed by the View method.
// If View is used with managed transactions, it would assume a read timestamp of MaxUint64.
//创建一个只读事务
func (db *DB) View(fn func(txn *Txn) error) error {
	if db.IsClosed() {
		return ErrDBClosed
	}
	var txn *Txn
	if db.opt.managedTxns {
		txn = db.NewTransactionAt(math.MaxUint64, false)
	} else {
		txn = db.NewTransaction(false)
	}
	defer txn.Discard()

	return fn(txn)
}

// Update executes a function, creating and managing a read-write transaction
// for the user. Error returned by the function is relayed by the Update method.
// Update cannot be used with managed transactions.
//创建一个读写事务
func (db *DB) Update(fn func(txn *Txn) error) error {
	//检查数据库是否关闭
	if db.IsClosed() {
		return ErrDBClosed
	}
	if db.opt.managedTxns {
		panic("Update can only be used with managedDB=false.")
	}
	//2. 创建一个事务对象
	txn := db.NewTransaction(true)
	//3. Defer 丢弃最终的事务
	defer txn.Discard()

	if err := fn(txn); err != nil {
		return err
	}

	return txn.Commit()
}
