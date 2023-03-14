/*
 * Copyright 2016-2018 Dgraph Labs, Inc. and Contributors
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

package y

import (
	"container/heap"
	"context"
	"sync/atomic"

	"github.com/dgraph-io/ristretto/z"
)

type uint64Heap []uint64

func (u uint64Heap) Len() int            { return len(u) }
func (u uint64Heap) Less(i, j int) bool  { return u[i] < u[j] }
func (u uint64Heap) Swap(i, j int)       { u[i], u[j] = u[j], u[i] }
func (u *uint64Heap) Push(x interface{}) { *u = append(*u, x.(uint64)) }
func (u *uint64Heap) Pop() interface{} {
	old := *u
	n := len(old)
	x := old[n-1]
	*u = old[0 : n-1]
	return x
}

// mark contains one of more indices, along with a done boolean to indicate the
// status of the index: begin or done. It also contains waiters, who could be
// waiting for the watermark to reach >= a certain index.
//mark包含一个或多个索引，以及一个表示索引状态的已完成布尔值：开始或完成。它还包含等待者，他们可能正在等待watermark达到>=某个索引。
type mark struct {
	// Either this is an (index, waiter) pair or (index, done) or (indices, done).
	index   uint64
	waiter  chan struct{}
	indices []uint64
	done    bool // Set to true if the index is done.
}

// WaterMark is used to keep track of the minimum un-finished index.  Typically, an index k becomes
// finished or "done" according to a WaterMark once Done(k) has been called
//   1. as many times as Begin(k) has, AND
//   2. a positive number of times.
//
// An index may also become "done" by calling SetDoneUntil at a time such that it is not
// inter-mingled with Begin/Done calls.
//
// Since doneUntil and lastIndex addresses are passed to sync/atomic packages, we ensure that they
// are 64-bit aligned by putting them at the beginning of the structure.
type WaterMark struct {
	doneUntil uint64
	lastIndex uint64
	Name      string
	markCh    chan mark
}

// Init initializes a WaterMark struct. MUST be called before using it.
func (w *WaterMark) Init(closer *z.Closer) {
	w.markCh = make(chan mark, 100)
	go w.process(closer)
}

// Begin sets the last index to the given value.
func (w *WaterMark) Begin(index uint64) {
	atomic.StoreUint64(&w.lastIndex, index)
	w.markCh <- mark{index: index, done: false}
}

// BeginMany works like Begin but accepts multiple indices.
func (w *WaterMark) BeginMany(indices []uint64) {
	atomic.StoreUint64(&w.lastIndex, indices[len(indices)-1])
	w.markCh <- mark{index: 0, indices: indices, done: false}
}

// Done sets a single index as done.
func (w *WaterMark) Done(index uint64) {
	w.markCh <- mark{index: index, done: true}
}

// DoneMany works like Done but accepts multiple indices.
func (w *WaterMark) DoneMany(indices []uint64) {
	w.markCh <- mark{index: 0, indices: indices, done: true}
}

// DoneUntil returns the maximum index that has the property that all indices
// less than or equal to it are done.
func (w *WaterMark) DoneUntil() uint64 {
	return atomic.LoadUint64(&w.doneUntil)
}

// SetDoneUntil sets the maximum index that has the property that all indices
// less than or equal to it are done.
func (w *WaterMark) SetDoneUntil(val uint64) {
	atomic.StoreUint64(&w.doneUntil, val)
}

// LastIndex returns the last index for which Begin has been called.
func (w *WaterMark) LastIndex() uint64 {
	return atomic.LoadUint64(&w.lastIndex)
}

// WaitForMark waits until the given index is marked as done.
//waitForMark等待被给定的时间戳为index的事务被标记为完成
func (w *WaterMark) WaitForMark(ctx context.Context, index uint64) error {
	//	获取最后一个已提交的时间戳
	if w.DoneUntil() >= index {
		return nil
	}
	// 创建一个用于wait回调的chan
	waitCh := make(chan struct{})

	w.markCh <- mark{index: index, waiter: waitCh}
	//  等待waitChan的回调，或者上下文的取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}

// process is used to process the Mark channel. This is not thread-safe,
// so only run one goroutine for process. One is sufficient, because
// all goroutine ops use purely memory and cpu.
// Each index has to emit at least one begin watermark in serial order otherwise waiters
// can get blocked idefinitely. Example: We had an watermark at 100 and a waiter at 101,
// if no watermark is emitted at index 101 then waiter would get stuck indefinitely as it
// can't decide whether the task at 101 has decided not to emit watermark or it didn't get
// scheduled yet.
// process用于处理Mark通道。这不是线程安全的，所以只为进程运行一个goroutine。一个就够了，因为所有的goroutine操作都单纯使用内存和cpu。
// 每个索引都必须按序列顺序至少发出一个watermark，否则等待者会被无限期地阻塞。例子：我们在100处有一个watermark，101处有一个等待者。
// 如果在索引101处没有发watermark，那么等待者就会被无限期地卡住，因为它不能确定101处的任务是决定不发出watermark还是还没有被排定计划。
func (w *WaterMark) process(closer *z.Closer) {
	defer closer.Done()

	//type uint64Heap []uint64
	var indices uint64Heap
	// pending maps raft proposal index to the number of pending mutations for this proposal.
	pending := make(map[uint64]int)
	waiters := make(map[uint64][]chan struct{})
	//初始化堆，这个堆使用的数据结构是最小二叉树，即根节点比左边子树和右边子树的所有值都小
	//对heap进行初始化，生成小根堆（或大根堆）
	heap.Init(&indices)

	processOne := func(index uint64, done bool) {
		// If not already done, then set. Otherwise, don't undo a done entry.
		prev, present := pending[index]
		if !present {
			heap.Push(&indices, index)
		}

		delta := 1
		if done {
			delta = -1
		}
		pending[index] = prev + delta

		// Update mark by going through all indices in order; and checking if they have
		// been done. Stop at the first index, which isn't done.
		doneUntil := w.DoneUntil()
		if doneUntil > index {
			AssertTruef(false, "Name: %s doneUntil: %d. Index: %d", w.Name, doneUntil, index)
		}

		until := doneUntil
		loops := 0

		for len(indices) > 0 {
			min := indices[0]
			if done := pending[min]; done > 0 {
				break // len(indices) will be > 0.
			}
			// Even if done is called multiple times causing it to become
			// negative, we should still pop the index.
			heap.Pop(&indices)
			delete(pending, min)
			until = min
			loops++
		}

		if until != doneUntil {
			AssertTrue(atomic.CompareAndSwapUint64(&w.doneUntil, doneUntil, until))
		}

		notifyAndRemove := func(idx uint64, toNotify []chan struct{}) {
			for _, ch := range toNotify {
				close(ch)
			}
			delete(waiters, idx) // Release the memory back.
		}

		if until-doneUntil <= uint64(len(waiters)) {
			// Issue #908 showed that if doneUntil is close to 2^60, while until is zero, this loop
			// can hog up CPU just iterating over integers creating a busy-wait loop. So, only do
			// this path if until - doneUntil is less than the number of waiters.
			for idx := doneUntil + 1; idx <= until; idx++ {
				if toNotify, ok := waiters[idx]; ok {
					notifyAndRemove(idx, toNotify)
				}
			}
		} else {
			for idx, toNotify := range waiters {
				if idx <= until {
					notifyAndRemove(idx, toNotify)
				}
			}
		} // end of notifying waiters.
	}

	for {
		select {
		//返回一个被关闭的channel
		case <-closer.HasBeenClosed():
			return
		case mark := <-w.markCh:
			if mark.waiter != nil {
				doneUntil := atomic.LoadUint64(&w.doneUntil)
				if doneUntil >= mark.index {
					close(mark.waiter)
				} else {
					ws, ok := waiters[mark.index]
					if !ok {
						waiters[mark.index] = []chan struct{}{mark.waiter}
					} else {
						waiters[mark.index] = append(ws, mark.waiter)
					}
				}
			} else {
				if mark.index > 0 {
					processOne(mark.index, mark.done)
				}
				for _, index := range mark.indices {
					processOne(index, mark.done)
				}
			}
		}
	}
}
