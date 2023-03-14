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

package skl

import (
	"sync/atomic"
	"unsafe"

	"github.com/dgraph-io/badger/v3/y"
)

const (
	//字节数
	offsetSize = int(unsafe.Sizeof(uint32(0))) //值为4，因为地址是uint32

	// Always align nodes on 64-bit boundaries, even on 32-bit architectures,
	// so that the node.value field is 64-bit aligned. This is necessary because
	// node.getValueOffset uses atomic.LoadUint64, which expects its input
	// pointer to be 64-bit aligned.
	//64位对齐
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1 //值为7
)

// Arena should be lock-free.
type Arena struct {
	n          uint32
	shouldGrow bool
	buf        []byte
}

// newArena returns a new arena.
//新建一个Arena
func newArena(n int64) *Arena {
	// Don't store data at position 0 in order to reserve offset=0 as a kind
	// of nil pointer.
	//在offset=0的起始位置不存数据
	out := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	return out
}

//用于定位写入的数据起始点，并且进行容量判断，如果未超过buf的大小就无需扩容，否则就启动扩容程序
func (s *Arena) allocate(sz uint32) uint32 {
	//offset是n的大小
	offset := atomic.AddUint32(&s.n, sz)
	if !s.shouldGrow {
		y.AssertTrue(int(offset) <= len(s.buf))
		return offset - sz
	}

	// We are keeping extra bytes in the end so that the checkptr doesn't fail. We apply some
	// intelligence to reduce the size of the node by only keeping towers upto valid height and not
	// maxHeight. This reduces the node's size, but checkptr doesn't know about its reduced size.
	// checkptr tries to verify that the node of size MaxNodeSize resides on a single heap
	// allocation which causes this error: checkptr:converted pointer straddles multiple allocations
	//如果插入node后容量过大，超过了无需扩容就可以插入一个node节点的容量，就启动扩容程序
	if int(offset) > len(s.buf)-MaxNodeSize {
		growBy := uint32(len(s.buf))
		//如果容量还很小的时候，每次只扩充sz大小（即需要插入的node大小）。如果容量已经超过了uint32最大，那么就一次性申请uint32大小的内存。
		if growBy > 1<<30 {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}
		newBuf := make([]byte, len(s.buf)+int(growBy))
		y.AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
	}
	return offset - sz
}

func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

// putNode allocates a node in the arena. The node is aligned on a pointer-sized
// boundary. The arena offset of the node is returned.
func (s *Arena) putNode(height int) uint32 {
	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := (maxHeight - height) * offsetSize

	// Pad the allocation with enough bytes to ensure pointer alignment.
	// 用足够的字节填充分配，以确保指针对齐
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	//n为插入该node的起始偏移量
	n := s.allocate(l)

	// Return the aligned offset.
	//a &^ b 的意思就是 清零a中，ab都为1的位。目的是为了对齐，使得开始插入数据的地址为8的整数倍。
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

// Put will *copy* val into arena. To make better use of this, reuse your input
// val buffer. Returns an offset into buf. User is responsible for remembering
// size of val. We could also store this size inside arena but the encoding and
// decoding will incur some overhead.
//将value写入到buf中
func (s *Arena) putVal(v y.ValueStruct) uint32 {
	l := uint32(v.EncodedSize())
	offset := s.allocate(l)
	//将数据写入到offset中
	v.Encode(s.buf[offset:])
	return offset
}

//将value写入到buf中
func (s *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := s.allocate(keySz)
	buf := s.buf[offset : offset+keySz]
	y.AssertTrue(len(key) == copy(buf, key))
	return offset
}

// getNode returns a pointer to the node located at offset. If the offset is
// zero, then the nil node pointer is returned.
//返回buf中固定偏移量的node指针。
func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

// getKey returns byte slice at offset.
//获取key，返回一个从其实位置开始固定大小size的字节切片
func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

// getVal returns byte slice at offset. The given size should be just the value
// size and should NOT include the meta bytes.
//获取value，返回一个value类型的结构体
func (s *Arena) getVal(offset uint32, size uint32) (ret y.ValueStruct) {
	ret.Decode(s.buf[offset : offset+size])
	return
}

// getNodeOffset returns the offset of node in the arena. If the node pointer is
// nil, then the zero offset is returned.
//返回该node在buf中的地址偏移量。
func (s *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0
	}

	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}
