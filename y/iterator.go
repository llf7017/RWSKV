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

package y

import (
	"bytes"
	"encoding/binary"
)

// ValueStruct represents the value info that can be associated with a key, but also the internal
// Meta field.
//用来表示value的信息
type ValueStruct struct {
	Meta      byte
	UserMeta  byte
	ExpiresAt uint64
	Value     []byte
	Version   uint64 // This field is not serialized. Only for internal usage.这个字段不被序列化。仅供内部使用。记录版本号
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

// EncodedSize is the size of the ValueStruct when encoded
//计算value所占空间
func (v *ValueStruct) EncodedSize() uint32 {
	sz := len(v.Value) + 2 // meta, usermeta.
	enc := sizeVarint(v.ExpiresAt)
	return uint32(sz + enc)
}

// Decode uses the length of the slice to infer the length of the Value field.
//进行解码，转换为value结构体
func (v *ValueStruct) Decode(b []byte) {
	v.Meta = b[0]
	v.UserMeta = b[1]
	var sz int
	v.ExpiresAt, sz = binary.Uvarint(b[2:])
	v.Value = b[2+sz:]
}

// Encode expects a slice of length at least v.EncodedSize().
//将value中的数据写入到buf中，进行加码，按字节存储
func (v *ValueStruct) Encode(b []byte) uint32 {
	b[0] = v.Meta
	b[1] = v.UserMeta
	//sz为写入到b[]中的字节数
	sz := binary.PutUvarint(b[2:], v.ExpiresAt)
	n := copy(b[2+sz:], v.Value)
	return uint32(2 + sz + n)
}

// EncodeTo should be kept in sync with the Encode function above. The reason
// this function exists is to avoid creating byte arrays per key-value pair in
// table/builder.go.
///EncodeTo应该与上面的Encode函数保持同步。这个函数存在的原因是为了避免在table/builder.go中为每个键-值对创建字节数组。
func (v *ValueStruct) EncodeTo(buf *bytes.Buffer) {
	buf.WriteByte(v.Meta)
	buf.WriteByte(v.UserMeta)
	var enc [binary.MaxVarintLen64]byte
	sz := binary.PutUvarint(enc[:], v.ExpiresAt)

	buf.Write(enc[:sz])
	buf.Write(v.Value)
}

// Iterator is an interface for a basic iterator./
//迭代器
type Iterator interface {
	Next()
	Rewind()
	Seek(key []byte)
	Key() []byte
	Value() ValueStruct
	Valid() bool

	// All iterators should be closed so that file garbage collection works.
	Close() error
}
