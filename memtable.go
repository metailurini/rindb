package rindb

import (
	"bytes"
)

var _ CmpType = (*Bytes)(nil)

type Bytes []byte

func (b Bytes) Compare(other any) int {
	o, _ := other.(Bytes)
	return bytes.Compare(b, o)
}

type Memtable struct {
	data *SkipList[Bytes, Bytes]
}

func toRecord(node *SLNode[Bytes, Bytes]) Record {
	return RecordImpl{node.Key, node.Value}
}

func InitMemtable() Memtable {
	list, _ := InitSkipList[Bytes, Bytes]()
	return Memtable{data: list}
}

func (m Memtable) Get(key Bytes) (Bytes, error) {
	return m.data.Get(key)
}

func (m Memtable) Put(key, value Bytes) {
	m.data.Put(key, value)
}

func (m Memtable) Clear() {
	m.data.Clear()
}
