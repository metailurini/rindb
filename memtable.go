package main

import (
	"bytes"
)

var _ CmpType = (*Bytes)(nil)

type Bytes []byte

func (b Bytes) Compare(other any) int {
	o, _ := other.(Bytes)
	return bytes.Compare(b, o)
}

type memtable struct {
	data *skipList[Bytes, Bytes]
}

func initMemtable() *memtable {
	list, _ := initSkipList[Bytes, Bytes]()
	return &memtable{data: list}
}

func (m *memtable) get(key Bytes) (Bytes, error) {
	return m.data.get(key)
}

func (m *memtable) put(key, value Bytes) {
	m.data.put(key, value)
}

func (m *memtable) delete(key Bytes) {
	m.data.put(key, nil)
}

func (m *memtable) flush() {
	panic("implement me")
}
