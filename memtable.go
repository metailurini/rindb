package rindb

import (
	"bytes"
	"math"
)

// Estimated overhead for a skip list node structure (slice headers + average pointers)
// Calculation:
// Key ([]byte): 24 bytes (slice header)
// Value ([]byte): 24 bytes (slice header)
// forwards ([]*SLNode): 24 bytes (slice header) + ~11 bytes (avg pointers for p=0.25)
// Total: 24 + 24 + 24 + 11 = 83 bytes
const slNodeOverhead = 83

var _ CmpType = (*Bytes)(nil)

type Bytes []byte

func (b Bytes) Compare(other any) int {
	o, _ := other.(Bytes)
	return bytes.Compare(b, o)
}

type Memtable struct {
	data *SkipList[InternalKey, Record]
	size int // Estimated size in bytes
}

// ByteSize returns the estimated size of the memtable in bytes.
// Note: This is a rough estimate and doesn't account for all overhead.
func (m *Memtable) ByteSize() int {
	return m.size
}

// InitMemtable initializes a new Memtable with the given configuration.
func InitMemtable(config Config) Memtable {
	list, _ := InitSkipList[InternalKey, Record](config)
	return Memtable{data: list, size: 0}
}

func (m *Memtable) Get(key Bytes, seq uint64) (Bytes, error) {
	start := InternalKey{user: key, seq: math.MaxUint64}
	end := InternalKey{user: key, seq: 0}
	it := newSnapshotIterator(m.data.IRange(start, end), seq)
	if it.HasNext() {
		rec, _ := it.Next()
		return rec.GetValue(), nil
	}
	return nil, ErrKeyNotFound
}

func (m *Memtable) Put(record Record) {
	key := record.GetKey()
	value := record.GetValue()

	entrySize := len(key) + len(value) + slNodeOverhead
	ik := InternalKey{user: key, seq: record.GetSequenceNumber()}
	m.data.Put(ik, record)
	m.size += entrySize
}

func (m *Memtable) Clear() {
	m.data.Clear()
	m.size = 0 // Reset size when clearing
}

func (m *Memtable) Iterator() Iterator[Record] {
	return m.data.Iterator()
}

// IRange returns an iterator over records whose keys fall within [start, end].
func (m *Memtable) IRange(start, end Bytes, seq uint64) Iterator[Record] {
	startIK := InternalKey{user: start, seq: math.MaxUint64}
	endIK := InternalKey{user: end, seq: 0}
	return newSnapshotIterator(m.data.IRange(startIK, endIK), seq)
}

// getMaxSequenceNumberFromMemtable iterates through the memtable to find the maximum sequence number.
func getMaxSequenceNumberFromMemtable(memtable Memtable) (uint64, error) {
	var maxSeqNum uint64
	memIterator := memtable.Iterator()
	for memIterator.HasNext() {
		rec, err := memIterator.Next()
		if err != nil {
			return 0, err
		}
		maxSeqNum = max(maxSeqNum, rec.GetSequenceNumber())
	}
	return maxSeqNum, nil
}
