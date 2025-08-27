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

// Clone returns a deep copy of the Bytes slice.
func (b Bytes) Clone() Bytes {
	c := make(Bytes, len(b))
	copy(c, b)
	return c
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

// Get returns the latest value for the given key.
func (m *Memtable) Get(key Bytes) (Bytes, error) {
	return m.GetAt(key, math.MaxUint64)
}

// GetAt returns the value for the highest sequence <= seq.
func (m *Memtable) GetAt(key Bytes, seq uint64) (Bytes, error) {
	searchKey := InternalKey{UserKey: key, Seq: seq, Type: TypeValue}
	node, err := m.data.FindGreaterOrEqual(searchKey)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(node.Key.UserKey, key) {
		return nil, ErrKeyNotFound
	}
	rec := node.Value
	if rec.GetType() == TypeDeletion {
		return nil, ErrKeyNotFound
	}
	return rec.GetValue(), nil
}

func (m *Memtable) Put(record Record) {
	key := record.GetKey()
	value := record.GetValue()
	ik := InternalKey{UserKey: key, Seq: record.GetSequenceNumber(), Type: record.GetType()}

	entrySize := len(key) + internalKeySuffixLen + len(value) + slNodeOverhead

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

// IRange returns an iterator over records whose keys fall within [start, end]
// and sequence numbers less than or equal to seq.
func (m *Memtable) IRange(start, end Bytes, seq uint64) Iterator[Record] {
	startKey := InternalKey{UserKey: start, Seq: math.MaxUint64, Type: TypeValue}
	endKey := InternalKey{UserKey: end, Seq: 0, Type: TypeMerge}
	it := m.data.IRange(startKey, endKey)
	return &memtableIRange{it: it, seq: seq}
}

type memtableIRange struct {
	it       Iterator[Record]
	seq      uint64
	next     Record
	prepared bool
	err      error
}

func (mi *memtableIRange) prepare() {
	for !mi.prepared && mi.err == nil {
		if !mi.it.HasNext() {
			mi.err = EOI
			return
		}
		rec, err := mi.it.Next()
		if err != nil {
			mi.err = err
			return
		}
		if rec.GetSequenceNumber() > mi.seq {
			continue
		}
		mi.next = rec
		mi.prepared = true
	}
}

func (mi *memtableIRange) HasNext() bool {
	mi.prepare()
	return mi.prepared
}

func (mi *memtableIRange) Next() (Record, error) {
	if !mi.HasNext() {
		var empty Record
		if mi.err != nil {
			return empty, mi.err
		}
		return empty, EOI
	}
	mi.prepared = false
	return mi.next, nil
}

// Cleanup removes records with sequence numbers less than minSeq.
func (m *Memtable) Cleanup(minSeq uint64) {
	type obsolete struct {
		key  InternalKey
		vlen int
	}
	var (
		obs     []obsolete
		lastKey Bytes
	)
	it := m.data.Iterator()
	for it.HasNext() {
		rec, err := it.Next()
		if err != nil {
			break
		}
		key := rec.GetKey()
		if !bytes.Equal(lastKey, key) {
			lastKey = key.Clone()
			continue // keep latest version for this key
		}
		if rec.GetSequenceNumber() < minSeq {
			ik := InternalKey{UserKey: key, Seq: rec.GetSequenceNumber(), Type: rec.GetType()}
			obs = append(obs, obsolete{key: ik, vlen: len(rec.GetValue())})
		}
	}
	for _, o := range obs {
		_ = m.data.Remove(o.key)
		m.size -= len(o.key.UserKey) + internalKeySuffixLen + o.vlen + slNodeOverhead
	}
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
