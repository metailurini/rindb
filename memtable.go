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

// Bytes is a convenience wrapper around a byte slice that implements the
// CmpType interface. It is exported so callers can use it with generic
// collections like SkipList.
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

type memtable struct {
	data *SkipList[InternalKey, Record]
	size int // Estimated size in bytes
}

// ByteSize returns the estimated size of the memtable in bytes.
// Note: This is a rough estimate and doesn't account for all overhead.
func (m *memtable) ByteSize() int {
	return m.size
}

// InitMemtable initializes a new Memtable with the given configuration.
func InitMemtable(config Config) memtable {
	list, _ := InitSkipList[InternalKey, Record](config)
	return memtable{data: list, size: 0}
}

// Get returns the latest value for the given key.
func (m *memtable) Get(key Bytes) (Bytes, error) {
	return m.GetAt(key, math.MaxUint64)
}

// GetAt returns the value for the highest sequence <= seq.
func (m *memtable) GetAt(key Bytes, seq uint64) (Bytes, error) {
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

func (m *memtable) Put(record Record) {
	key := record.GetKey()
	value := record.GetValue()
	ik := InternalKey{UserKey: key, Seq: record.GetSequenceNumber(), Type: record.GetType()}

	entrySize := len(key) + internalKeySuffixLen + len(value) + slNodeOverhead

	m.data.Put(ik, record)
	m.size += entrySize
}

func (m *memtable) Clear() {
	m.data.Clear()
	m.size = 0 // Reset size when clearing
}

func (m *memtable) Iterator() Iterator[Record] {
	return m.data.Iterator()
}

// IRange returns an iterator over records whose keys fall within [start, end]
// and sequence numbers less than or equal to seq.
func (m *memtable) IRange(start, end Bytes, seq uint64) Iterator[Record] {
	startKey := InternalKey{UserKey: start, Seq: math.MaxUint64, Type: TypeValue}
	endKey := InternalKey{UserKey: end, Seq: 0, Type: TypeMerge}
	it := m.data.IRange(startKey, endKey)
	return &memtableIRange{it: it, seq: seq}
}

type memtableIRange struct {
	it           Iterator[Record]
	seq          uint64
	next         Record
	preparedNext bool
	prev         Record
	preparedPrev bool
	err          error
}

func (mi *memtableIRange) prepareNext() {
	for !mi.preparedNext && mi.err == nil {
		if !mi.it.HasNext() {
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
		mi.preparedNext = true
	}
}

func (mi *memtableIRange) HasNext() bool {
	mi.prepareNext()
	return mi.preparedNext
}

func (mi *memtableIRange) Next() (Record, error) {
	if !mi.HasNext() {
		var empty Record
		if mi.err != nil {
			return empty, mi.err
		}
		return empty, EOI
	}
	mi.preparedNext = false
	mi.preparedPrev = false
	return mi.next, nil
}

func (mi *memtableIRange) preparePrev() {
	for !mi.preparedPrev && mi.err == nil {
		if !mi.it.HasPrev() {
			return
		}
		rec, err := mi.it.Prev()
		if err != nil {
			mi.err = err
			return
		}
		if rec.GetSequenceNumber() > mi.seq {
			continue
		}
		mi.prev = rec
		mi.preparedPrev = true
	}
}

// HasPrev implements Iterator[Record].
func (mi *memtableIRange) HasPrev() bool {
	mi.preparePrev()
	return mi.preparedPrev
}

// Prev implements Iterator[Record].
func (mi *memtableIRange) Prev() (Record, error) {
	if !mi.preparedPrev {
		if !mi.HasPrev() {
			var empty Record
			if mi.err != nil {
				return empty, mi.err
			}
			return empty, EOI
		}
	}
	mi.preparedPrev = false
	mi.preparedNext = false
	return mi.prev, nil
}

// Cleanup removes records with sequence numbers less than minSeq.
func (m *memtable) Cleanup(minSeq uint64) {
	type obsolete struct {
		key  InternalKey
		vlen int
	}
	var (
		obs      []obsolete
		lastKey  Bytes
		snapKept bool
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
			snapKept = rec.GetSequenceNumber() <= minSeq
			continue // keep latest version for this key
		}
		seq := rec.GetSequenceNumber()
		if !snapKept && seq <= minSeq {
			snapKept = true
			continue
		}
		if seq < minSeq {
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
func getMaxSequenceNumberFromMemtable(memtable memtable) (uint64, error) {
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
