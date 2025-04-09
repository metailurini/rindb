package rindb

import (
	"bytes"
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
	data *SkipList[Bytes, Bytes]
	size int // Estimated size in bytes
}

// ByteSize returns the estimated size of the memtable in bytes.
// Note: This is a rough estimate and doesn't account for all overhead.
func (m *Memtable) ByteSize() int {
	return m.size
}

func toRecord(node *SLNode[Bytes, Bytes]) Record {
	return RecordImpl{node.Key, node.Value}
}

// InitMemtable initializes a new Memtable with the given configuration.
func InitMemtable(config Config) Memtable {
	list, _ := InitSkipList[Bytes, Bytes](config)
	// Initialize size to a baseline overhead estimate if desired, or 0
	return Memtable{data: list, size: 0}
}

func (m *Memtable) Get(key Bytes) (Bytes, error) {
	return m.data.Get(key)
}

func (m *Memtable) Put(key, value Bytes) {
	// Estimate size increase: key length + value length + node overhead
	entrySize := len(key) + len(value) + slNodeOverhead

	// Check if the key already exists to adjust size calculation
	oldValue, err := m.data.Get(key)
	if err == nil {
		// Key exists, subtract the old entry's estimated size contribution
		m.size -= (len(key) + len(oldValue) + slNodeOverhead)
	}

	m.data.Put(key, value)
	m.size += entrySize // Add the new entry's size
}

func (m *Memtable) Clear() {
	m.data.Clear()
	m.size = 0 // Reset size when clearing
}
