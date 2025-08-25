package rindb

import "errors"

type pqItem struct {
	rec  Record
	iter Iterator[Record]
}

func buildRangePQ(iterators []Iterator[Record]) (*PriorityQueue[pqItem], error) {
	less := func(a, b pqItem) bool {
		cmp := a.rec.GetKey().Compare(b.rec.GetKey())
		if cmp == CmpEqual {
			return a.rec.GetSequenceNumber() > b.rec.GetSequenceNumber()
		}
		return cmp == CmpLess
	}

	pq := NewPriorityQueue(less)
	for _, it := range iterators {
		if it.HasNext() {
			rec, err := it.Next()
			if err != nil {
				if !errors.Is(err, EOI) {
					return nil, err
				}
				continue
			}
			pq.PushItem(pqItem{rec: rec, iter: it})
		}
	}
	return pq, nil
}

// RangeIterator merges multiple iterators and iterates over them in order.
// It implements Iterator[Record] and provides a Close method for resource
// cleanup.
type RangeIterator struct {
	pq         *PriorityQueue[pqItem]
	lastKey    Bytes
	lastKeySet bool
	next       Record
	prepared   bool
	cleanup    func()
	err        error
}

func (m *RangeIterator) prepare() {
	for !m.prepared && m.err == nil && m.pq.Len() > 0 {
		item := m.pq.PopItem()
		key := item.rec.GetKey()

		if !m.lastKeySet || key.Compare(m.lastKey) != CmpEqual {
			// Emit the newest record for this user_key (PUT or DELETE).
			m.next = item.rec
			m.prepared = true

			m.lastKey = key.Clone()
			m.lastKeySet = true
		}

		if item.iter.HasNext() {
			rec, err := item.iter.Next()
			if err != nil {
				if !errors.Is(err, EOI) {
					m.err = err
				}
			} else {
				m.pq.PushItem(pqItem{rec: rec, iter: item.iter})
			}
		}
	}
}

// HasNext implements Iterator[Record].
func (m *RangeIterator) HasNext() bool {
	m.prepare()
	return m.prepared
}

// Next implements Iterator[Record].
func (m *RangeIterator) Next() (Record, error) {
	if !m.HasNext() {
		var empty Record
		if m.err != nil {
			return empty, m.err
		}
		return empty, EOI
	}
	m.prepared = false
	return m.next, nil
}

// Close releases any resources held by the iterator. It is safe to call multiple times.
func (m *RangeIterator) Close() error {
	if m.cleanup != nil {
		m.cleanup()
		m.cleanup = nil
	}
	return m.err
}
