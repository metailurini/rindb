package rindb

import "errors"

type pqItem struct {
	rec  Record
	iter Iterator[Record]
}

// MergingIterator merges multiple iterators without deduplication. It yields
// records ordered by key and sequence number (descending).
type MergingIterator struct {
	pq       *PriorityQueue[pqItem]
	next     Record
	prepared bool
	cleanup  func()
	err      error
}

// NewMergingIterator constructs a MergingIterator over provided iterators.
// The optional cleanup function is called when Close is invoked.
func NewMergingIterator(iterators []Iterator[Record], cleanup func()) (*MergingIterator, error) {
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
					if cleanup != nil {
						cleanup()
					}
					return nil, err
				}
				continue
			}
			pq.PushItem(pqItem{rec: rec, iter: it})
		}
	}

	return &MergingIterator{pq: pq, cleanup: cleanup}, nil
}

func (m *MergingIterator) prepare() {
	if m.prepared || m.err != nil || m.pq.Len() == 0 {
		return
	}

	item := m.pq.PopItem()
	m.next = item.rec
	m.prepared = true

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

// HasNext implements Iterator[Record].
func (m *MergingIterator) HasNext() bool {
	m.prepare()
	return m.prepared
}

// Next implements Iterator[Record].
func (m *MergingIterator) Next() (Record, error) {
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

// HasPrev implements Iterator[Record].
func (m *MergingIterator) HasPrev() bool {
	return false
}

// Prev implements Iterator[Record].
func (m *MergingIterator) Prev() (Record, error) {
	var empty Record
	return empty, EOI
}

// Close releases any resources held by the iterator. It is safe to call
// multiple times.
func (m *MergingIterator) Close() error {
	if m.cleanup != nil {
		m.cleanup()
		m.cleanup = nil
	}
	return m.err
}
