package rindb

import "errors"

type pqItem struct {
	rec  Record
	iter Iterator[Record]
}

// MergingIterator merges multiple iterators without deduplication. It yields
// records ordered by key and sequence number (descending) and supports
// bidirectional traversal.
type MergingIterator struct {
	fwd     *PriorityQueue[pqItem]
	rev     *PriorityQueue[pqItem]
	cur     pqItem
	curSet  bool
	cleanup func()
	err     error
}

// NewMergingIterator constructs a MergingIterator over provided iterators.
// The optional cleanup function is called when Close is invoked.
func NewMergingIterator(iterators []Iterator[Record], cleanup func()) (*MergingIterator, error) {
	lessFwd := func(a, b pqItem) bool {
		cmp := a.rec.GetKey().Compare(b.rec.GetKey())
		if cmp == CmpEqual {
			return a.rec.GetSequenceNumber() > b.rec.GetSequenceNumber()
		}
		return cmp == CmpLess
	}
	lessRev := func(a, b pqItem) bool {
		cmp := a.rec.GetKey().Compare(b.rec.GetKey())
		if cmp == CmpEqual {
			return a.rec.GetSequenceNumber() < b.rec.GetSequenceNumber()
		}
		return cmp == CmpGreater
	}
	fwd := NewPriorityQueue(lessFwd)
	rev := NewPriorityQueue(lessRev)
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
			fwd.PushItem(pqItem{rec: rec, iter: it})
		}
	}
	return &MergingIterator{fwd: fwd, rev: rev, cleanup: cleanup}, nil
}

// HasNext implements Iterator[Record].
func (m *MergingIterator) HasNext() bool {
	return m.err == nil && m.fwd.Len() > 0
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
	item := m.fwd.PopItem()
	m.rev.PushItem(item)
	m.cur = item
	m.curSet = true
	if item.iter.HasNext() {
		rec, err := item.iter.Next()
		if err != nil {
			if !errors.Is(err, EOI) {
				m.err = err
			}
		} else {
			m.fwd.PushItem(pqItem{rec: rec, iter: item.iter})
		}
	}
	return item.rec, nil
}

// HasPrev implements Iterator[Record].
func (m *MergingIterator) HasPrev() bool {
	return m.err == nil && m.rev.Len() > 1
}

// Prev implements Iterator[Record].
func (m *MergingIterator) Prev() (Record, error) {
	if !m.HasPrev() {
		var empty Record
		if m.err != nil {
			return empty, m.err
		}
		return empty, EOI
	}
	cur := m.rev.PopItem()
	if cur.iter.HasPrev() {
		rec, err := cur.iter.Prev()
		if err != nil {
			if !errors.Is(err, EOI) {
				m.err = err
			}
		} else if rec.GetKey().Compare(cur.rec.GetKey()) != CmpEqual || rec.GetSequenceNumber() != cur.rec.GetSequenceNumber() {
			m.fwd.PushItem(pqItem{rec: rec, iter: cur.iter})
		}
	}
	m.fwd.PushItem(cur)
	prev := m.rev.PeekItem()
	m.cur = prev
	m.curSet = true
	return prev.rec, nil
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
