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
	cleanup func()
	err     error

	// prepared next state
	nextPrepared bool
	nextItem     pqItem

	// prepared prev state
	prevPrepared bool
	prevItem     pqItem
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
	if m.nextPrepared {
		return true
	}
	if m.err != nil {
		return false
	}
	if m.fwd.Len() == 0 {
		return false
	}
	m.prepareNext()
	return m.nextPrepared
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
	item := m.nextItem
	m.nextPrepared = false
	return item.rec, nil
}

// HasPrev implements Iterator[Record].
func (m *MergingIterator) HasPrev() bool {
	if m.prevPrepared {
		return true
	}
	if m.err != nil {
		return false
	}
	if m.rev.Len() == 0 {
		return false
	}
	m.preparePrev()
	return m.prevPrepared
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
	item := m.prevItem
	m.prevPrepared = false
	if m.err != nil {
		return item.rec, m.err
	}
	return item.rec, nil
}

// prepareNext stages the next item so that HasNext is idempotent.
func (m *MergingIterator) prepareNext() {
	if m.nextPrepared {
		return
	}
	m.prevPrepared = false

	if m.err != nil || m.fwd.Len() == 0 {
		return
	}

	item := m.fwd.PopItem()
	m.rev.PushItem(item)

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

	m.nextItem = item
	m.nextPrepared = true
}

// preparePrev stages the previous item so that HasPrev is idempotent.
func (m *MergingIterator) preparePrev() {
	if m.prevPrepared {
		return
	}
	m.nextPrepared = false

	if m.err != nil || m.rev.Len() == 0 {
		return
	}

	curItem := m.rev.PopItem()
	_, err := curItem.iter.Prev()
	switch {
	case err == nil:
		m.fwd.PushItem(curItem)
	case errors.Is(err, EOI):
		// Return the record without surfacing EOI.
	default:
		m.err = err
	}

	m.prevItem = curItem
	m.prevPrepared = true
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
