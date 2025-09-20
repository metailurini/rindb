package rindb

import (
	"errors"
)

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

	// forward tracks the last direction of travel. It starts in forward
	// mode because the iterator is positioned before the first element.
	forward bool

	// crossingAnchor holds the last surfaced record so we can detect the
	// direction change boundary and avoid surfacing the same record more
	// than once when oscillating between Next and Prev.
	crossingAnchor    pqItem
	crossingAnchorSet bool
}

// NewMergingIterator constructs a MergingIterator over provided iterators.
// The optional cleanup function is called when Close is invoked.
func NewMergingIterator(iterators []Iterator[Record], cleanup func()) (*MergingIterator, error) {
	// lessFwd defines the comparison logic for the forward priority queue.
	// It determines the order in which records are retrieved when iterating forward.
	//
	// The primary sorting key is the record's Key, in ascending order.
	// If two records have the same Key, their sequence numbers are used as a
	// secondary sorting key, in descending order (higher sequence number first).
	//
	// This ensures that when iterating forward, we prioritize:
	// 1. Records with smaller keys first.
	// 2. For the same key, newer records (higher sequence numbers) first.
	//
	// Example scenarios for lessFwd(a, b):
	// - a = {key: "apple", seq: 10}, b = {key: "banana", seq: 5}
	//   Comparison: a.key ("apple") < b.key ("banana"). Result: true (a is "less").
	// - a = {key: "apple", seq: 10}, b = {key: "apple", seq: 5}
	//   Comparison: a.key == b.key. Then a.seq (10) > b.seq (5). Result: true (a is "less").
	// - a = {key: "apple", seq: 5}, b = {key: "apple", seq: 10}
	//   Comparison: a.key == b.key. Then a.seq (5) is not > b.seq (10). Result: false (a is NOT "less").
	lessFwd := func(a, b pqItem) bool {
		cmp := a.rec.GetKey().Compare(b.rec.GetKey())
		if cmp == CmpEqual {
			return a.rec.GetSequenceNumber() > b.rec.GetSequenceNumber()
		}
		return cmp == CmpLess
	}

	// lessRev defines the comparison logic for the reverse priority queue.
	// It determines the order in which records are retrieved when iterating backward.
	//
	// The primary sorting key is the record's Key, in descending order.
	// If two records have the same Key, their sequence numbers are used as a
	// secondary sorting key, in descending order (higher sequence number first).
	//
	// This ensures that when iterating backward, we prioritize:
	// 1. Records with larger keys first.
	// 2. For the same key, newer records (higher sequence numbers) first.
	//
	// Example scenarios for lessRev(a, b):
	// - a = {key: "banana", seq: 5}, b = {key: "apple", seq: 10}
	//   Comparison: a.key ("banana") > b.key ("apple"). Result: true (a is "less").
	// - a = {key: "apple", seq: 10}, b = {key: "apple", seq: 5}
	//   Comparison: a.key == b.key. Then a.seq (10) > b.seq (5). Result: true (a is "less").
	// - a = {key: "apple", seq: 5}, b = {key: "apple", seq: 10}
	//   Comparison: a.key == b.key. Then a.seq (5) is not > b.seq (10). Result: false (a is NOT "less").
	lessRev := func(a, b pqItem) bool {
		cmp := a.rec.GetKey().Compare(b.rec.GetKey())
		if cmp == CmpEqual {
			return a.rec.GetSequenceNumber() > b.rec.GetSequenceNumber()
		}
		return cmp == CmpGreater
	}
	fwd := NewPriorityQueue(lessFwd)
	rev := NewPriorityQueue(lessRev)
	for _, it := range iterators {
		if it.HasNext() {
			rec, err := it.Next()
			switch {
			case err == nil:
				// No error, continue processing
			case errors.Is(err, EOI):
				// End of iteration, skip to next iterator
				continue
			default:
				// Actual error occurred
				if cleanup != nil {
					cleanup()
				}
				return nil, err
			}
			fwd.PushItem(pqItem{rec: rec, iter: it})
		}
	}
	return &MergingIterator{fwd: fwd, rev: rev, cleanup: cleanup, forward: true}, nil
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
	m.prevPrepared = false // Invalidate prev on forward movement.
	m.forward = true
	m.crossingAnchor = item
	m.crossingAnchorSet = true
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
	m.nextPrepared = false // Invalidate next on backward movement.
	m.forward = false
	m.crossingAnchor = item
	m.crossingAnchorSet = true
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

	// When moving forward, any previously prepared backward state is invalidated.
	m.prevPrepared = false

	for !m.nextPrepared && m.err == nil {
		if m.fwd.Len() == 0 {
			return
		}

		item := m.fwd.PopItem()
		if m.matchesCrossingAnchor(item) {
			if !m.forward {
				// Allow the boundary element to surface once when
				// changing direction.
				m.crossingAnchorSet = false
			} else {
				continue
			}
		}

		m.rev.PushItem(item)

		if item.iter.HasNext() {
			rec, err := item.iter.Next()
			switch {
			case err == nil:
				m.fwd.PushItem(pqItem{rec: rec, iter: item.iter})
			case errors.Is(err, EOI):
				// End of iteration for this underlying iterator, do nothing.
			default:
				m.err = err
				// Preserve the currently prepared item; the error
				// will surface on the next HasNext/Next call.
			}
		}

		m.nextItem = item
		m.nextPrepared = true
	}
}

// preparePrev stages the previous item so that HasPrev is idempotent.
func (m *MergingIterator) preparePrev() {
	if m.prevPrepared {
		return
	}

	// When moving backward, any previously prepared forward state is invalidated.
	m.nextPrepared = false

	for !m.prevPrepared && m.err == nil {
		if m.rev.Len() == 0 {
			return
		}

		curItem := m.rev.PopItem()
		if m.matchesCrossingAnchor(curItem) {
			if m.forward {
				m.crossingAnchorSet = false
			} else {
				continue
			}
		}

		_, err := curItem.iter.Prev()
		if err != nil && !errors.Is(err, EOI) {
			m.err = err
			m.prevItem = curItem
			m.prevPrepared = true
			return
		}

		// If Prev() succeeds, the underlying iterator moved back. If it returns EOI,
		// it's at the beginning. In both cases, the current item should be requeued so
		// that subsequent Next calls can surface it again.
		m.fwd.PushItem(curItem)

		m.prevItem = curItem
		m.prevPrepared = true
	}
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

func (m *MergingIterator) matchesCrossingAnchor(item pqItem) bool {
	if !m.crossingAnchorSet {
		return false
	}
	if m.crossingAnchor.rec == nil || item.rec == nil {
		return false
	}
	if m.crossingAnchor.iter != item.iter {
		return false
	}
	if item.rec.GetSequenceNumber() != m.crossingAnchor.rec.GetSequenceNumber() {
		return false
	}
	if item.rec.GetType() != m.crossingAnchor.rec.GetType() {
		return false
	}
	if item.rec.GetKey().Compare(m.crossingAnchor.rec.GetKey()) != CmpEqual {
		return false
	}
	return true
}
