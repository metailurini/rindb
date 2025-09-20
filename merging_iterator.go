package rindb

import (
	"errors"
	"fmt"
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
}

// NewMergingIterator constructs a MergingIterator over provided iterators.
// The optional cleanup function is called when Close is invoked.
func NewMergingIterator(iterators []Iterator[Record], cleanup func()) (*MergingIterator, error) {
	fmt.Printf("NewMergingIterator: Initializing with %d iterators\n", len(iterators))
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
			fmt.Printf("NewMergingIterator: Pushed initial rec %s from iterator to fwd queue\n", rec.GetKey())
		}
	}
	fmt.Printf("NewMergingIterator: Initialized with fwd queue len %d\n", fwd.Len())
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
	m.prevPrepared = false // Invalidate prev on forward movement.
	fmt.Printf("Next: Returning rec %s\n", item.rec.GetKey())
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
	fmt.Printf("Prev: Returning rec %s\n", item.rec.GetKey())
	if m.err != nil {
		return item.rec, m.err
	}
	return item.rec, nil
}

// prepareNext stages the next item so that HasNext is idempotent.
func (m *MergingIterator) prepareNext() {
	fmt.Printf("prepareNext: start, fwd.Len=%d, rev.Len=%d, nextPrepared=%v, prevPrepared=%v\n", m.fwd.Len(), m.rev.Len(), m.nextPrepared, m.prevPrepared)
	if m.nextPrepared {
		return
	}

	// When moving forward, any previously prepared backward state is invalidated.
	m.prevPrepared = false

	if m.fwd.Len() == 0 {
		fmt.Println("prepareNext: Forward queue is empty, nothing to prepare.")
		return
	}

	item := m.fwd.PopItem()
	fmt.Printf("prepareNext: Popped rec %s from fwd queue\n", item.rec.GetKey())
	m.rev.PushItem(item)
	fmt.Printf("prepareNext: Pushed rec %s to rev queue\n", item.rec.GetKey())

	if item.iter.HasNext() {
		rec, err := item.iter.Next()
		switch {
		case err == nil:
			m.fwd.PushItem(pqItem{rec: rec, iter: item.iter})
			fmt.Printf("prepareNext: Pushed rec %s from underlying iterator back to fwd queue\n", rec.GetKey())
		case errors.Is(err, EOI):
			// End of iteration for this underlying iterator, do nothing.
			fmt.Printf("prepareNext: Underlying iterator for rec %s returned EOI\n", item.rec.GetKey())
		default:
			m.err = err
			fmt.Printf("prepareNext: Underlying iterator for rec %s returned error %v\n", item.rec.GetKey(), err)
		}
	}

	m.nextItem = item
	m.nextPrepared = true
	fmt.Printf("prepareNext: Prepared next item %s\n", m.nextItem.rec.GetKey())
}

// preparePrev stages the previous item so that HasPrev is idempotent.
func (m *MergingIterator) preparePrev() {
	fmt.Printf("preparePrev: start, fwd.Len=%d, rev.Len=%d, nextPrepared=%v, prevPrepared=%v\n", m.fwd.Len(), m.rev.Len(), m.nextPrepared, m.prevPrepared)
	if m.prevPrepared {
		return
	}

	// When moving backward, any previously prepared forward state is invalidated.
	m.nextPrepared = false

	if m.rev.Len() == 0 {
		fmt.Println("preparePrev: Reverse queue is empty, nothing to prepare.")
		return
	}

	curItem := m.rev.PopItem()
	fmt.Printf("preparePrev: Popped rec %s from rev queue\n", curItem.rec.GetKey())
	_, err := curItem.iter.Prev()
	fmt.Printf("preparePrev: Underlying iterator for rec %s Prev() returned err=%v\n", curItem.rec.GetKey(), err)
	switch {
	case err == nil || errors.Is(err, EOI):
		// If Prev() succeeds, the underlying iterator moved back. If it returns EOI,
		// it's at the beginning. In both cases, the current item should be
		// requeued so that subsequent Next calls can surface it again.
		m.fwd.PushItem(curItem)
		fmt.Printf("preparePrev: Pushed rec %s to fwd queue\n", curItem.rec.GetKey())
	default:
		m.err = err
	}

	m.prevItem = curItem
	m.prevPrepared = true
	fmt.Printf("preparePrev: Prepared prev item %s\n", m.prevItem.rec.GetKey())
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
