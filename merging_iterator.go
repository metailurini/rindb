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

	nextPrepared bool
	nextItem     pqItem

	prevPrepared bool
	prevItem     pqItem

	// forward tracks the last direction of travel so boundary handling can
	// decide whether to suppress the crossing anchor.
	forward bool

	// crossingAnchor stores the last surfaced record so oscillating between Next
	// and Prev does not surface the same record twice.
	crossingAnchor    pqItem
	crossingAnchorSet bool

	order            RangeOrder
	iters            []Iterator[Record]
	descendingPrimed bool

	cachedReverse pqItem
	reverseErr    error
	reversePrimed bool
}

// NewMergingIterator constructs a MergingIterator over provided iterators.
// The optional cleanup function is called when Close is invoked.
func NewMergingIterator(iterators []Iterator[Record], cleanup func(), order RangeOrder) (*MergingIterator, error) {
	// lessFwd describes how items are ordered inside the forward priority queue.
	// PriorityQueue is backed by container/heap and behaves like a min-heap: the
	// element considered "less" is promoted to the root and will be the one
	// popped first. We therefore sort primarily by key in ascending order and,
	// for duplicate keys, by sequence number in descending order so newer
	// versions are surfaced before older ones.
	//
	// Visualising the heap: imagine fwd currently holds
	//   [apple@10, apple@5, banana@1].
	// PopItem removes apple@10 because it is the smallest key and the newest
	// version for that key. A subsequent PopItem would yield apple@5 followed by
	// banana@1. This ordering is what Next observes when draining the queue.
	lessFwd := func(a, b pqItem) bool {
		cmp := a.rec.GetKey().Compare(b.rec.GetKey())
		if cmp == CmpEqual {
			return a.rec.GetSequenceNumber() > b.rec.GetSequenceNumber()
		}
		return cmp == CmpLess
	}

	// lessRev performs the same duty for the reverse queue. The heap still
	// treats items for which lessRev returns true as higher priority, so we flip
	// the key comparison: larger keys should appear closer to the root so that
	// popping from rev yields the lexicographically greatest key first. Sequence
	// numbers continue to be ordered descending to keep newer values ahead of
	// older ones.
	//
	// Example state: rev contains [carrot@7, banana@2, apple@4]. PopItem returns
	// carrot@7 because it has the highest key. If we continue popping we would
	// see banana@2 and then apple@4. Prev relies on this ordering when walking
	// backwards through the merged stream.
	lessRev := func(a, b pqItem) bool {
		cmp := a.rec.GetKey().Compare(b.rec.GetKey())
		if cmp == CmpEqual {
			return a.rec.GetSequenceNumber() > b.rec.GetSequenceNumber()
		}
		return cmp == CmpGreater
	}
	fwd := NewPriorityQueue(lessFwd)
	rev := NewPriorityQueue(lessRev)
	active := make([]Iterator[Record], 0, len(iterators))
	for _, it := range iterators {
		if it == nil {
			continue
		}
		active = append(active, it)
	}

	m := &MergingIterator{
		fwd:     fwd,
		rev:     rev,
		cleanup: cleanup,
		forward: order != RangeDesc,
		order:   order,
		iters:   active,
	}

	if err := m.seedForward(active); err != nil {
		if cleanup != nil {
			cleanup()
		}
		return nil, err
	}
	if order == RangeDesc {
		if err := m.seedReverse(active); err != nil {
			if cleanup != nil {
				cleanup()
			}
			return nil, err
		}
	}

	return m, nil
}

func (m *MergingIterator) seedForward(children []Iterator[Record]) error {
	if m.order == RangeDesc {
		return nil
	}
	for _, child := range children {
		if child == nil {
			continue
		}
		if child.HasNext() {
			rec, err := child.Next()
			switch {
			case err == nil:
				// continue
			case errors.Is(err, EOI):
				continue
			default:
				return err
			}
			m.fwd.PushItem(pqItem{rec: rec, iter: child})
		}
	}
	return nil
}

func (m *MergingIterator) seedReverse(children []Iterator[Record]) error {
	for _, child := range children {
		rec, err := child.Last()
		switch {
		case errors.Is(err, EOI):
			continue
		case err != nil:
			return err
		default:
			m.rev.PushItem(pqItem{rec: rec, iter: child})
		}
	}
	return nil
}

func (m *MergingIterator) peekReverse() (Record, error) {
	if m.reversePrimed {
		if m.reverseErr != nil {
			if errors.Is(m.reverseErr, EOI) && m.rev.Len() > 0 {
				m.reversePrimed = false
				m.reverseErr = nil
			} else {
				var empty Record
				return empty, m.reverseErr
			}
		} else {
			return m.cachedReverse.rec, nil
		}
	}

	if m.rev.Len() == 0 {
		m.reverseErr = EOI
		m.reversePrimed = true
		var empty Record
		return empty, m.reverseErr
	}

	m.cachedReverse = m.rev.PeekItem()
	m.reverseErr = nil
	m.reversePrimed = true
	return m.cachedReverse.rec, nil
}

func (m *MergingIterator) commitPeekedReverse() {
	if !m.reversePrimed {
		return
	}
	defer func() {
		m.reversePrimed = false
		m.cachedReverse = pqItem{}
		m.reverseErr = nil
	}()
	if m.reverseErr != nil {
		if !errors.Is(m.reverseErr, EOI) {
			m.err = m.reverseErr
		}
		return
	}

	item := m.rev.PopItem()
	m.fwd.PushItem(item)
	if item.iter.HasPrev() {
		prev, err := item.iter.Prev()
		switch {
		case err == nil:
			m.rev.PushItem(pqItem{rec: prev, iter: item.iter})
		case errors.Is(err, EOI):
			// exhausted
		default:
			m.err = err
		}
	}
	m.forward = false
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

// Last implements Iterator[Record].
func (m *MergingIterator) Last() (Record, error) {
	var empty Record
	if m.err != nil {
		return empty, m.err
	}
	m.nextPrepared = false
	m.prevPrepared = false
	m.forward = false
	m.crossingAnchorSet = false
	m.nextItem = pqItem{}
	m.prevItem = pqItem{}

	if m.fwd != nil {
		m.fwd.Clear()
	}
	if m.rev != nil {
		m.rev.Clear()
	}

	m.descendingPrimed = true

	for _, it := range m.iters {
		if it == nil {
			continue
		}
		rec, err := it.Last()
		switch {
		case err == nil:
			m.rev.PushItem(pqItem{rec: rec, iter: it})
		case errors.Is(err, EOI):
			continue
		default:
			m.err = err
			return empty, err
		}
	}

	if m.rev.Len() == 0 {
		return empty, EOI
	}

	lastItem := m.rev.PopItem()
	if prev, err := lastItem.iter.Prev(); err == nil {
		m.rev.PushItem(pqItem{rec: prev, iter: lastItem.iter})
	} else if !errors.Is(err, EOI) {
		m.err = err
		return empty, err
	}

	m.crossingAnchor = lastItem
	m.crossingAnchorSet = true

	return lastItem.rec, nil
}

// prepareNext stages the next item so that HasNext is idempotent.
func (m *MergingIterator) prepareNext() {
	if m.nextPrepared {
		return
	}

	m.descendingPrimed = false

	// When moving forward, any previously prepared backward state is invalidated.
	m.prevPrepared = false

	// Iterate until we successfully stage the next record or exhaust the
	// queue. Each pass pulls the highest priority element from the forward
	// heap and attempts to prefetch a replacement from the same iterator.
	for !m.nextPrepared && m.err == nil {
		if m.fwd.Len() == 0 {
			return
		}

		// PopItem returns the smallest key (and newest sequence for ties)
		// due to the lessFwd comparator. This is the next candidate record
		// to expose to callers.
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

		// Keep the popped element in the reverse heap so Prev() can walk
		// back across it later.
		m.rev.PushItem(item)
		m.reversePrimed = false
		m.reverseErr = nil

		skipAdvance := m.order == RangeDesc && !m.forward
		if !skipAdvance && item.iter.HasNext() {
			rec, err := item.iter.Next()
			switch {
			case err == nil:
				// Feed the freshly retrieved record into the forward
				// heap so the merged stream remains primed.
				m.fwd.PushItem(pqItem{rec: rec, iter: item.iter})
			case errors.Is(err, EOI):
				// End of iteration for this underlying iterator, do nothing.
			default:
				m.err = err
				// Preserve the currently prepared item; the error
				// will surface on the next HasNext/Next call.
			}
		}

		// Stage the popped item as the prepared result for HasNext/Next.
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

	// Similar to prepareNext, drain the reverse heap until we can surface a
	// record or encounter an error. The reverse heap presents the largest key
	// (newest sequence first) as its next element.
	for !m.prevPrepared && m.err == nil {
		if m.rev.Len() == 0 {
			return
		}

		// PopItem selects the record that should appear when iterating
		// backwards: highest key and newest sequence number first.
		curItem := m.rev.PopItem()
		if m.matchesCrossingAnchor(curItem) {
			if m.forward {
				m.crossingAnchorSet = false
			} else {
				continue
			}
		}

		// Prev returns the current element and rewinds the iterator by one
		// position so another Prev call can continue walking backwards. When
		// descendingPrimed is set, the returned record represents the next
		// candidate for reverse iteration and is pushed onto the reverse heap.
		if m.descendingPrimed {
			prevRec, err := curItem.iter.Prev()
			switch {
			case err == nil:
				m.rev.PushItem(pqItem{rec: prevRec, iter: curItem.iter})
			case errors.Is(err, EOI):
				// No more records from this iterator when walking backwards.
			default:
				m.err = err
				m.prevItem = curItem
				m.prevPrepared = true
				return
			}
		} else {
			if _, err := curItem.iter.Prev(); err != nil && !errors.Is(err, EOI) {
				m.err = err
				m.prevItem = curItem
				m.prevPrepared = true
				return
			}
		}

		// Requeue the current item so a subsequent forward traversal can surface it
		// again. This maintains the invariant that Prev followed by Next returns the
		// same record.
		m.fwd.PushItem(curItem)

		// Store the prepared item for Prev().
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
	return m.crossingAnchorSet &&
		m.crossingAnchor.rec != nil &&
		item.rec != nil &&
		m.crossingAnchor.iter == item.iter &&
		item.rec.GetSequenceNumber() == m.crossingAnchor.rec.GetSequenceNumber() &&
		item.rec.GetType() == m.crossingAnchor.rec.GetType() &&
		item.rec.GetKey().Compare(m.crossingAnchor.rec.GetKey()) == CmpEqual
}
