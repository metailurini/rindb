package rindb

import "errors"

type Direction int

const (
	DirForward Direction = iota
	DirReverse
)

func directionFromOrder(order RangeOrder) Direction {
	if order == RangeAsc {
		return DirForward
	}
	return DirReverse
}

func oppositeDirection(dir Direction) Direction {
	if dir == DirForward {
		return DirReverse
	}
	return DirForward
}

type cursorCandidate struct {
	record     Record
	stagedItem pqItem
	peeked     bool
}

type rangeCursor struct {
	it *MergingIterator

	reverseCached Record
	reversePrimed bool
	reverseErr    error
}

// newRangeCursor creates a new range cursor that wraps a MergingIterator.
func newRangeCursor(mi *MergingIterator) *rangeCursor {
	return &rangeCursor{it: mi}
}

// ensureReversePrimed populates the reverse-iteration cache if it's not already
// populated. It calls peekReverse on the underlying MergingIterator to get the
// next record without advancing the iterator, storing it in reverseCached. This
// is essential for reverse traversal, especially when collapsing records.
func (c *rangeCursor) ensureReversePrimed() error {
	if c.reversePrimed || c.reverseErr != nil {
		return c.reverseErr
	}
	rec, err := c.it.peekReverse()
	if err != nil {
		c.reverseErr = err
		return err
	}
	c.reverseCached = rec
	c.reversePrimed = true
	return nil
}

// consumePeekedReverse consumes the cached reverse-peeked record from the
// underlying MergingIterator and resets the reverse cache state. It returns the
// underlying priority queue item and a flag indicating success.
func (c *rangeCursor) consumePeekedReverse() (pqItem, bool) {
	item, ok := c.it.consumePeekedReverse()
	c.reversePrimed = false
	c.reverseCached = nil
	c.reverseErr = nil
	if !ok {
		return pqItem{}, false
	}
	return item, true
}

// collapseDescendingRun processes a sequence of records with the same key during
// reverse iteration. It starts with a seed record and iterates backwards,
// consuming all versions of that key to find the one with the highest sequence
// number. If the most recent version is a deletion record, it returns no record.
// This ensures that only the latest, non-deleted version of a key is returned.
func (c *rangeCursor) collapseDescendingRun(seed Record, seedItem pqItem) (Record, pqItem, bool, error) {
	key := seed.GetKey()
	candidate := seed
	candidateItem := seedItem

	for {
		if err := c.ensureReversePrimed(); err != nil {
			if errors.Is(err, EOI) {
				break
			}
			return nil, pqItem{}, false, err
		}
		if !c.reversePrimed || c.reverseCached == nil {
			break
		}
		next := c.reverseCached
		if next.GetKey().Compare(key) != CmpEqual {
			break
		}

		candidateUpdated := next.GetSequenceNumber() > candidate.GetSequenceNumber()
		item, ok := c.consumePeekedReverse()
		if candidateUpdated && ok {
			candidate = next
			candidateItem = item
		}
		if !ok {
			break
		}
	}

	if candidate.GetType() == TypeDeletion {
		return nil, pqItem{}, false, nil
	}

	return candidate, candidateItem, true, nil
}

// next retrieves the next candidate record in the specified direction. If the
// direction is reverse and collapse is true, it also handles key collapsing.
func (c *rangeCursor) next(dir Direction, collapse bool) (cursorCandidate, bool, error) {
	if dir == DirReverse {
		return c.nextReverse(collapse)
	}
	return c.nextForward()
}

// nextForward retrieves the next record in the forward direction from the
// underlying MergingIterator. It does not perform any collapsing.
func (c *rangeCursor) nextForward() (cursorCandidate, bool, error) {
	rec, err := c.it.Next()
	if err != nil {
		if errors.Is(err, EOI) {
			return cursorCandidate{}, false, EOI
		}
		return cursorCandidate{}, false, err
	}
	return cursorCandidate{record: rec}, true, nil
}

// nextReverse retrieves the next record in the reverse direction. If collapse
// is true, it invokes collapseDescendingRun to ensure only the latest version of
// a key is returned. Otherwise, it returns the next raw record.
func (c *rangeCursor) nextReverse(collapse bool) (cursorCandidate, bool, error) {
	if err := c.ensureReversePrimed(); err != nil {
		if errors.Is(err, EOI) {
			return cursorCandidate{}, false, EOI
		}
		return cursorCandidate{}, false, err
	}
	if !c.reversePrimed || c.reverseCached == nil {
		return cursorCandidate{}, false, nil
	}
	seed := c.reverseCached
	item, ok := c.consumePeekedReverse()
	if !ok {
		return cursorCandidate{}, false, nil
	}
	candidate := cursorCandidate{peeked: true}
	if !collapse {
		candidate.record = seed
		candidate.stagedItem = item
		return candidate, true, nil
	}
	rec, stagedItem, okCollapse, err := c.collapseDescendingRun(seed, item)
	if err != nil {
		return cursorCandidate{}, false, err
	}
	if okCollapse {
		candidate.record = rec
		candidate.stagedItem = stagedItem
		return candidate, true, nil
	}
	return candidate, false, nil
}

// stageForPrev pushes a priority queue item back into the MergingIterator's
// forward queue. This is used to make a record available for a subsequent Prev()
// call, which is important for oscillating iterators (Next -> Prev).
func (c *rangeCursor) stageForPrev(item pqItem) {
	if item.rec == nil || item.iter == nil {
		return
	}
	c.it.stageForPrev(item)
}

// reverseError returns any error encountered during reverse iteration.
func (c *rangeCursor) reverseError() error {
	return c.reverseErr
}

// clearReverseError resets the reverse iteration error state.
func (c *rangeCursor) clearReverseError() {
	c.reverseErr = nil
}

// resetReverse clears the entire reverse iteration cache, including the peeked
// record and any associated error. This is called when the iterator is reset or
// moves to a new position non-sequentially (e.g., Last).
func (c *rangeCursor) resetReverse() {
	c.reversePrimed = false
	c.reverseCached = nil
	c.reverseErr = nil
}

// currentReverseCandidate returns the currently cached record for reverse
// iteration without consuming it.
func (c *rangeCursor) currentReverseCandidate() Record {
	return c.reverseCached
}
