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

type cursorCandidate struct {
	record     Record
	stagedItem pqItem
	peeked     bool
	staged     bool
}

type rangeCursor struct {
	it *MergingIterator

	reverseCached Record
	reversePrimed bool
	reverseErr    error

	stagedForPrev int
}

func newRangeCursor(mi *MergingIterator) *rangeCursor {
	return &rangeCursor{it: mi}
}

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
		return candidate, candidateItem, true, nil
	}

	return candidate, candidateItem, true, nil
}

func (c *rangeCursor) next(dir Direction, collapse bool) (cursorCandidate, bool, error) {
	if dir == DirReverse {
		return c.nextReverse(collapse)
	}
	return c.nextForward()
}

func (c *rangeCursor) nextForward() (cursorCandidate, bool, error) {
	rec, err := c.it.Next()
	if err != nil {
		if errors.Is(err, EOI) {
			return cursorCandidate{}, false, EOI
		}
		return cursorCandidate{}, false, err
	}
	candidate := cursorCandidate{record: rec}
	if c.stagedForPrev > 0 {
		candidate.staged = true
		c.stagedForPrev--
	}
	return candidate, true, nil
}

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

func (c *rangeCursor) stageForPrev(item pqItem) {
	if item.rec == nil || item.iter == nil {
		return
	}
	c.it.stageForPrev(item)
	c.stagedForPrev++
}

func (c *rangeCursor) clearReverseError() {
	c.reverseErr = nil
}
