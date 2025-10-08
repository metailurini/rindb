package rindb

import (
	"errors"
)

// RangeOrder represents the initial traversal direction for range iterators.
type RangeOrder int

const (
	// RangeAsc streams keys from smallest to largest.
	RangeAsc RangeOrder = iota
	// RangeDesc streams keys from largest to smallest.
	RangeDesc
)

type rangeConfig struct {
	order       RangeOrder
	snapshotSeq *uint64
}

// rangeDefaultConfig returns the default configuration for a range iterator.
func rangeDefaultConfig() rangeConfig {
	return rangeConfig{order: RangeAsc}
}

// RangeOption configures IRange behaviour.
type RangeOption func(*rangeConfig)

// IRangeOrder sets the initial iteration order for IRange.
func IRangeOrder(order RangeOrder) RangeOption {
	return func(cfg *rangeConfig) {
		cfg.order = order
	}
}

// IRangeSnapshot restricts IRange to records visible at or below seq.
func IRangeSnapshot(seq uint64) RangeOption {
	return func(cfg *rangeConfig) {
		cfg.snapshotSeq = &seq
	}
}

// RangeIterator is a user-facing iterator that hides tombstones and
// duplicates. It wraps a MergingIterator which provides all records in key and
// sequence order.
type RangeIterator struct {
	mi       *MergingIterator
	cursor   *rangeCursor
	filter   *recordFilter
	anchors  *anchorState
	prefetch *prefetchState
	err      error
	order    RangeOrder
}

// NewRangeIterator creates a new RangeIterator from a MergingIterator.
func NewRangeIterator(mi *MergingIterator, order RangeOrder) *RangeIterator {
	cursor := newRangeCursor(mi)
	ri := &RangeIterator{
		mi:       mi,
		cursor:   cursor,
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    order,
	}
	if order == RangeDesc {
		if err := cursor.ensureReversePrimed(); err != nil && !errors.Is(err, EOI) {
			ri.err = err
		}
	}
	return ri
}

// advance moves the iterator in the given direction, pulling records from the
// underlying storage and returning the next record that passes the filter.
// It handles direction changes, pending boundary records, and filtering.
func (r *RangeIterator) advance(dir Direction, pull func(*rangeCursor) (Record, bool, error)) (Record, bool, error) {
	if changed := r.anchors.onDirectionChange(dir); changed {
		r.filter.reset()
	}

	if rec, ok := r.anchors.popPending(dir); ok {
		r.filter.markEmitted(rec, dir)
		r.anchors.markLastEmitted(rec, dir)
		return rec, true, nil
	}

	for {
		rec, ok, err := pull(r.cursor)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		if accepted, ok := r.filter.accept(rec, dir); ok {
			r.anchors.markLastEmitted(accepted, dir)
			return accepted, true, nil
		}
	}
}

// primeNext prefetches the next record in the iterator's primary direction. It
// populates the prefetch buffer with the next valid record that passes the
// filter, ensuring it's ready for the Next() or HasNext() calls. It handles
// tombstones and filters out duplicates.
func (r *RangeIterator) primeNext() {
	direction := directionFromOrder(r.order)
	if r.prefetch.has(direction) {
		return
	}
	filterClone := r.filter.clone()
	for !r.prefetch.has(direction) && r.err == nil {
		collapse := r.shouldCollapseForNext()
		candidate, ok, err := r.cursor.next(direction, collapse)
		if err != nil {
			if errors.Is(err, EOI) {
				return
			}
			r.err = err
			return
		}
		if !ok {
			if direction == DirReverse {
				continue
			}
			return
		}

		rec := candidate.record
		if rec == nil {
			continue
		}
		if rec.GetType() == TypeDeletion {
			filterClone.markEmitted(rec, direction)
			if candidate.peeked {
				r.cursor.stageForPrev(candidate.stagedItem)
			}
			continue
		}
		if _, accepted := filterClone.accept(rec, direction); !accepted {
			if candidate.peeked {
				r.cursor.stageForPrev(candidate.stagedItem)
			}
			continue
		}
		if candidate.peeked {
			// Ensure the discarded candidate remains available for Prev(),
			// so oscillating at the boundary can resurface the prior key.
			r.cursor.stageForPrev(candidate.stagedItem)
		}
		r.prefetch.stage(direction, rec)
	}
	if r.prefetch.has(direction) {
		r.prefetch.clear(oppositeDirection(direction))
	}
}

// primePrev prefetches the next record in the opposite of the iterator's
// primary direction. It populates the prefetch buffer with the next valid
// record, which is used by Prev() and HasPrev().
func (r *RangeIterator) primePrev() {
	r.primePrevWithOrder(r.order)
}

// shouldCollapseForNext determines whether to collapse records for the next
// iteration. In descending order, it returns true unless the last move was
// forward, preventing re-collapsing the same keys.
func (r *RangeIterator) shouldCollapseForNext() bool {
	if r.order != RangeDesc {
		return false
	}
	lastDir, ok := r.anchors.lastDirection()
	if ok && lastDir == DirForward {
		return false
	}
	return true
}

// primePrevWithOrder prefetches the previous record, respecting the given
// iteration order. It populates the prefetch buffer for reverse traversal.
func (r *RangeIterator) primePrevWithOrder(order RangeOrder) {
	dir := oppositeDirection(directionFromOrder(order))
	if r.anchors.hasPending(dir) {
		return
	}
	if r.prefetch.has(dir) {
		return
	}
	for !r.prefetch.has(dir) && r.err == nil {
		var (
			rec Record
			err error
		)
		if order == RangeDesc {
			if r.mi != nil {
				r.mi.forward = false
			}
			rec, err = r.mi.Next()
			if order == r.order && errors.Is(r.cursor.reverseError(), EOI) {
				r.cursor.clearReverseError()
			}
		} else {
			rec, err = r.mi.Prev()
		}
		switch {
		case errors.Is(err, EOI):
			return
		case err != nil:
			r.err = err
			return
		}

		if !r.stagePrevCandidate(rec, order) {
			continue
		}
	}
	if r.prefetch.has(dir) {
		r.prefetch.clear(oppositeDirection(dir))
	}
}

// stagePrevCandidate evaluates a record to determine if it can be staged as a
// candidate for the previous item in the sequence. It returns true if the
// record is valid and staged; otherwise, it returns false.
func (r *RangeIterator) stagePrevCandidate(rec Record, order RangeOrder) bool {
	if rec == nil {
		return false
	}
	dir := oppositeDirection(directionFromOrder(order))
	if rec.GetType() == TypeDeletion {
		r.filter.markEmitted(rec, dir)
		return false
	}
	r.prefetch.stage(dir, rec)
	r.prefetch.clear(oppositeDirection(dir))
	return true
}

// HasNext implements Iterator[Record].
func (r *RangeIterator) HasNext() bool {
	dir := directionFromOrder(r.order)
	if r.anchors.hasPending(dir) {
		return true
	}
	if changed := r.anchors.onDirectionChange(dir); changed {
		r.filter.reset()
		if r.anchors.hasPending(dir) {
			return true
		}
	}
	for {
		// This check-act-check sequence ensures that we attempt to fill the
		// prefetch buffer only when it's empty and correctly handle cases
		// where priming fails (e.g., at the end of the iterator).
		if !r.prefetch.has(dir) {
			// If the prefetch buffer is empty, attempt to prime it with the next
			// available record.
			r.primeNext()
		}
		if !r.prefetch.has(dir) {
			// If the buffer is still empty after priming, it means there are no
			// more records, so we can stop.
			return false
		}
		candidate, ok := r.prefetch.peek(dir)
		if !ok {
			return false
		}
		filterClone := r.filter.clone()
		if _, accepted := filterClone.accept(candidate, dir); accepted {
			return true
		}
		r.prefetch.pop(dir)
	}
}

// Next implements Iterator[Record].
func (r *RangeIterator) Next() (Record, error) {
	dir := directionFromOrder(r.order)
	rec, ok, err := r.advance(dir, r.pullNextPrepared)
	if err != nil {
		return nil, err
	}
	if !ok {
		if r.err != nil {
			return nil, r.err
		}
		if r.order == RangeDesc && errors.Is(r.cursor.reverseError(), EOI) {
			r.cursor.clearReverseError()
			return nil, EOI
		}
		return nil, EOI
	}
	r.anchors.markLastEmitted(rec, dir)
	return rec, nil
}

// HasPrev implements Iterator[Record].
func (r *RangeIterator) HasPrev() bool {
	dir := oppositeDirection(directionFromOrder(r.order))
	if r.anchors.hasPending(dir) {
		return true
	}
	if changed := r.anchors.onDirectionChange(dir); changed {
		r.filter.reset()
		if r.anchors.hasPending(dir) {
			return true
		}
	}
	for {
		// This check-act-check sequence ensures that we attempt to fill the
		// prefetch buffer only when it's empty and correctly handle cases
		// where priming fails (e.g., at the end of the iterator).
		if !r.prefetch.has(dir) {
			// If the prefetch buffer is empty, attempt to prime it with the
			// previous available record.
			r.primePrev()
		}
		if !r.prefetch.has(dir) {
			// If the buffer is still empty after priming, it means there are no
			// more records, so we can stop.
			return false
		}
		candidate, ok := r.prefetch.peek(dir)
		if !ok {
			return false
		}
		filterClone := r.filter.clone()
		if _, accepted := filterClone.accept(candidate, dir); accepted {
			return true
		}
		r.prefetch.pop(dir)
	}
}

// pullNextPrepared retrieves the next record from the prefetch buffer. If the
// buffer is empty, it calls primeNext to populate it. It returns the record,
// a boolean indicating if a record was found, and any error encountered.
func (r *RangeIterator) pullNextPrepared(*rangeCursor) (Record, bool, error) {
	dir := directionFromOrder(r.order)
	// This check-act-check sequence ensures that we attempt to fill the
	// prefetch buffer only when it's empty and correctly handle cases
	// where priming fails (e.g., at the end of the iterator).
	if !r.prefetch.has(dir) {
		// If the prefetch buffer is empty, attempt to prime it with the next
		// available record.
		r.primeNext()
	}
	if !r.prefetch.has(dir) {
		// If the buffer is still empty after priming, it means there are no
		// more records. Return the stored error if any, otherwise stop.
		if r.err != nil {
			return nil, false, r.err
		}
		return nil, false, nil
	}
	candidate, _ := r.prefetch.pop(dir)
	return candidate, true, nil
}

// Prev implements Iterator[Record].
func (r *RangeIterator) Prev() (Record, error) {
	dir := oppositeDirection(directionFromOrder(r.order))
	rec, ok, err := r.advance(dir, r.makePrevPuller(r.order))
	if err != nil {
		return nil, err
	}
	if !ok {
		if r.err != nil {
			return nil, r.err
		}
		return nil, EOI
	}
	r.anchors.markLastEmitted(rec, dir)
	return rec, nil
}

// makePrevPuller returns a function that pulls the previous record from the
// prefetch buffer. If the buffer is empty, it primes it by calling
// primePrevWithOrder.
func (r *RangeIterator) makePrevPuller(order RangeOrder) func(*rangeCursor) (Record, bool, error) {
	return func(*rangeCursor) (Record, bool, error) {
		dir := oppositeDirection(directionFromOrder(order))
		// This check-act-check sequence ensures that we attempt to fill the
		// prefetch buffer only when it's empty and correctly handle cases
		// where priming fails (e.g., at the end of the iterator).
		if !r.anchors.hasPending(dir) && !r.prefetch.has(dir) {
			// If the prefetch buffer is empty, attempt to prime it with the
			// previous available record.
			r.primePrevWithOrder(order)
		}
		if !r.prefetch.has(dir) {
			// If the buffer is still empty after priming, it means there are no
			// more records. Return the stored error if any, otherwise stop.
			if r.err != nil {
				return nil, false, r.err
			}
			return nil, false, nil
		}
		candidate, _ := r.prefetch.pop(dir)
		return candidate, true, nil
	}
}

// Last implements Iterator[Record].
func (r *RangeIterator) Last() (Record, error) {
	var empty Record
	r.err = nil

	rec, err := r.mi.Last()
	if err != nil {
		return empty, err
	}

	if r.order == RangeDesc {
		for {
			peekErr := r.cursor.ensureReversePrimed()
			switch {
			case errors.Is(peekErr, EOI):
				rec = nil
			case peekErr != nil:
				return empty, peekErr
			default:
				peeked := r.cursor.currentReverseCandidate()
				item, ok := r.cursor.consumePeekedReverse()
				if !ok {
					rec = nil
					break
				}
				collapsed, stagedItem, ok, collapseErr := r.cursor.collapseDescendingRun(peeked, item)
				if collapseErr != nil {
					return empty, collapseErr
				}
				if ok {
					rec = collapsed
					r.cursor.stageForPrev(stagedItem)
				} else {
					rec = nil
				}
			}
			if rec != nil {
				break
			}
			if errors.Is(peekErr, EOI) {
				break
			}
		}
	}

	dir := directionFromOrder(r.order)
	r.anchors.clearPending(dir)
	r.anchors.clearPending(oppositeDirection(dir))
	r.prefetch.clearAll()
	r.err = nil
	r.cursor.resetReverse()
	r.anchors.Reset()
	r.filter.reset()

	searchOrder := RangeAsc

	lastValid := Record(nil)
	if rec != nil {
		r.stagePrevCandidate(rec, searchOrder)
	}

	walkDir := oppositeDirection(directionFromOrder(r.order))
	pullPrev := r.makePrevPuller(searchOrder)

	for {
		current, ok, err := r.advance(walkDir, pullPrev)
		if err != nil {
			return empty, err
		}
		if !ok {
			if r.err != nil {
				return empty, r.err
			}
			if r.order == RangeDesc && lastValid != nil {
				r.anchors.markLastEmitted(lastValid, walkDir)
				return lastValid, nil
			}
			return empty, EOI
		}

		if r.order != RangeDesc {
			r.anchors.markLastEmitted(current, walkDir)
			return current, nil
		}

		lastValid = current
	}
}

// Close releases any resources held by the iterator.
func (r *RangeIterator) Close() error {
	return r.mi.Close()
}

func newEmptyRangeIterator() *RangeIterator {
	mi, err := NewMergingIterator(nil, nil, RangeAsc)
	if err != nil {
		panic(err)
	}
	return NewRangeIterator(mi, RangeAsc)
}
