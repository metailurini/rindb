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
	mi                *MergingIterator
	cursor            *rangeCursor
	filter            *recordFilter
	anchors           *anchorState
	lastDir           Direction
	lastEmitted       Record
	lastKey           Bytes
	lastKeySet        bool
	next              Record
	prev              Record
	nextPrepared      bool
	prevPrepared      bool
	err               error
	forward           bool
	crossingAnchor    Record
	crossingAnchorSet bool
	order             RangeOrder
}

// NewRangeIterator creates a new RangeIterator from a MergingIterator.
func NewRangeIterator(mi *MergingIterator, order RangeOrder) *RangeIterator {
	cursor := newRangeCursor(mi)
	ri := &RangeIterator{
		mi:      mi,
		cursor:  cursor,
		filter:  newRecordFilter(nil),
		anchors: &anchorState{},
		order:   order,
		forward: order != RangeDesc,
	}
	if order == RangeDesc {
		if err := cursor.ensureReversePrimed(); err != nil && !errors.Is(err, EOI) {
			ri.err = err
		}
	}
	return ri
}

func (r *RangeIterator) advance(dir Direction, pull func(*rangeCursor) (Record, bool, error)) (Record, bool, error) {
	if changed := r.anchors.OnDirectionChange(dir, r.lastEmitted); changed {
		r.filter.Reset()
	}

	if rec, ok := r.anchors.popPending(); ok {
		r.filter.MarkEmitted(rec, dir)
		r.lastEmitted = rec
		r.lastDir = dir
		return rec, true, nil
	}

	for {
		rec, ok, err := pull(r.cursor)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			r.lastDir = dir
			return nil, false, nil
		}
		if accepted, ok := r.filter.Accept(rec, dir); ok {
			r.lastEmitted = accepted
			r.lastDir = dir
			return accepted, true, nil
		}
	}
}

func (r *RangeIterator) allowAnchorOnNext() bool {
	if r.order == RangeDesc {
		return r.forward
	}
	return !r.forward
}

func (r *RangeIterator) allowAnchorOnPrevForOrder(order RangeOrder) bool {
	if order == RangeDesc {
		return !r.forward
	}
	return r.forward
}

func (r *RangeIterator) primeNext() {
	if r.order == RangeDesc && r.forward && r.crossingAnchorSet {
		r.next = r.crossingAnchor
		r.nextPrepared = true
		r.crossingAnchorSet = false
		return
	}
	direction := directionFromOrder(r.order)
	for !r.nextPrepared && r.err == nil {
		collapse := r.order == RangeDesc && r.err == nil && !r.forward
		candidate, ok, err := r.cursor.next(direction, collapse)
		if err != nil {
			if errors.Is(err, EOI) {
				return
			}
			r.err = err
			return
		}
		if !ok {
			if candidate.peeked {
				r.forward = false
			}
			if direction == DirReverse {
				continue
			}
			return
		}

		rec := candidate.record
		peeked := candidate.peeked

		sameKey := r.lastKeySet && rec.GetKey().Compare(r.lastKey) == CmpEqual
		if sameKey {
			if r.allowAnchorOnNext() && r.matchesCrossingAnchor(rec) {
				r.crossingAnchorSet = false
			} else {
				if peeked {
					// Ensure the discarded candidate remains available for Prev()
					// so oscillating at the boundary can resurface the prior key.
					r.cursor.stageForPrev(candidate.stagedItem)
					r.forward = false
				}
				continue
			}
		}
		r.lastKey = rec.GetKey().Clone()
		r.lastKeySet = true
		if rec.GetType() == TypeDeletion {
			if peeked {
				r.forward = false
			}
			continue
		}
		if peeked {
			r.cursor.stageForPrev(candidate.stagedItem)
			r.forward = false
		}
		r.next = rec
		r.nextPrepared = true
	}
	if r.nextPrepared {
		r.prevPrepared = false
	}
}

func (r *RangeIterator) primePrev() {
	r.primePrevWithOrder(r.order)
}

func (r *RangeIterator) primePrevWithOrder(order RangeOrder) {
	if order == RangeDesc && !r.forward && r.crossingAnchorSet {
		r.prev = r.crossingAnchor
		r.prevPrepared = true
		r.crossingAnchorSet = false
		return
	}
	for !r.prevPrepared && r.err == nil {
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
	if r.prevPrepared {
		r.nextPrepared = false
	}
}

func (r *RangeIterator) stagePrevCandidate(rec Record, order RangeOrder) bool {
	sameKey := r.lastKeySet && rec.GetKey().Compare(r.lastKey) == CmpEqual
	if sameKey {
		if r.allowAnchorOnPrevForOrder(order) && r.matchesCrossingAnchor(rec) {
			r.crossingAnchorSet = false
		} else if r.nextPrepared && recordsEqual(rec, r.next) {
			// A forward peek staged this record; treat it as the anchor so Prev can surface it.
			r.crossingAnchor = rec
			r.crossingAnchorSet = true
		} else {
			return false
		}
	}
	r.lastKey = rec.GetKey().Clone()
	r.lastKeySet = true
	if rec.GetType() == TypeDeletion {
		return false
	}
	r.prev = rec
	r.prevPrepared = true
	r.nextPrepared = false
	return true
}

// HasNext implements Iterator[Record].
func (r *RangeIterator) HasNext() bool {
	r.primeNext()
	return r.nextPrepared
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
	r.forward = r.order != RangeDesc
	r.crossingAnchor = rec
	r.crossingAnchorSet = true
	return rec, nil
}

// HasPrev implements Iterator[Record].
func (r *RangeIterator) HasPrev() bool {
	r.primePrev()
	return r.prevPrepared
}

func (r *RangeIterator) pullNextPrepared(*rangeCursor) (Record, bool, error) {
	if !r.nextPrepared {
		r.primeNext()
	}
	if !r.nextPrepared {
		if r.err != nil {
			return nil, false, r.err
		}
		return nil, false, nil
	}
	candidate := r.next
	r.nextPrepared = false
	return candidate, true, nil
}

// Prev implements Iterator[Record].
func (r *RangeIterator) Prev() (Record, error) {
	dir := oppositeDirection(directionFromOrder(r.order))
	rec, ok, err := r.advance(dir, r.newPrevPuller(r.order))
	if err != nil {
		return nil, err
	}
	if !ok {
		if r.err != nil {
			return nil, r.err
		}
		return nil, EOI
	}
	r.forward = r.order == RangeDesc
	r.crossingAnchor = rec
	r.crossingAnchorSet = true
	return rec, nil
}

func (r *RangeIterator) newPrevPuller(order RangeOrder) func(*rangeCursor) (Record, bool, error) {
	return func(*rangeCursor) (Record, bool, error) {
		if !r.prevPrepared {
			r.primePrevWithOrder(order)
		}
		if !r.prevPrepared {
			if r.err != nil {
				return nil, false, r.err
			}
			return nil, false, nil
		}
		candidate := r.prev
		r.prevPrepared = false
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

	r.prevPrepared = false
	r.nextPrepared = false
	r.forward = false
	r.crossingAnchorSet = false
	r.prev = nil
	r.next = nil
	r.err = nil
	r.lastKeySet = false
	r.cursor.resetReverse()
	r.lastEmitted = nil
	r.anchors = &anchorState{}
	r.filter.Reset()
	r.filter.dirSet = false

	searchOrder := RangeAsc

	lastValid := Record(nil)
	if rec != nil {
		r.stagePrevCandidate(rec, searchOrder)
	}

	dir := oppositeDirection(directionFromOrder(r.order))
	pullPrev := r.newPrevPuller(searchOrder)

	for {
		current, ok, err := r.advance(dir, pullPrev)
		if err != nil {
			return empty, err
		}
		if !ok {
			if r.err != nil {
				return empty, r.err
			}
			if r.order == RangeDesc && lastValid != nil {
				r.crossingAnchor = lastValid
				r.crossingAnchorSet = true
				return lastValid, nil
			}
			return empty, EOI
		}

		if r.order != RangeDesc {
			r.crossingAnchor = current
			r.crossingAnchorSet = true
			return current, nil
		}

		lastValid = current
	}
}

// Close releases any resources held by the iterator.
func (r *RangeIterator) Close() error {
	return r.mi.Close()
}

func (r *RangeIterator) matchesCrossingAnchor(rec Record) bool {
	return r.crossingAnchorSet && recordsEqual(rec, r.crossingAnchor)
}

func recordsEqual(a, b Record) bool {
	if a == nil || b == nil {
		return false
	}
	return a.GetSequenceNumber() == b.GetSequenceNumber() &&
		a.GetType() == b.GetType() &&
		a.GetKey().Compare(b.GetKey()) == CmpEqual
}

func newEmptyRangeIterator() *RangeIterator {
	mi, err := NewMergingIterator(nil, nil, RangeAsc)
	if err != nil {
		panic(err)
	}
	return NewRangeIterator(mi, RangeAsc)
}
