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
	mi       *MergingIterator
	cursor   *rangeCursor
	filter   *recordFilter
	anchors  *anchorState
	prefetch *prefetchState
	err      error
	order    RangeOrder
}

type prefetchState struct {
	records [2]Record
	ready   [2]bool
}

func (p *prefetchState) Has(dir Direction) bool {
	return p.ready[dirIndex(dir)]
}

func (p *prefetchState) Peek(dir Direction) (Record, bool) {
	idx := dirIndex(dir)
	if !p.ready[idx] {
		return nil, false
	}
	return p.records[idx], true
}

func (p *prefetchState) Stage(dir Direction, rec Record) {
	idx := dirIndex(dir)
	if rec == nil {
		p.records[idx] = nil
		p.ready[idx] = false
		return
	}
	p.records[idx] = rec
	p.ready[idx] = true
}

func (p *prefetchState) Pop(dir Direction) (Record, bool) {
	idx := dirIndex(dir)
	if !p.ready[idx] {
		return nil, false
	}
	rec := p.records[idx]
	p.records[idx] = nil
	p.ready[idx] = false
	return rec, true
}

func (p *prefetchState) Clear(dir Direction) {
	idx := dirIndex(dir)
	p.records[idx] = nil
	p.ready[idx] = false
}

func (p *prefetchState) ClearAll() {
	p.Clear(DirForward)
	p.Clear(DirReverse)
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

func (r *RangeIterator) advance(dir Direction, pull func(*rangeCursor) (Record, bool, error)) (Record, bool, error) {
	if changed := r.anchors.OnDirectionChange(dir); changed {
		r.filter.Reset()
	}

	if rec, ok := r.anchors.PopPending(dir); ok {
		r.filter.MarkEmitted(rec, dir)
		r.anchors.MarkLastEmitted(rec, dir)
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
		if accepted, ok := r.filter.Accept(rec, dir); ok {
			r.anchors.MarkLastEmitted(accepted, dir)
			return accepted, true, nil
		}
	}
}

func (r *RangeIterator) primeNext() {
	direction := directionFromOrder(r.order)
	if r.prefetch.Has(direction) {
		return
	}
	for !r.prefetch.Has(direction) && r.err == nil {
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
			r.filter.MarkEmitted(rec, direction)
			if candidate.peeked {
				r.cursor.stageForPrev(candidate.stagedItem)
			}
			continue
		}
		clone := r.filter.clone()
		if _, accepted := clone.Accept(rec, direction); !accepted {
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
		r.prefetch.Stage(direction, rec)
	}
	if r.prefetch.Has(direction) {
		r.prefetch.Clear(oppositeDirection(direction))
	}
}

func (r *RangeIterator) primePrev() {
	r.primePrevWithOrder(r.order)
}

func (r *RangeIterator) shouldCollapseForNext() bool {
	if r.order != RangeDesc {
		return false
	}
	lastDir, ok := r.anchors.LastDirection()
	if ok && lastDir == DirForward {
		return false
	}
	return true
}

func (r *RangeIterator) primePrevWithOrder(order RangeOrder) {
	dir := oppositeDirection(directionFromOrder(order))
	if r.anchors.HasPending(dir) {
		return
	}
	if r.prefetch.Has(dir) {
		return
	}
	for !r.prefetch.Has(dir) && r.err == nil {
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
	if r.prefetch.Has(dir) {
		r.prefetch.Clear(oppositeDirection(dir))
	}
}

func (r *RangeIterator) stagePrevCandidate(rec Record, order RangeOrder) bool {
	if rec == nil {
		return false
	}
	dir := oppositeDirection(directionFromOrder(order))
	if rec.GetType() == TypeDeletion {
		r.filter.MarkEmitted(rec, dir)
		return false
	}
	r.prefetch.Stage(dir, rec)
	r.prefetch.Clear(oppositeDirection(dir))
	return true
}

// HasNext implements Iterator[Record].
func (r *RangeIterator) HasNext() bool {
	dir := directionFromOrder(r.order)
	if r.anchors.HasPending(dir) {
		return true
	}
	if changed := r.anchors.OnDirectionChange(dir); changed {
		r.filter.Reset()
		if r.anchors.HasPending(dir) {
			return true
		}
	}
	for {
		if !r.prefetch.Has(dir) {
			r.primeNext()
		}
		if !r.prefetch.Has(dir) {
			return false
		}
		candidate, ok := r.prefetch.Peek(dir)
		if !ok {
			return false
		}
		clone := r.filter.clone()
		if _, accepted := clone.Accept(candidate, dir); accepted {
			return true
		}
		r.prefetch.Pop(dir)
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
	r.anchors.MarkLastEmitted(rec, dir)
	return rec, nil
}

// HasPrev implements Iterator[Record].
func (r *RangeIterator) HasPrev() bool {
	dir := oppositeDirection(directionFromOrder(r.order))
	if r.anchors.HasPending(dir) {
		return true
	}
	if changed := r.anchors.OnDirectionChange(dir); changed {
		r.filter.Reset()
		if r.anchors.HasPending(dir) {
			return true
		}
	}
	for {
		if !r.prefetch.Has(dir) {
			r.primePrev()
		}
		if !r.prefetch.Has(dir) {
			return false
		}
		candidate, ok := r.prefetch.Peek(dir)
		if !ok {
			return false
		}
		clone := r.filter.clone()
		if _, accepted := clone.Accept(candidate, dir); accepted {
			return true
		}
		r.prefetch.Pop(dir)
	}
}

func (r *RangeIterator) pullNextPrepared(*rangeCursor) (Record, bool, error) {
	dir := directionFromOrder(r.order)
	if !r.prefetch.Has(dir) {
		r.primeNext()
	}
	if !r.prefetch.Has(dir) {
		if r.err != nil {
			return nil, false, r.err
		}
		return nil, false, nil
	}
	candidate, _ := r.prefetch.Pop(dir)
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
	r.anchors.MarkLastEmitted(rec, dir)
	return rec, nil
}

func (r *RangeIterator) makePrevPuller(order RangeOrder) func(*rangeCursor) (Record, bool, error) {
	return func(*rangeCursor) (Record, bool, error) {
		dir := oppositeDirection(directionFromOrder(order))
		if !r.anchors.HasPending(dir) && !r.prefetch.Has(dir) {
			r.primePrevWithOrder(order)
		}
		if !r.prefetch.Has(dir) {
			if r.err != nil {
				return nil, false, r.err
			}
			return nil, false, nil
		}
		candidate, _ := r.prefetch.Pop(dir)
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
	r.anchors.ClearPending(dir)
	r.anchors.ClearPending(oppositeDirection(dir))
	r.prefetch.ClearAll()
	r.err = nil
	r.cursor.resetReverse()
	r.anchors = &anchorState{}
	r.filter.Reset()
	r.filter.haveDir = false

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
				r.anchors.MarkLastEmitted(lastValid, walkDir)
				return lastValid, nil
			}
			return empty, EOI
		}

		if r.order != RangeDesc {
			r.anchors.MarkLastEmitted(current, walkDir)
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
