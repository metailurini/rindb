package rindb

import "errors"

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

type preparedState struct {
	record Record
	err    error
	ready  bool
}

// RangeIterator is a user-facing iterator that hides tombstones and duplicates.
// It wraps a MergingIterator which provides all records in key and sequence
// order.
type RangeIterator struct {
	mi      *MergingIterator
	cursor  *rangeCursor
	filter  *recordFilter
	anchors anchorState
	order   RangeOrder

	lastEmitted Record
	prepared    [2]preparedState
}

// NewRangeIterator creates a new RangeIterator from a MergingIterator.
func NewRangeIterator(mi *MergingIterator, order RangeOrder) *RangeIterator {
	return &RangeIterator{
		mi:     mi,
		cursor: newRangeCursor(mi),
		filter: newRecordFilter(nil),
		order:  order,
	}
}

// HasNext implements Iterator[Record].
func (r *RangeIterator) HasNext() bool {
	dir := r.nextDirection()
	state := r.prepare(dir, r.pullFor(dir))
	return state.err == nil
}

// Next implements Iterator[Record].
func (r *RangeIterator) Next() (Record, error) {
	dir := r.nextDirection()
	state := r.prepare(dir, r.pullFor(dir))
	defer r.invalidate(dir)
	if state.err != nil {
		var empty Record
		return empty, state.err
	}
	rec := state.record
	r.lastEmitted = rec
	r.invalidate(oppositeDirection(dir))
	return rec, nil
}

// HasPrev implements Iterator[Record].
func (r *RangeIterator) HasPrev() bool {
	dir := r.prevDirection()
	state := r.prepare(dir, r.pullFor(dir))
	return state.err == nil
}

// Prev implements Iterator[Record].
func (r *RangeIterator) Prev() (Record, error) {
	dir := r.prevDirection()
	state := r.prepare(dir, r.pullFor(dir))
	defer r.invalidate(dir)
	if state.err != nil {
		var empty Record
		return empty, state.err
	}
	rec := state.record
	r.lastEmitted = rec
	r.invalidate(oppositeDirection(dir))
	return rec, nil
}

// Last implements Iterator[Record].
func (r *RangeIterator) Last() (Record, error) {
	var empty Record

	// Reset any prepared state so Last can reposition the iterator without
	// leaking cached reads from prior traversal.
	r.invalidate(DirForward)
	r.invalidate(DirReverse)
	r.anchors = anchorState{}
	r.cursor = newRangeCursor(r.mi)

	// Position the underlying merging iterator at the logical tail.
	if _, err := r.mi.Last(); err != nil {
		return empty, err
	}

	pullDir := r.prevDirection()
	filterDir := pullDir
	if r.order == RangeDesc {
		pullDir = DirForward
		filterDir = DirReverse
	}
	pull := r.pullFor(pullDir)
	if r.order == RangeDesc && pullDir == DirForward {
		pull = r.pullDescendingTail()
	}

	consumeAll := r.order == RangeDesc
	var rec Record
	for {
		candidate, ok, err := pull(r.cursor)
		if err != nil {
			if errors.Is(err, EOI) {
				break
			}
			return empty, err
		}
		if !ok {
			continue
		}
		if candidate == nil {
			continue
		}
		if accepted, ok := r.filter.Accept(candidate, filterDir); ok {
			rec = accepted
			if !consumeAll {
				break
			}
		}
	}

	if rec == nil {
		return empty, EOI
	}

	if consumeAll && r.cursor.it.fwd.Len() > 0 {
		_ = r.cursor.it.fwd.PopItem()
	}

	r.lastEmitted = rec
	r.anchors = anchorState{lastDir: pullDir, dirSet: true}
	r.invalidate(DirForward)
	r.invalidate(DirReverse)
	return rec, nil
}

// Close releases any resources held by the iterator.
func (r *RangeIterator) Close() error {
	return r.mi.Close()
}

func (r *RangeIterator) prepare(dir Direction, pull pullFunc) *preparedState {
	state := &r.prepared[dir]
	if state.ready {
		return state
	}
	rec, err := r.advance(dir, pull)
	if err != nil {
		state.err = err
		state.record = nil
	} else {
		state.record = rec
		state.err = nil
	}
	state.ready = true
	return state
}

func (r *RangeIterator) advance(dir Direction, pull pullFunc) (Record, error) {
	if r.anchors.OnDirectionChange(dir, r.lastEmitted) {
		if rec, ok := r.anchors.popPending(); ok {
			r.filter.MarkEmitted(rec, dir)
			return rec, nil
		}
	} else if rec, ok := r.anchors.popPending(); ok {
		r.filter.MarkEmitted(rec, dir)
		return rec, nil
	}

	for {
		rec, ok, err := pull(r.cursor)
		if err != nil {
			if errors.Is(err, EOI) {
				if dir == DirReverse {
					r.cursor.clearReverseError()
				}
				return nil, EOI
			}
			return nil, err
		}
		if !ok {
			continue
		}
		if accepted, ok := r.filter.Accept(rec, dir); ok {
			return accepted, nil
		}
	}
}

func (r *RangeIterator) pullFor(dir Direction) pullFunc {
	switch dir {
	case DirReverse:
		collapse := r.shouldCollapseReverse(dir)
		return func(c *rangeCursor) (Record, bool, error) {
			candidate, ok, err := c.next(DirReverse, collapse)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, nil
			}
			if candidate.peeked && candidate.record != nil && candidate.record.GetType() != TypeDeletion {
				c.stageForPrev(candidate.stagedItem)
			}
			return candidate.record, true, nil
		}
	default:
		return func(c *rangeCursor) (Record, bool, error) {
			candidate, ok, err := c.next(DirForward, false)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, nil
			}
			if r.order == RangeDesc && !candidate.staged {
				if r.lastEmitted == nil || candidate.record.GetKey().Compare(r.lastEmitted.GetKey()) != CmpGreater {
					return nil, false, EOI
				}
			}
			return candidate.record, true, nil
		}
	}
}

func (r *RangeIterator) pullDescendingTail() pullFunc {
	return func(c *rangeCursor) (Record, bool, error) {
		for {
			if err := c.ensureReversePrimed(); err != nil {
				if errors.Is(err, EOI) {
					return nil, false, EOI
				}
				return nil, false, err
			}
			if !c.reversePrimed || c.reverseCached == nil {
				return nil, false, nil
			}
			seed := c.reverseCached
			item, ok := c.consumePeekedReverse()
			if !ok {
				return nil, false, nil
			}

			candidate := seed
			stagedItem := item
			if r.shouldCollapseReverse(DirReverse) {
				rec, staged, okCollapse, err := c.collapseDescendingRun(seed, item)
				if err != nil {
					return nil, false, err
				}
				if okCollapse {
					candidate = rec
					stagedItem = staged
				} else {
					candidate = nil
				}
			}

			if candidate == nil {
				continue
			}
			if candidate.GetType() != TypeDeletion {
				c.stageForPrev(stagedItem)
			}
			return candidate, true, nil
		}
	}
}

func (r *RangeIterator) shouldCollapseReverse(dir Direction) bool {
	return r.order == RangeDesc && dir == DirReverse
}

func (r *RangeIterator) invalidate(dir Direction) {
	r.prepared[dir] = preparedState{}
}

func (r *RangeIterator) nextDirection() Direction {
	return directionFromOrder(r.order)
}

func (r *RangeIterator) prevDirection() Direction {
	if r.nextDirection() == DirForward {
		return DirReverse
	}
	return DirForward
}

func oppositeDirection(dir Direction) Direction {
	if dir == DirForward {
		return DirReverse
	}
	return DirForward
}

type pullFunc func(*rangeCursor) (Record, bool, error)

func newEmptyRangeIterator() *RangeIterator {
	mi, err := NewMergingIterator(nil, nil, RangeAsc)
	if err != nil {
		panic(err)
	}
	return NewRangeIterator(mi, RangeAsc)
}
