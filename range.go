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
	var (
		empty Record
		last  Record
	)

	dir := r.nextDirection()
	r.invalidate(dir)
	r.invalidate(oppositeDirection(dir))
	r.anchors = anchorState{}
	prevDir := r.prevDirection()

	for {
		rec, err := r.Next()
		if err != nil {
			if errors.Is(err, EOI) {
				if last == nil {
					return empty, EOI
				}
				r.filter.MarkEmitted(last, prevDir)
				r.anchors = anchorState{lastDir: prevDir, dirSet: true}
				r.lastEmitted = last
				r.invalidate(dir)
				r.invalidate(oppositeDirection(dir))
				return last, nil
			}
			return empty, err
		}
		last = rec
	}
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
	if changed := r.anchors.OnDirectionChange(dir, r.lastEmitted); changed {
		if rec, ok := r.anchors.popPending(); ok {
			r.filter.MarkEmitted(rec, dir)
			return rec, nil
		}
	}

	if rec, ok := r.anchors.popPending(); ok {
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
				if candidate.record == nil {
					return nil, false, EOI
				}
				if r.lastEmitted == nil {
					return nil, false, EOI
				}
				if candidate.record.GetKey().Compare(r.lastEmitted.GetKey()) != CmpGreater {
					return nil, false, EOI
				}
			}
			return candidate.record, true, nil
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
