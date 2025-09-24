package rindb

import (
	"errors"
)

// ErrRangeOrderNotReady indicates that descending range iteration is not yet
// supported. It is returned when callers request RangeDesc in Step 1 of the
// asc/desc rollout plan.
var ErrRangeOrderNotReady = errors.New("descending range iteration is not yet supported")

// RangeOrder represents the initial traversal direction for range iterators.
type RangeOrder int

const (
	// RangeAsc streams keys from smallest to largest.
	RangeAsc RangeOrder = iota
	// RangeDesc streams keys from largest to smallest. Step 1 guards callers
	// behind ErrRangeOrderNotReady until descending support is wired
	// end-to-end.
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
	return &RangeIterator{mi: mi, forward: true, order: order}
}

func (r *RangeIterator) prepareNext() {
	for !r.nextPrepared && r.err == nil {
		rec, err := r.mi.Next()
		if err != nil {
			if errors.Is(err, EOI) {
				return
			}
			r.err = err
			return
		}
		sameKey := r.lastKeySet && rec.GetKey().Compare(r.lastKey) == CmpEqual
		if sameKey {
			if !r.forward && r.matchesCrossingAnchor(rec) {
				r.crossingAnchorSet = false
			} else {
				continue
			}
		}
		r.lastKey = rec.GetKey().Clone()
		r.lastKeySet = true
		if rec.GetType() == TypeDeletion {
			continue
		}
		r.next = rec
		r.nextPrepared = true
	}
	if r.nextPrepared {
		r.prevPrepared = false
	}
}

func (r *RangeIterator) preparePrev() {
	for !r.prevPrepared && r.err == nil {
		rec, err := r.mi.Prev()
		if err != nil {
			if errors.Is(err, EOI) {
				return
			}
			r.err = err
			return
		}
		sameKey := r.lastKeySet && rec.GetKey().Compare(r.lastKey) == CmpEqual
		if sameKey {
			if r.forward && r.matchesCrossingAnchor(rec) {
				r.crossingAnchorSet = false
			} else {
				continue
			}
		}
		r.lastKey = rec.GetKey().Clone()
		r.lastKeySet = true
		if rec.GetType() == TypeDeletion {
			continue
		}
		r.prev = rec
		r.prevPrepared = true
	}
	if r.prevPrepared {
		r.nextPrepared = false
	}
}

// HasNext implements Iterator[Record].
func (r *RangeIterator) HasNext() bool {
	r.prepareNext()
	return r.nextPrepared
}

// Next implements Iterator[Record].
func (r *RangeIterator) Next() (Record, error) {
	if !r.nextPrepared && !r.HasNext() {
		var empty Record
		if r.err != nil {
			return empty, r.err
		}
		return empty, EOI
	}
	r.nextPrepared = false
	r.forward = true
	rec := r.next
	r.crossingAnchor = rec
	r.crossingAnchorSet = true
	return rec, nil
}

// HasPrev implements Iterator[Record].
func (r *RangeIterator) HasPrev() bool {
	r.preparePrev()
	return r.prevPrepared
}

// Prev implements Iterator[Record].
func (r *RangeIterator) Prev() (Record, error) {
	if !r.prevPrepared && !r.HasPrev() {
		var empty Record
		if r.err != nil {
			return empty, r.err
		}
		return empty, EOI
	}
	r.prevPrepared = false
	r.forward = false
	rec := r.prev
	r.crossingAnchor = rec
	r.crossingAnchorSet = true
	return rec, nil
}

// Last implements Iterator[Record].
func (r *RangeIterator) Last() (Record, error) {
	var empty Record
	r.err = nil

	rec, err := r.mi.Last()
	if err != nil {
		return empty, err
	}

	r.prevPrepared = false
	r.nextPrepared = false
	r.forward = false
	r.crossingAnchorSet = false
	r.prev = nil
	r.next = nil
	r.err = nil
	r.lastKeySet = false

	for {
		sameKey := r.lastKeySet && rec.GetKey().Compare(r.lastKey) == CmpEqual
		if sameKey {
			prev, err := r.mi.Prev()
			switch {
			case err == nil:
				rec = prev
			case errors.Is(err, EOI):
				return empty, EOI
			default:
				return empty, err
			}
			continue
		}

		r.lastKey = rec.GetKey().Clone()
		r.lastKeySet = true
		if rec.GetType() == TypeDeletion {
			prev, err := r.mi.Prev()
			switch {
			case err == nil:
				rec = prev
			case errors.Is(err, EOI):
				return empty, EOI
			default:
				return empty, err
			}
			continue
		}

		r.crossingAnchor = rec
		r.crossingAnchorSet = true
		return rec, nil
	}
}

// Close releases any resources held by the iterator.
func (r *RangeIterator) Close() error {
	return r.mi.Close()
}

func (r *RangeIterator) matchesCrossingAnchor(rec Record) bool {
	return r.crossingAnchorSet &&
		r.crossingAnchor != nil &&
		rec.GetSequenceNumber() == r.crossingAnchor.GetSequenceNumber() &&
		rec.GetType() == r.crossingAnchor.GetType() &&
		rec.GetKey().Compare(r.crossingAnchor.GetKey()) == CmpEqual
}

func newEmptyRangeIterator() *RangeIterator {
	mi, err := NewMergingIterator(nil, nil, RangeAsc)
	if err != nil {
		panic(err)
	}
	return NewRangeIterator(mi, RangeAsc)
}
