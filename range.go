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
	reverseCached     Record
	reverseErr        error
	reversePrimed     bool
}

// NewRangeIterator creates a new RangeIterator from a MergingIterator.
func NewRangeIterator(mi *MergingIterator, order RangeOrder) *RangeIterator {
	ri := &RangeIterator{mi: mi, order: order, forward: order != RangeDesc}
	if order == RangeDesc {
		rec, err := mi.peekReverse()
		switch {
		case errors.Is(err, EOI):
		// Empty range; leave reversePrimed false so HasNext falls through.
		case err != nil:
			ri.err = err
		default:
			ri.reverseCached = rec
			ri.reversePrimed = true
		}
	}
	return ri
}

func (r *RangeIterator) ensureReversePrimed() {
	if r.reversePrimed || r.err != nil || r.reverseErr != nil {
		return
	}
	rec, err := r.mi.peekReverse()
	switch {
	case errors.Is(err, EOI):
		r.reverseErr = err
	case err != nil:
		r.err = err
	default:
		r.reverseCached = rec
		r.reversePrimed = true
	}
}

func (r *RangeIterator) consumePeekedReverse() {
	r.mi.commitPeekedReverse()
	r.reversePrimed = false
	r.reverseCached = nil
	r.reverseErr = nil
}

func (r *RangeIterator) allowAnchorOnNext() bool {
	return !r.forward
}

func (r *RangeIterator) allowAnchorOnPrev() bool {
	if r.order == RangeDesc {
		return !r.forward
	}
	return r.forward
}

func (r *RangeIterator) primeNext() {
	for !r.nextPrepared && r.err == nil {
		var (
			rec         Record
			recFromPeek bool
		)
		if r.order == RangeDesc {
			if !r.reversePrimed && r.reverseErr == nil {
				r.ensureReversePrimed()
			}
			if r.reverseErr != nil {
				if !errors.Is(r.reverseErr, EOI) {
					r.err = r.reverseErr
				}
				return
			}
			if !r.reversePrimed {
				return
			}
			rec = r.reverseCached
			recFromPeek = true
		} else {
			var err error
			rec, err = r.mi.Next()
			switch {
			case errors.Is(err, EOI):
				return
			case err != nil:
				r.err = err
				return
			}
		}

		peeked := recFromPeek
		if recFromPeek {
			r.consumePeekedReverse()
		}

		sameKey := r.lastKeySet && rec.GetKey().Compare(r.lastKey) == CmpEqual
		if sameKey {
			if r.allowAnchorOnNext() && r.matchesCrossingAnchor(rec) {
				r.crossingAnchorSet = false
			} else {
				if peeked {
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
	for !r.prevPrepared && r.err == nil {
		var (
			rec Record
			err error
		)
		if r.order == RangeDesc {
			rec, err = r.mi.Next()
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

		sameKey := r.lastKeySet && rec.GetKey().Compare(r.lastKey) == CmpEqual
		if sameKey {
			if r.allowAnchorOnPrev() && r.matchesCrossingAnchor(rec) {
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
	if r.order == RangeDesc {
		if !r.reversePrimed && r.reverseErr == nil {
			r.ensureReversePrimed()
		}
		if r.reverseErr != nil && r.err == nil && !errors.Is(r.reverseErr, EOI) {
			r.err = r.reverseErr
		}
		return r.reversePrimed && r.err == nil
	}
	r.primeNext()
	return r.nextPrepared
}

// Next implements Iterator[Record].
func (r *RangeIterator) Next() (Record, error) {
	if !r.nextPrepared {
		r.primeNext()
	}
	if !r.nextPrepared {
		var empty Record
		if r.err != nil {
			return empty, r.err
		}
		if r.order == RangeDesc && errors.Is(r.reverseErr, EOI) {
			r.reverseErr = nil
			return empty, EOI
		}
		return empty, EOI
	}
	r.nextPrepared = false
	r.forward = r.order != RangeDesc
	rec := r.next
	r.crossingAnchor = rec
	r.crossingAnchorSet = true
	return rec, nil
}

// HasPrev implements Iterator[Record].
func (r *RangeIterator) HasPrev() bool {
	r.primePrev()
	return r.prevPrepared
}

// Prev implements Iterator[Record].
func (r *RangeIterator) Prev() (Record, error) {
	if !r.prevPrepared {
		r.primePrev()
	}
	if !r.prevPrepared {
		var empty Record
		if r.err != nil {
			return empty, r.err
		}
		return empty, EOI
	}
	r.prevPrepared = false
	r.forward = r.order == RangeDesc
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
	r.reversePrimed = false
	r.reverseCached = nil
	r.reverseErr = nil

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
