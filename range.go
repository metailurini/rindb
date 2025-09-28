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

func (r *RangeIterator) consumePeekedReverse() (pqItem, bool) {
	item, ok := r.mi.consumePeekedReverse()
	r.reversePrimed = false
	r.reverseCached = nil
	r.reverseErr = nil
	if !ok {
		return pqItem{}, false
	}
	return item, true
}

func (r *RangeIterator) collapseDescendingRun(seed Record, seedItem pqItem) (Record, pqItem, bool) {
	key := seed.GetKey()
	candidate := seed
	candidateItem := seedItem

	// Consume all other versions of this key, tracking the one with the highest
	// sequence number.
	for r.err == nil {
		if !r.reversePrimed && r.reverseErr == nil {
			r.ensureReversePrimed()
		}
		if r.reverseErr != nil {
			if !errors.Is(r.reverseErr, EOI) {
				r.err = r.reverseErr
			}
			break
		}
		if !r.reversePrimed || r.reverseCached == nil {
			break
		}
		next := r.reverseCached
		if next.GetKey().Compare(key) != CmpEqual {
			break
		}

		candidateUpdated := next.GetSequenceNumber() > candidate.GetSequenceNumber()
		if candidateUpdated {
			candidate = next
		}
		item, ok := r.consumePeekedReverse()
		if candidateUpdated && ok {
			candidateItem = item
		}
	}

	if candidate.GetType() == TypeDeletion {
		return nil, pqItem{}, false
	}
	return candidate, candidateItem, true
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
		var stagedItem pqItem
		if recFromPeek {
			item, ok := r.consumePeekedReverse()
			if !ok {
				r.forward = false
				continue
			}
			stagedItem = item
			if r.order == RangeDesc && r.err == nil && !r.forward {
				var collapseOK bool
				rec, stagedItem, collapseOK = r.collapseDescendingRun(rec, item)
				if !collapseOK {
					r.forward = false
					continue
				}
			}
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
			r.mi.stageForPrev(stagedItem)
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
			if order == r.order && errors.Is(r.reverseErr, EOI) {
				r.reverseErr = nil
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

	if r.order == RangeDesc {
		for {
			peeked, peekErr := r.mi.peekReverse()
			switch {
			case errors.Is(peekErr, EOI):
				rec = nil
			case peekErr != nil:
				return empty, peekErr
			default:
				item, ok := r.consumePeekedReverse()
				if !ok {
					rec = nil
					break
				}
				collapsed, stagedItem, ok := r.collapseDescendingRun(peeked, item)
				if ok {
					rec = collapsed
					r.mi.stageForPrev(stagedItem)
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
	r.reversePrimed = false
	r.reverseCached = nil
	r.reverseErr = nil

	searchOrder := RangeAsc

	lastValid := Record(nil)
	if rec != nil {
		r.stagePrevCandidate(rec, searchOrder)
	}

	for {
		if !r.prevPrepared {
			r.primePrevWithOrder(searchOrder)
			if !r.prevPrepared {
				if r.err != nil {
					return empty, r.err
				}
				if r.order == RangeDesc && lastValid != nil {
					return lastValid, nil
				}
				return empty, EOI
			}
		}

		current, err := r.Prev()
		if err != nil {
			return empty, err
		}

		if r.order != RangeDesc {
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
