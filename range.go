package rindb

import "errors"

// RangeIterator is a user-facing iterator that hides tombstones and
// duplicates. It wraps a MergingIterator which provides all records in key and
// sequence order.
type RangeIterator struct {
	mi           *MergingIterator
	lastKey      Bytes
	lastKeySet   bool
	next         Record
	prev         Record
	nextPrepared bool
	prevPrepared bool
	err          error
	forward      bool
}

// NewRangeIterator creates a new RangeIterator from a MergingIterator.
func NewRangeIterator(mi *MergingIterator) *RangeIterator {
	return &RangeIterator{mi: mi, forward: true}
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
		if r.lastKeySet && rec.GetKey().Compare(r.lastKey) == CmpEqual {
			continue
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
		if r.lastKeySet && rec.GetKey().Compare(r.lastKey) == CmpEqual {
			continue
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
	if !r.forward {
		r.lastKeySet = false
	}
	r.prepareNext()
	return r.nextPrepared
}

// Next implements Iterator[Record].
func (r *RangeIterator) Next() (Record, error) {
	if !r.forward {
		r.lastKeySet = false
	}
	if !r.nextPrepared && !r.HasNext() {
		var empty Record
		if r.err != nil {
			return empty, r.err
		}
		return empty, EOI
	}
	r.nextPrepared = false
	r.forward = true
	return r.next, nil
}

// HasPrev implements Iterator[Record].
func (r *RangeIterator) HasPrev() bool {
	if r.forward {
		r.lastKeySet = false
	}
	r.preparePrev()
	return r.prevPrepared
}

// Prev implements Iterator[Record].
func (r *RangeIterator) Prev() (Record, error) {
	if r.forward {
		r.lastKeySet = false
	}
	if !r.prevPrepared && !r.HasPrev() {
		var empty Record
		if r.err != nil {
			return empty, r.err
		}
		return empty, EOI
	}
	r.prevPrepared = false
	r.forward = false
	return r.prev, nil
}

// Close releases any resources held by the iterator.
func (r *RangeIterator) Close() error {
	return r.mi.Close()
}
