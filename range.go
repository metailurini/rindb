package rindb

import "errors"

// RangeIterator is a user-facing iterator that hides tombstones and
// duplicates. It wraps a MergingIterator which provides all records in key and
// sequence order.
type RangeIterator struct {
	mi         *MergingIterator
	lastKey    Bytes
	lastKeySet bool
	next       Record
	prepared   bool
	err        error
}

// NewRangeIterator creates a new RangeIterator from a MergingIterator.
func NewRangeIterator(mi *MergingIterator) *RangeIterator {
	return &RangeIterator{mi: mi}
}

func (r *RangeIterator) prepare() {
	for !r.prepared && r.err == nil {
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
		r.prepared = true
	}
}

// HasNext implements Iterator[Record].
func (r *RangeIterator) HasNext() bool {
	r.prepare()
	return r.prepared
}

// Next implements Iterator[Record].
func (r *RangeIterator) Next() (Record, error) {
	if !r.HasNext() {
		var empty Record
		if r.err != nil {
			return empty, r.err
		}
		return empty, EOI
	}
	r.prepared = false
	return r.next, nil
}

// Close releases any resources held by the iterator.
func (r *RangeIterator) Close() error {
	return r.mi.Close()
}
