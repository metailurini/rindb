package rindb

import "errors"

// EOI is end of iteration
//
//lint:ignore ST1012 this is a sentinel error, not a typical error
var EOI = errors.New("EOI")

// Iterator defines the minimal forward-only iteration contract. It is
// exported so callers can provide their own iterator implementations that work
// with Rindb primitives.
type Iterator[T any] interface {
	HasNext() bool
	Next() (T, error)
}

// BiIterator extends Iterator with optional backward traversal support.
// Implementations that are forward-only can embed Iterator and have HasPrev
// return false while Prev returns EOI.
type BiIterator[T any] interface {
	Iterator[T]
	HasPrev() bool
	Prev() (T, error)
}

// forwardIterator adapts a forward-only Iterator into a BiIterator.
type forwardIterator[T any] struct {
	Iterator[T]
}

// HasPrev implements BiIterator for forwardIterator.
func (f *forwardIterator[T]) HasPrev() bool { return false }

// Prev implements BiIterator for forwardIterator.
func (f *forwardIterator[T]) Prev() (T, error) {
	var zero T
	return zero, EOI
}

// asBiIterator ensures the provided iterator satisfies BiIterator by
// wrapping forward-only implementations.
func asBiIterator[T any](it Iterator[T]) BiIterator[T] {
	if bi, ok := any(it).(BiIterator[T]); ok {
		return bi
	}
	return &forwardIterator[T]{Iterator: it}
}

// sliceBiIterator iterates over a fixed slice in both directions.
type sliceBiIterator[T any] struct {
	items   []T
	nextIdx int
	prevIdx int
}

// newSliceBiIterator constructs a bidirectional iterator over the provided slice.
func newSliceBiIterator[T any](items []T) *sliceBiIterator[T] {
	return &sliceBiIterator[T]{items: items, nextIdx: 0, prevIdx: len(items)}
}

// HasNext implements Iterator.
func (s *sliceBiIterator[T]) HasNext() bool {
	return s.nextIdx < len(s.items)
}

// Next implements Iterator.
func (s *sliceBiIterator[T]) Next() (T, error) {
	if !s.HasNext() {
		var zero T
		return zero, EOI
	}
	item := s.items[s.nextIdx]
	s.nextIdx++
	return item, nil
}

// HasPrev implements BiIterator.
func (s *sliceBiIterator[T]) HasPrev() bool {
	return s.prevIdx > 0
}

// Prev implements BiIterator.
func (s *sliceBiIterator[T]) Prev() (T, error) {
	if !s.HasPrev() {
		var zero T
		return zero, EOI
	}
	s.prevIdx--
	return s.items[s.prevIdx], nil
}
