package rindb

import "errors"

// EOI is end of iteration
//
//lint:ignore ST1012 this is a sentinel error, not a typical error
var EOI = errors.New("EOI")

// Iterator defines the iteration contract. Implementations may support
// moving both forward and backward. Iterators start positioned before the
// first element. Next advances to the next element and returns it. Prev
// returns the current element and moves the iterator one step backward; as a
// consequence, a Prev call immediately after a successful Next returns the
// same element again. It is exported so callers can provide their own iterator
// implementations that work with Rindb primitives.
type Iterator[T any] interface {
	// HasNext reports whether calling Next will succeed.
	HasNext() bool
	// Next advances to the next element and returns it. It returns EOI
	// when the iteration is exhausted.
	Next() (T, error)
	// HasPrev reports whether calling Prev will succeed.
	HasPrev() bool
	// Prev returns the current element and moves the iterator one step
	// backward. It returns EOI when no more elements remain.
	Prev() (T, error)
}
