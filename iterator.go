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
