package rindb

import "errors"

// EOI is end of iteration
//
//lint:ignore ST1012 this is a sentinel error, not a typical error
var EOI = errors.New("EOI")

type Iterator[T any] interface {
	HasNext() bool
	Next() (T, error)
}
