package rindb

type Iterator[T any] interface {
	HasNext() bool
	Next() (T, error)
}
