package rindb

import (
	"cmp"
	"errors"
)

// CompareResult represents the outcome of a comparison between two values.
// It follows the semantics of cmp.Compare.
type CompareResult = int

const (
	UnsupportedTypeCode CompareResult = -2

	// CmpLess if x is less than y,
	CmpLess CompareResult = -1
	//	CmpEqual if x equals y,
	CmpEqual CompareResult = 0
	// CmpGreater if x is greater than y.
	CmpGreater CompareResult = 1
)

// ErrUnsupportedType is returned when a value does not implement the CmpType
// interface and therefore cannot be compared with Compare.
var ErrUnsupportedType = errors.New("unsupported type: type does not implement CmpType interface")

// CmpType must be implemented by types that provide their own comparison logic
// through the Compare function.
type CmpType interface {
	Compare(other any) int
}

// Comparable is a union constraint that lists all types which can be compared
// using the generic Compare function. It is exported so applications can define
// their own comparable types.
type Comparable interface {
	cmp.Ordered | *CmpType | any
}

// Compare returns the ordering between a and b. It supports builtin ordered
// types and any custom type that implements CmpType. Unsupported types return
// UnsupportedTypeCode.
func Compare[T Comparable](a, b T) CompareResult {
	switch a := any(a).(type) {
	case int:
		return cmp.Compare(a, any(b).(int))
	case int8:
		return cmp.Compare(a, any(b).(int8))
	case int16:
		return cmp.Compare(a, any(b).(int16))
	case int32:
		return cmp.Compare(a, any(b).(int32))
	case int64:
		return cmp.Compare(a, any(b).(int64))
	case uint:
		return cmp.Compare(a, any(b).(uint))
	case uint8:
		return cmp.Compare(a, any(b).(uint8))
	case uint16:
		return cmp.Compare(a, any(b).(uint16))
	case uint32:
		return cmp.Compare(a, any(b).(uint32))
	case uint64:
		return cmp.Compare(a, any(b).(uint64))
	case uintptr:
		return cmp.Compare(a, any(b).(uintptr))
	case float32:
		return cmp.Compare(a, any(b).(float32))
	case float64:
		return cmp.Compare(a, any(b).(float64))
	case string:
		return cmp.Compare(a, any(b).(string))
	case CmpType:
		return a.Compare(b)
	default:
		return UnsupportedTypeCode
	}
}

// ValidateCmpType verifies that the provided value is a supported Comparable
// type. It returns ErrUnsupportedType if the value cannot be compared.
func ValidateCmpType[T Comparable](a T) error {
	if Compare(a, a) == UnsupportedTypeCode {
		return ErrUnsupportedType
	}
	return nil
}
