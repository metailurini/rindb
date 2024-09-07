package rindb

import (
	"cmp"
	"errors"
)

const UnsupportedTypeCode = -2

var ErrUnsupportedType = errors.New("unsupported type: type does not implement CmpType interface")

type CmpType interface {
	Compare(other any) int
}

type Comparable interface {
	cmp.Ordered | *CmpType | any
}

func Compare[T Comparable](a, b T) int {
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

func ValidateCmpType[T Comparable](a T) error {
	if Compare(a, a) == UnsupportedTypeCode {
		return ErrUnsupportedType
	}
	return nil
}
