package rindb

import "bytes"

// InternalKey represents a user key combined with sequence number and type.
type InternalKey struct {
	UserKey Bytes
	Seq     uint64
	Type    RecordType
}

// Compare implements CmpType for InternalKey.
// Order is by user key ascending, sequence descending, type ascending.
func (k InternalKey) Compare(other any) int {
	o := other.(InternalKey)
	if cmp := bytes.Compare(k.UserKey, o.UserKey); cmp != 0 {
		if cmp < 0 {
			return CmpLess
		}
		return CmpGreater
	}
	if k.Seq > o.Seq {
		return CmpLess
	}
	if k.Seq < o.Seq {
		return CmpGreater
	}
	if k.Type < o.Type {
		return CmpLess
	}
	if k.Type > o.Type {
		return CmpGreater
	}
	return CmpEqual
}
