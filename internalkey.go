package rindb

import "bytes"

type InternalKey struct {
	user Bytes
	seq  uint64
}

func NewInternalKey(user Bytes, seq uint64) InternalKey {
	return InternalKey{user: user, seq: seq}
}

func (k InternalKey) Compare(other any) int {
	o := other.(InternalKey)
	if c := bytes.Compare(k.user, o.user); c != 0 {
		if c < 0 {
			return CmpLess
		}
		return CmpGreater
	}
	// sequence descending
	if k.seq > o.seq {
		return CmpLess
	}
	if k.seq < o.seq {
		return CmpGreater
	}
	return CmpEqual
}

func (k InternalKey) Encode() Bytes {
	buf := make([]byte, len(k.user)+8)
	copy(buf, k.user)
	byteOrder.PutUint64(buf[len(k.user):], ^k.seq)
	return buf
}

func DecodeInternalKey(b Bytes) InternalKey {
	if len(b) < 8 {
		return InternalKey{user: b, seq: 0}
	}
	user := make(Bytes, len(b)-8)
	copy(user, b[:len(b)-8])
	seq := byteOrder.Uint64(b[len(b)-8:])
	return InternalKey{user: user, seq: ^seq}
}

func (k InternalKey) UserKey() Bytes { return k.user }
func (k InternalKey) Seq() uint64    { return k.seq }
