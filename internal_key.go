package rindb

import (
	"bytes"
	"fmt"
)

// InternalKey represents a user key combined with sequence number and type.
type InternalKey struct {
	UserKey Bytes
	Seq     uint64
	Type    RecordType
}

const (
	seqNumBytes          = 8
	internalKeySuffixLen = seqNumBytes + 1 // sequence number + type
)

// Compare implements CmpType for InternalKey.
// Order is by user key ascending, sequence descending, type ascending.
func (k InternalKey) Compare(other any) int {
	o := other.(InternalKey)
	if cmp := bytes.Compare(k.UserKey, o.UserKey); cmp != 0 {
		return cmp
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

func EncodeInternalKey(key Bytes, seq uint64, typ RecordType) []byte {
	internalKey := make([]byte, len(key)+internalKeySuffixLen)
	copy(internalKey, key)
	var seqBuf [seqNumBytes]byte
	byteOrder.PutUint64(seqBuf[:], ^seq)
	copy(internalKey[len(key):], seqBuf[:])
	internalKey[len(key)+seqNumBytes] = byte(typ)
	return internalKey
}

func DecodeInternalKey(ikey Bytes) (Bytes, uint64, RecordType, error) {
	if len(ikey) < internalKeySuffixLen {
		return nil, 0, 0, fmt.Errorf("internal key too short: %d", len(ikey))
	}
	userKeyEnd := len(ikey) - internalKeySuffixLen
	seq := ^byteOrder.Uint64(ikey[userKeyEnd : userKeyEnd+seqNumBytes])
	typ := RecordType(ikey[len(ikey)-1])
	var userKey Bytes
	if userKeyEnd > 0 {
		userKey = append(Bytes(nil), ikey[:userKeyEnd]...)
	}
	return userKey, seq, typ, nil
}
