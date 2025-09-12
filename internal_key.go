package rindb

import (
	"bytes"
	"cmp"
	"encoding/binary"
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
	return cmp.Or(bytes.Compare(k.UserKey, o.UserKey),
		cmp.Compare(o.Seq, k.Seq),
		cmp.Compare(k.Type, o.Type),
	)
}

func EncodeInternalKey(key Bytes, seq uint64, typ RecordType) []byte {
	internalKey := make([]byte, len(key)+internalKeySuffixLen)
	copy(internalKey, key)
	var seqBuf [seqNumBytes]byte
	binary.BigEndian.PutUint64(seqBuf[:], ^seq)
	copy(internalKey[len(key):], seqBuf[:])
	internalKey[len(key)+seqNumBytes] = byte(typ)
	return internalKey
}

func DecodeInternalKey(ikey Bytes) (Bytes, uint64, RecordType, error) {
	if len(ikey) < internalKeySuffixLen {
		return nil, 0, 0, fmt.Errorf("internal key too short: %d", len(ikey))
	}
	userKeyEnd := len(ikey) - internalKeySuffixLen
	// Cap to userKeyEnd so appends won’t run into the suffix by mistake.
	userKey := ikey[:userKeyEnd:userKeyEnd]
	if userKeyEnd == 0 {
		userKey = nil
	}

	seq := ^binary.BigEndian.Uint64(ikey[userKeyEnd : userKeyEnd+seqNumBytes])
	typ := RecordType(ikey[len(ikey)-1])
	return userKey, seq, typ, nil
}
