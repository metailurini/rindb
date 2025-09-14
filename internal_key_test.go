package rindb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInternalKey_Decode(t *testing.T) {
	cases := []struct {
		name    string
		userKey Bytes
		seq     uint64
		typ     RecordType
		wantKey Bytes
	}{
		{
			name:    "regular key",
			userKey: Bytes("mykey"),
			seq:     123,
			typ:     TypeValue,
			wantKey: Bytes("mykey"),
		},
		{
			name:    "nil user key",
			userKey: nil,
			seq:     456,
			typ:     TypeDeletion,
			wantKey: nil,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			internalKey := EncodeInternalKey(tt.userKey, tt.seq, tt.typ)
			decodedUserKey, decodedSeq, decodedTyp, err := DecodeInternalKey(internalKey)

			require.NoError(t, err)
			if tt.wantKey == nil {
				require.Nil(t, decodedUserKey)
			} else {
				require.True(t, bytes.Equal(tt.wantKey, decodedUserKey))
			}
			require.Equal(t, tt.seq, decodedSeq)
			require.Equal(t, tt.typ, decodedTyp)
		})
	}
}

func BenchmarkDecodeInternalKey(b *testing.B) {
	key := []byte("mykey")
	internalKey := EncodeInternalKey(key, 123, TypeValue)

	for b.Loop() {
		_, _, _, _ = DecodeInternalKey(internalKey)
	}
}
