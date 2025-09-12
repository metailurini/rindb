package rindb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeInternalKey(t *testing.T) {
	t.Run("RegularKey", func(t *testing.T) {
		userKey := []byte("mykey")
		seq := uint64(123)
		typ := TypeValue

		internalKey := EncodeInternalKey(userKey, seq, typ)
		decodedUserKey, decodedSeq, decodedTyp, err := DecodeInternalKey(internalKey)

		require.NoError(t, err)
		require.True(t, bytes.Equal(userKey, decodedUserKey))
		require.Equal(t, seq, decodedSeq)
		require.Equal(t, typ, decodedTyp)
	})

	t.Run("NilUserKey", func(t *testing.T) {
		userKey := Bytes(nil)
		seq := uint64(456)
		typ := TypeDeletion

		internalKey := EncodeInternalKey(userKey, seq, typ)
		decodedUserKey, decodedSeq, decodedTyp, err := DecodeInternalKey(internalKey)

		require.NoError(t, err)
		require.Nil(t, decodedUserKey)
		require.Equal(t, seq, decodedSeq)
		require.Equal(t, typ, decodedTyp)
	})
}

func BenchmarkDecodeInternalKey(b *testing.B) {
	key := []byte("mykey")
	internalKey := EncodeInternalKey(key, 123, TypeValue)

	for b.Loop() {
		_, _, _, _ = DecodeInternalKey(internalKey)
	}
}
