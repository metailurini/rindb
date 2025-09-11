package rindb

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOffsetReader_Read(t *testing.T) {
	t.Run("advances offset on full read", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1, [][]byte{[]byte("hello")})
		defer closer()

		r := newOffsetReader(fss[0], 0)
		buf := make([]byte, 5)
		n, err := r.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		assert.Equal(t, int64(5), r.Offset())
	})

	t.Run("partial read leaves offset unchanged", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1, [][]byte{[]byte("hello")})
		defer closer()

		r := newOffsetReader(fss[0], 0)
		buf := make([]byte, 10)
		n, err := r.Read(buf)
		assert.ErrorIs(t, err, io.EOF)
		assert.Equal(t, 5, n)
		assert.Equal(t, int64(0), r.Offset())
	})

	t.Run("read error leaves offset unchanged", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1, [][]byte{[]byte("hello")})
		defer closer()

		fs := fss[0]
		r := newOffsetReader(fs, 0)
		require.NoError(t, fs.Close())

		n, err := r.Read(make([]byte, 5))
		assert.ErrorIs(t, err, ErrFileNotOpened)
		assert.Zero(t, n)
		assert.Equal(t, int64(0), r.Offset())
	})
}
