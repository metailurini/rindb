package rindb

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOffsetReader_Read(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		bufSize   int
		wantN     int
		wantOff   int64
		wantErr   error
		setupFunc func(fs *FileSystem)
	}{
		{
			name:    "advances offset on full read",
			bufSize: 5,
			wantN:   5,
			wantOff: 5,
		},
		{
			name:    "partial read advances offset",
			bufSize: 10,
			wantN:   5,
			wantOff: 5,
			wantErr: io.EOF,
		},
		{
			name:    "read error leaves offset unchanged",
			bufSize: 5,
			wantN:   0,
			wantOff: 0,
			wantErr: ErrFileNotOpened,
			setupFunc: func(fs *FileSystem) {
				require.NoError(t, fs.Close())
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			fss, closer := initTempFileSystems(t, 1, [][]byte{[]byte("hello")})
			defer closer()

			fs := fss[0]
			r := newOffsetReader(fs, 0)
			if tt.setupFunc != nil {
				tt.setupFunc(fs)
			}

			buf := make([]byte, tt.bufSize)
			n, err := r.Read(buf)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantN, n)
			assert.Equal(t, tt.wantOff, r.Offset())
		})
	}
}

func TestOffsetReader_PrevOffset(t *testing.T) {
	t.Parallel()
	t.Run("rewinds using trailer", func(t *testing.T) {
		data := make([]byte, 24)
		byteOrder.PutUint64(data[len(data)-mdByteSize:], uint64(len(data)))

		fss, closer := initTempFileSystems(t, 1, [][]byte{data})
		defer closer()

		fs := fss[0]
		r := newOffsetReader(fs, int64(len(data)))

		start, err := r.PrevOffset()
		require.NoError(t, err)
		assert.Equal(t, int64(0), start)
	})

	t.Run("offset zero returns eof", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1, [][]byte{nil})
		defer closer()

		fs := fss[0]
		r := newOffsetReader(fs, 0)

		start, err := r.PrevOffset()
		assert.ErrorIs(t, err, io.EOF)
		assert.Equal(t, int64(0), start)
	})

	t.Run("invalid trailer", func(t *testing.T) {
		data := make([]byte, 24)
		byteOrder.PutUint64(data[len(data)-mdByteSize:], uint64(len(data))*2)

		fss, closer := initTempFileSystems(t, 1, [][]byte{data})
		defer closer()

		fs := fss[0]
		r := newOffsetReader(fs, int64(len(data)))

		_, err := r.PrevOffset()
		require.Error(t, err)
	})
}
