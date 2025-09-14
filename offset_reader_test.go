package rindb

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOffsetReader_Read(t *testing.T) {
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
			wantErr: ErrFileNotOpened,
			setupFunc: func(fs *FileSystem) {
				require.NoError(t, fs.Close())
			},
		},
	}

	for _, tt := range cases {
		tt := tt
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
