package rindb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilePathHelpers_GenerateAndParse(t *testing.T) {
	cases := []struct {
		name string
		fn   func(*testing.T)
	}{
		{
			name: "walPath",
			fn: func(t *testing.T) {
				require.Equal(t, "000001"+walExt, walPath(1))
			},
		},
		{
			name: "sstPath",
			fn: func(t *testing.T) {
				require.Equal(t, "000002"+sstExt, sstPath(2))
			},
		},
		{
			name: "manifestPath",
			fn: func(t *testing.T) {
				require.Equal(t, "MANIFEST-000003", manifestPath(3))
			},
		},
		{
			name: "fileNum",
			fn: func(t *testing.T) {
				n, err := fileNum("/path/000123" + sstExt)
				require.NoError(t, err)
				require.Equal(t, uint64(123), n)
			},
		},
		{
			name: "manifestNum",
			fn: func(t *testing.T) {
				mn, err := manifestNum("MANIFEST-000007")
				require.NoError(t, err)
				require.Equal(t, 7, mn)
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, tt.fn)
	}
}
