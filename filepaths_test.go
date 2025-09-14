package rindb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilepaths_Generation(t *testing.T) {
	cases := []struct {
		name     string
		got      string
		expected string
	}{
		{"walPath", walPath(1), "000001.wal"},
		{"sstPath", sstPath(2), "000002.sst"},
		{"manifestPath", manifestPath(3), "MANIFEST-000003"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.got)
		})
	}
}

func TestFilepaths_NumParsing(t *testing.T) {
	cases := []struct {
		name      string
		path      string
		wantNum   uint64
		assertErr require.ErrorAssertionFunc
	}{
		{
			name:      "valid sst",
			path:      "/path/000123.sst",
			wantNum:   123,
			assertErr: require.NoError,
		},
		{
			name:      "valid wal",
			path:      "000001.wal",
			wantNum:   1,
			assertErr: require.NoError,
		},
		{
			name:      "no extension",
			path:      "000001",
			wantNum:   0,
			assertErr: require.Error,
		},
		{
			name:      "invalid extension",
			path:      "000001.log",
			wantNum:   0,
			assertErr: require.Error,
		},
		{
			name:      "not a number",
			path:      "abcdef.sst",
			wantNum:   0,
			assertErr: require.Error,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			num, err := fileNum(tc.path)
			tc.assertErr(t, err)
			require.Equal(t, tc.wantNum, num)
		})
	}
}

func TestFilepaths_ManifestNumParsing(t *testing.T) {
	cases := []struct {
		name      string
		path      string
		wantNum   int
		assertErr require.ErrorAssertionFunc
	}{
		{
			name:      "valid",
			path:      "MANIFEST-000007",
			wantNum:   7,
			assertErr: require.NoError,
		},
		{
			name:      "no prefix",
			path:      "000007",
			wantNum:   0,
			assertErr: require.Error,
		},
		{
			name:      "not a number",
			path:      "MANIFEST-abcdef",
			wantNum:   0,
			assertErr: require.Error,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			num, err := manifestNum(tc.path)
			tc.assertErr(t, err)
			require.Equal(t, tc.wantNum, num)
		})
	}
}
