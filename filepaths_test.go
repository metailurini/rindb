package rindb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilePathHelpers(t *testing.T) {
	t.Run("walPath", func(t *testing.T) {
		require.Equal(t, "000001"+walExt, walPath(1))
	})
	t.Run("sstPath", func(t *testing.T) {
		require.Equal(t, "000002"+sstExt, sstPath(2))
	})
	t.Run("manifestPath", func(t *testing.T) {
		require.Equal(t, "MANIFEST-000003", manifestPath(3))
	})
	t.Run("fileNum", func(t *testing.T) {
		n, err := fileNum("/path/000123" + sstExt)
		require.NoError(t, err)
		require.Equal(t, uint64(123), n)
	})
	t.Run("manifestNum", func(t *testing.T) {
		mn, err := manifestNum("MANIFEST-000007")
		require.NoError(t, err)
		require.Equal(t, 7, mn)
	})
}
