package rindb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoveFiles(t *testing.T) {
	dir := t.TempDir()
	paths := []string{
		filepath.Join(dir, sstPath(1)),
		filepath.Join(dir, sstPath(2)),
	}
	for _, p := range paths {
		f, err := os.Create(p)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
	files := []FileMeta{{Number: 1}, {Number: 2}, {Number: 3}}
	require.NoError(t, removeFiles(dir, files))
	for _, p := range paths {
		_, err := os.Stat(p)
		require.ErrorIs(t, err, os.ErrNotExist)
	}
}
