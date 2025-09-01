package rindb

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManifestRotation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rin, cleanup := initRinDBWithCleanup(t,
		WithDatabaseDir(dir),
		WithMaxMemtableSize(1),
		WithLevel0CompactionThreshold(1000),
		WithManifestSizeThreshold(1024),
	)
	defer cleanup()

	puts := 11
	for i := 0; i < puts; i++ {
		key := Bytes(fmt.Sprintf("k%02d", i))
		require.NoError(t, rin.Put(ctx, key, Bytes("v")))
	}
	rin.wg.Wait()

	data, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	require.NoError(t, err)
	mf := strings.TrimSpace(string(data))
	require.NotEqual(t, "MANIFEST-000001", mf)

	r, err := NewManifestReader(ctx, filepath.Join(dir, mf))
	require.NoError(t, err)
	edit, err := r.Next()
	require.NoError(t, err)
	require.Len(t, edit.AddFiles, puts)
	_, err = r.Next()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, r.Close())
}
