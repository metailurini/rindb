//go:build integration && smoke

package rindb_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestManifestRotationRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	db, err := rindb.InitRinDB(ctx,
		rindb.WithDatabaseDir(dir),
		rindb.WithMaxMemtableSize(1),
		rindb.WithLevel0CompactionThreshold(1000),
		rindb.WithManifestSizeThreshold(1024),
	)
	require.NoError(t, err)

	const numPuts = 11
	for i := 0; i < numPuts; i++ {
		key := rindb.Bytes(fmt.Sprintf("k%02d", i))
		require.NoError(t, db.Put(ctx, key, rindb.Bytes("v")))
	}
	require.NoError(t, db.Close())

	data, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	require.NoError(t, err)
	mf := strings.TrimSpace(string(data))
	require.NotEqual(t, "MANIFEST-000001", mf)

	alloc := rindb.NewFileNumberAllocator(1)
	vs, manifestPath, err := rindb.RecoverVersionSet(ctx, dir, alloc)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, mf), manifestPath)
	require.Len(t, vs.Levels[0], numPuts)
}
