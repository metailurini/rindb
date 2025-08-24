//go:build integration

package rindb_test

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

var walRecoveryKVs = []struct{ key, val string }{
	{"k1", "v1"},
	{"k2", "v2"},
	{"k3", "v3"},
}

func TestWALRecovery(t *testing.T) {
	dir := os.Getenv("WAL_RECOVERY_TEST_DIR")
	if dir != "" {
		walRecoveryHelper(t, dir)
		return
	}

	dir = t.TempDir()

	cmd := exec.Command(os.Args[0], "-test.run", "TestWALRecovery", "-test.v")
	cmd.Env = append(os.Environ(), "WAL_RECOVERY_TEST_DIR="+dir)
	require.NoError(t, cmd.Run())

	ctx := context.Background()
	db, err := rindb.InitRinDB(ctx, rindb.WithDatabaseDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	for _, kv := range walRecoveryKVs {
		got, err := db.Get(ctx, rindb.Bytes(kv.key))
		require.NoError(t, err)
		require.Equal(t, rindb.Bytes(kv.val), got)
	}
}

func walRecoveryHelper(t *testing.T, dir string) {
	ctx := context.Background()
	db, err := rindb.InitRinDB(ctx, rindb.WithDatabaseDir(dir))
	require.NoError(t, err)

	for _, kv := range walRecoveryKVs {
		require.NoError(t, db.Put(ctx, rindb.Bytes(kv.key), rindb.Bytes(kv.val)))
	}

	os.Exit(0)
}
