//go:build integration

package rindb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestFlushPersistsDataToSSTables(t *testing.T) {
	dir := t.TempDir()
	db, cleanup := initTestDB(t, rindb.WithDatabaseDir(dir), rindb.WithMaxMemtableSize(2))
	ctx := context.Background()

	kvs := []struct{ key, val string }{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
	}
	for _, kv := range kvs {
		require.NoError(t, db.Put(ctx, rindb.Bytes(kv.key), rindb.Bytes(kv.val)))
	}

	cleanup()

	reopened, err := rindb.InitRinDB(ctx, rindb.WithDatabaseDir(dir), rindb.WithMaxMemtableSize(2))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	for _, kv := range kvs {
		got, err := reopened.Get(ctx, rindb.Bytes(kv.key))
		require.NoError(t, err)
		require.Equal(t, rindb.Bytes(kv.val), got)
	}
}
