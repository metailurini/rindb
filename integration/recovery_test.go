//go:build integration

package rindb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestRecovery(t *testing.T) {
	dir := t.TempDir()

	db, cleanup := initTestDB(t, rindb.WithDatabaseDir(dir))
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

	db, cleanup = initTestDB(t, rindb.WithDatabaseDir(dir))
	defer cleanup()

	for _, kv := range kvs {
		got, err := db.Get(ctx, rindb.Bytes(kv.key))
		require.NoError(t, err)
		require.Equal(t, rindb.Bytes(kv.val), got)
	}
}
