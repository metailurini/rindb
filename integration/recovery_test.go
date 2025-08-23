//go:build integration

package rindb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestDataPersistenceOnReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	dbOpt := rindb.WithDatabaseDir(dir)

	db, err := rindb.InitRinDB(ctx, dbOpt)
	require.NoError(t, err)

	kvs := []struct{ key, val string }{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
	}
	for _, kv := range kvs {
		require.NoError(t, db.Put(ctx, rindb.Bytes(kv.key), rindb.Bytes(kv.val)))
	}
	require.NoError(t, db.Close())

	db, err = rindb.InitRinDB(ctx, dbOpt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	for _, kv := range kvs {
		got, err := db.Get(ctx, rindb.Bytes(kv.key))
		require.NoError(t, err)
		require.Equal(t, rindb.Bytes(kv.val), got)
	}
}
