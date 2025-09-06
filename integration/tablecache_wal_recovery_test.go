//go:build integration

package rindb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestTableCacheAfterWALRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	opts := []rindb.Option{
		rindb.WithDatabaseDir(dir),
		rindb.WithMaxMemtableSize(175),
	}

	db, err := rindb.InitRinDB(ctx, opts...)
	require.NoError(t, err)

	kvs := []struct{ key, val string }{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"}, // triggers flush
	}
	for _, kv := range kvs {
		require.NoError(t, db.Put(ctx, rindb.Bytes(kv.key), rindb.Bytes(kv.val)))
	}
	require.NoError(t, db.Put(ctx, rindb.Bytes("k4"), rindb.Bytes("v4")))
	require.NoError(t, db.Close())

	db, err = rindb.InitRinDB(ctx, opts...)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	before := db.TableCacheStats()

	v, err := db.Get(ctx, rindb.Bytes("k1"))
	require.NoError(t, err)
	require.Equal(t, rindb.Bytes("v1"), v)
	mid := db.TableCacheStats()
	require.Equal(t, before.Misses+1, mid.Misses)

	v, err = db.Get(ctx, rindb.Bytes("k1"))
	require.NoError(t, err)
	require.Equal(t, rindb.Bytes("v1"), v)
	after := db.TableCacheStats()

require.Equal(t, mid.Hits+1, after.Hits)
}
