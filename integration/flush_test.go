//go:build integration && smoke

package rindb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestFlush_PersistsDataToSSTables(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	// An entry with a small key/value has an estimated size of ~87 bytes (key + value + node overhead).
	// To trigger a flush once the memtable is full with 2 entries, we set the max size
	// to a value that will be exceeded by the 3rd entry.
	const maxMemtableSize = 175
	opts := []rindb.Option{
		rindb.WithDatabaseDir(dir),
		rindb.WithMaxMemtableSize(maxMemtableSize),
	}

	// Phase 1: Create DB, write data to trigger a flush, then close.
	db, err := rindb.InitRinDB(ctx, opts...)
	require.NoError(t, err)

	kvs := []struct{ key, val string }{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"}, // This 3rd entry should trigger the flush.
	}
	for _, kv := range kvs {
		require.NoError(t, db.Put(ctx, rindb.Bytes(kv.key), rindb.Bytes(kv.val)))
	}
	require.NoError(t, db.Close())

	// Phase 2: Reopen DB and verify all data was persisted.
	reopened, err := rindb.InitRinDB(ctx, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	for _, kv := range kvs {
		got, err := reopened.Get(ctx, rindb.Bytes(kv.key))
		require.NoError(t, err)
		require.Equal(t, rindb.Bytes(kv.val), got)
	}
}
