//go:build integration && smoke

package rindb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestRemove_PersistsTombstone(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const maxMemtableSize = uint(64)
	key := rindb.Bytes("k1")

	func() {
		db, err := rindb.InitRinDB(ctx,
			rindb.WithDatabaseDir(dir),
			rindb.WithMaxMemtableSize(maxMemtableSize),
		)
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()

		val := rindb.Bytes("v1")
		require.NoError(t, db.Put(ctx, key, val))
		require.NoError(t, db.Remove(ctx, key))

		// This Put should trigger a flush including the tombstone for key.
		bigVal := make([]byte, maxMemtableSize)
		require.NoError(t, db.Put(ctx, rindb.Bytes("other"), bigVal))
	}()

	db, err := rindb.InitRinDB(ctx,
		rindb.WithDatabaseDir(dir),
		rindb.WithMaxMemtableSize(maxMemtableSize),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	_, err = db.Get(ctx, key)
	require.ErrorIs(t, err, rindb.ErrKeyNotFound)
}
