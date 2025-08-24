//go:build integration

package rindb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestRemovePersistsTombstone(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const maxMemtableSize = uint(64)

	db, err := rindb.InitRinDB(ctx,
		rindb.WithDatabaseDir(dir),
		rindb.WithMaxMemtableSize(maxMemtableSize),
	)
	require.NoError(t, err)

	key := rindb.Bytes("k1")
	val := rindb.Bytes("v1")
	require.NoError(t, db.Put(ctx, key, val))
	require.NoError(t, db.Remove(ctx, key))

	bigVal := make([]byte, maxMemtableSize)
	require.NoError(t, db.Put(ctx, rindb.Bytes("other"), bigVal))

	require.NoError(t, db.Close())

	db, err = rindb.InitRinDB(ctx,
		rindb.WithDatabaseDir(dir),
		rindb.WithMaxMemtableSize(maxMemtableSize),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	_, err = db.Get(ctx, key)
	require.ErrorIs(t, err, rindb.ErrKeyNotFound)
}
