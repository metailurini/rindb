//go:build integration

package rindb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestTransactionManagerIntegration(t *testing.T) {
	ctx := context.Background()
	cfg := rindb.NewConfig(rindb.WithDatabaseDir(t.TempDir()))
	w, err := rindb.DefaultNewWALFunc(ctx, cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, w.Close()) }()

	tm := rindb.NewTransactionManager()

	t.Run("commit", func(t *testing.T) {
		require.NoError(t, w.Clean())
		txn := tm.Begin()
		rec := rindb.RecordImpl{Key: rindb.Bytes("key"), Value: rindb.Bytes("value"), SequenceNumber: 1}
		require.NoError(t, rindb.WriteRecord(txn, rec))
		require.NoError(t, txn.Commit(ctx, w))
		require.NoError(t, w.Sync())

		mem, err := w.Load(ctx)
		require.NoError(t, err)
		got, err := mem.Get(rindb.Bytes("key"))
		require.NoError(t, err)
		require.Equal(t, rindb.Bytes("value"), got)
	})

	t.Run("rollback", func(t *testing.T) {
		require.NoError(t, w.Clean())
		txn := tm.Begin()
		rec := rindb.RecordImpl{Key: rindb.Bytes("key2"), Value: rindb.Bytes("value2"), SequenceNumber: 1}
		require.NoError(t, rindb.WriteRecord(txn, rec))
		require.NoError(t, txn.Rollback(ctx))
		require.NoError(t, w.Sync())

		mem, err := w.Load(ctx)
		require.NoError(t, err)
		_, err = mem.Get(rindb.Bytes("key2"))
		require.Error(t, err)
	})
}
