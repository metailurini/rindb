//go:build integration

package rindb_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestTransactionCommitRollbackConcurrency(t *testing.T) {
	ctx := context.Background()
	cfg := rindb.NewConfig(rindb.WithDatabaseDir(t.TempDir()))
	w, err := rindb.DefaultNewWALFunc(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })

	tm := rindb.NewTransactionManager()

	txnCommit := tm.Begin()
	txnRollback := tm.Begin()

	seq := uint64(1)
	recCommit := rindb.RecordImpl{Key: rindb.Bytes("key"), Value: rindb.Bytes("commit"), SequenceNumber: seq}
	seq++
	recRollback := rindb.RecordImpl{Key: rindb.Bytes("key"), Value: rindb.Bytes("rollback"), SequenceNumber: seq}

	require.NoError(t, rindb.WriteRecord(txnCommit, recCommit))
	require.NoError(t, rindb.WriteRecord(txnRollback, recRollback))

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		require.NoError(t, txnCommit.Commit(ctx, w))
	}()

	go func() {
		defer wg.Done()
		require.NoError(t, txnRollback.Rollback(ctx))
	}()

	wg.Wait()

	require.NoError(t, w.Sync())

	mem, err := w.Load(ctx)
	require.NoError(t, err)

	val, err := mem.Get(rindb.Bytes("key"))
	require.NoError(t, err)
	require.Equal(t, rindb.Bytes("commit"), val)
}
