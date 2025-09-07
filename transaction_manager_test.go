package rindb

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTempFS(t *testing.T) *FileSystem {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "wal.log")
	fs, err := OpenFS(context.Background(), p)
	require.NoError(t, err)
	return fs
}

func TestBeginCopiesExistingData(t *testing.T) {
	fs := newTempFS(t)
	_, err := fs.Write([]byte("old"))
	require.NoError(t, err)
	require.NoError(t, fs.Sync())

	tm := newTransactionManager()
	txn, err := tm.begin(fs)
	require.NoError(t, err)
	require.NotNil(t, txn)

	data, err := os.ReadFile(txn.log.Path())
	require.NoError(t, err)
	require.Equal(t, []byte("old"), data)
}

func TestWriteAndCommit(t *testing.T) {
	fs := newTempFS(t)
	tm := newTransactionManager()
	txn, err := tm.begin(fs)
	require.NoError(t, err)

	_, err = txn.write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, txn.commit(context.Background()))

	data, err := os.ReadFile(fs.Path())
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), data)
}

func TestRollback(t *testing.T) {
	fs := newTempFS(t)
	_, err := fs.Write([]byte("base"))
	require.NoError(t, err)
	require.NoError(t, fs.Sync())

	tm := newTransactionManager()
	txn, err := tm.begin(fs)
	require.NoError(t, err)

	_, err = txn.write([]byte("new"))
	require.NoError(t, err)
	require.NoError(t, txn.rollback(context.Background()))

	data, err := os.ReadFile(fs.Path())
	require.NoError(t, err)
	require.Equal(t, []byte("base"), data)
}

func TestWriteAfterCommit(t *testing.T) {
	fs := newTempFS(t)
	tm := newTransactionManager()
	txn, err := tm.begin(fs)
	require.NoError(t, err)

	_, err = txn.write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, txn.commit(context.Background()))

	_, err = txn.write([]byte("more"))
	require.Error(t, err)
}

func TestConcurrentBegin(t *testing.T) {
	tm := newTransactionManager()
	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	txns := make([]*transaction, n)

	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			fs := newTempFS(t)
			tx, err := tm.begin(fs)
			require.NoError(t, err)
			txns[i] = tx
		}(i)
	}

	wg.Wait()
	for _, tx := range txns {
		require.NotNil(t, tx)
		require.True(t, tx.isActive())
	}
}

func TestConcurrentWrite(t *testing.T) {
	tm := newTransactionManager()
	fs := newTempFS(t)
	tx, err := tm.begin(fs)
	require.NoError(t, err)

	const numWrites = 100
	var wg sync.WaitGroup
	wg.Add(numWrites)
	for i := 0; i < numWrites; i++ {
		go func() {
			defer wg.Done()
			_, err := tx.write([]byte{1})
			require.NoError(t, err)
		}()
	}
	wg.Wait()
	require.Equal(t, int64(numWrites), tx.size())
}

func TestTransactionCommitRollbackConcurrency(t *testing.T) {
	ctx := context.Background()
	cfg := NewConfig(WithDatabaseDir(t.TempDir()))
	w, err := DefaultNewWALFunc(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	require.NoError(t, w.Clean(ctx, math.MaxUint64))

	tm := newTransactionManager()

	txCommit, err := tm.begin(w.FileSystem)
	require.NoError(t, err)
	txRollback, err := tm.begin(w.FileSystem)
	require.NoError(t, err)

	seq := uint64(1)
	recCommit := RecordImpl{Key: Bytes("key"), Value: Bytes("commit"), SequenceNumber: seq}
	seq++
	recRollback := RecordImpl{Key: Bytes("key"), Value: Bytes("rollback"), SequenceNumber: seq}

	require.NoError(t, writeRecord(txCommit, recCommit))
	require.NoError(t, writeRecord(txRollback, recRollback))

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		require.NoError(t, txCommit.commit(ctx))
	}()

	go func() {
		defer wg.Done()
		require.NoError(t, txRollback.rollback(ctx))
	}()

	wg.Wait()
	require.NoError(t, w.Sync())

	mem, err := w.Load(ctx)
	require.NoError(t, err)
	val, err := mem.Get(Bytes("key"))
	require.NoError(t, err)
	require.Equal(t, Bytes("commit"), val)
}
