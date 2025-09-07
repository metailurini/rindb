package rindb

import (
	"context"
	"os"
	"path/filepath"
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
