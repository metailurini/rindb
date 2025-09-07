package rindb

import (
	"context"
	"errors"
	"io"
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
	txn, err := tm.begin(context.Background(), fs)
	require.NoError(t, err)
	require.NotNil(t, txn)

	data, err := os.ReadFile(txn.log.Path())
	require.NoError(t, err)
	require.Equal(t, []byte("old"), data)

	_, err = txn.write([]byte("new"))
	require.NoError(t, err)
	require.NoError(t, txn.commit(context.Background()))

	final, err := os.ReadFile(fs.Path())
	require.NoError(t, err)
	require.Equal(t, []byte("oldnew"), final)
}

func TestBeginCopyOpenError(t *testing.T) {
	fs := newTempFS(t)
	require.NoError(t, fs.Close())
	tm := newTransactionManager()

	wantErr := errors.New("open fail")
	origOpen := osOpen
	osOpen = func(string) (*os.File, error) { return nil, wantErr }
	defer func() { osOpen = origOpen }()

	txn, err := tm.begin(context.Background(), fs)
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, txn)

	entries, err := os.ReadDir(filepath.Dir(fs.Path()))
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestBeginCopyCopyError(t *testing.T) {
	fs := newTempFS(t)
	tm := newTransactionManager()

	wantErr := errors.New("copy fail")
	origCopy := ioCopy
	ioCopy = func(io.Writer, io.Reader) (int64, error) { return 0, wantErr }
	defer func() { ioCopy = origCopy }()

	txn, err := tm.begin(context.Background(), fs)
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, txn)

	entries, err := os.ReadDir(filepath.Dir(fs.Path()))
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestBeginCloseError(t *testing.T) {
	fs := newTempFS(t)
	// Close underlying file without updating fs to simulate close error
	require.NoError(t, fs.file.Close())

	tm := newTransactionManager()
	txn, err := tm.begin(context.Background(), fs)
	require.Error(t, err)
	require.Nil(t, txn)
}

func TestBeginShadowFSOpenError(t *testing.T) {
	fs := newTempFS(t)
	dir := filepath.Dir(fs.Path())
	require.NoError(t, fs.Close())
	require.NoError(t, os.RemoveAll(dir))

	tm := newTransactionManager()
	txn, err := tm.begin(context.Background(), fs)
	require.Error(t, err)
	require.Nil(t, txn)
}

func TestBeginSrcCloseError(t *testing.T) {
	fs := newTempFS(t)
	tm := newTransactionManager()

	tmp, err := os.CreateTemp("", "src")
	require.NoError(t, err)
	require.NoError(t, tmp.Close())
	defer os.Remove(tmp.Name())

	origOpen := osOpen
	origCopy := ioCopy
	osOpen = func(string) (*os.File, error) { return tmp, nil }
	ioCopy = func(io.Writer, io.Reader) (int64, error) { return 0, nil }
	defer func() { osOpen = origOpen; ioCopy = origCopy }()

	txn, err := tm.begin(context.Background(), fs)
	require.Error(t, err)
	require.Nil(t, txn)
}

func TestWriteAndCommit(t *testing.T) {
	fs := newTempFS(t)
	tm := newTransactionManager()
	txn, err := tm.begin(context.Background(), fs)
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
	txn, err := tm.begin(context.Background(), fs)
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
	txn, err := tm.begin(context.Background(), fs)
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
			tx, err := tm.begin(context.Background(), fs)
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
	tx, err := tm.begin(context.Background(), fs)
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

	txCommit, err := tm.begin(ctx, w.FileSystem)
	require.NoError(t, err)
	txRollback, err := tm.begin(ctx, w.FileSystem)
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

func TestCommitSyncError(t *testing.T) {
	fs := newTempFS(t)
	tm := newTransactionManager()
	txn, err := tm.begin(context.Background(), fs)
	require.NoError(t, err)

	wantErr := errors.New("sync fail")
	origSync := fsSync
	fsSync = func(*FileSystem) error { return wantErr }
	defer func() { fsSync = origSync }()

	err = txn.commit(context.Background())
	require.ErrorIs(t, err, wantErr)
}

func TestCommitLogCloseError(t *testing.T) {
	fs := newTempFS(t)
	tm := newTransactionManager()
	txn, err := tm.begin(context.Background(), fs)
	require.NoError(t, err)

	wantErr := errors.New("log close fail")
	origSync := fsSync
	origClose := fsClose
	fsSync = func(*FileSystem) error { return nil }
	fsClose = func(f *FileSystem) error {
		if f == txn.log {
			return wantErr
		}
		return origClose(f)
	}
	defer func() { fsSync = origSync; fsClose = origClose }()

	err = txn.commit(context.Background())
	require.ErrorIs(t, err, wantErr)
}

func TestCommitRenameError(t *testing.T) {
	fs := newTempFS(t)
	tm := newTransactionManager()
	txn, err := tm.begin(context.Background(), fs)
	require.NoError(t, err)

	wantErr := errors.New("rename fail")
	origRename := osRename
	osRename = func(_, _ string) error { return wantErr }
	defer func() { osRename = origRename }()

	err = txn.commit(context.Background())
	require.ErrorIs(t, err, wantErr)
}

func TestCommitTargetCloseError(t *testing.T) {
	fs := newTempFS(t)
	tm := newTransactionManager()
	txn, err := tm.begin(context.Background(), fs)
	require.NoError(t, err)

	wantErr := errors.New("target close fail")
	origClose := fsClose
	fsClose = func(f *FileSystem) error {
		if f == txn.target {
			return wantErr
		}
		return origClose(f)
	}
	defer func() { fsClose = origClose }()

	err = txn.commit(context.Background())
	require.ErrorIs(t, err, wantErr)
}

func TestCommitOpenExistingError(t *testing.T) {
	fs := newTempFS(t)
	tm := newTransactionManager()
	txn, err := tm.begin(context.Background(), fs)
	require.NoError(t, err)

	wantErr := errors.New("open existing fail")
	origOpenExisting := fsOpenExisting
	fsOpenExisting = func(*FileSystem, context.Context) error { return wantErr }
	defer func() { fsOpenExisting = origOpenExisting }()

	err = txn.commit(context.Background())
	require.ErrorIs(t, err, wantErr)
}

func TestRollbackCloseError(t *testing.T) {
	fs := newTempFS(t)
	tm := newTransactionManager()
	txn, err := tm.begin(context.Background(), fs)
	require.NoError(t, err)

	wantErr := errors.New("log close fail")
	origClose := fsClose
	fsClose = func(f *FileSystem) error {
		if f == txn.log {
			return wantErr
		}
		return origClose(f)
	}
	defer func() { fsClose = origClose }()

	err = txn.rollback(context.Background())
	require.ErrorIs(t, err, wantErr)
}

func TestRollbackRemoveError(t *testing.T) {
	fs := newTempFS(t)
	tm := newTransactionManager()
	txn, err := tm.begin(context.Background(), fs)
	require.NoError(t, err)

	wantErr := errors.New("remove fail")
	origRemove := osRemove
	osRemove = func(string) error { return wantErr }
	defer func() { osRemove = origRemove }()

	err = txn.rollback(context.Background())
	require.ErrorIs(t, err, wantErr)
}
