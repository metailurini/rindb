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

func TestTransactionManager_BeginCopiesExistingData(t *testing.T) {
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

func TestTransactionManager_BeginErrors(t *testing.T) {
	ctx := context.Background()
	wantOpenFail := errors.New("open fail")
	wantCopyFail := errors.New("copy fail")
	tests := []struct {
		name    string
		setup   func(t *testing.T) (*FileSystem, func())
		wantErr error
		post    func(t *testing.T, fs *FileSystem)
	}{
		{
			name: "copy open error",
			setup: func(t *testing.T) (*FileSystem, func()) {
				t.Helper()
				fs := newTempFS(t)
				require.NoError(t, fs.Close())
				origOpen := osOpen
				osOpen = func(string) (*os.File, error) { return nil, wantOpenFail }
				cleanup := func() { osOpen = origOpen }
				return fs, cleanup
			},
			wantErr: wantOpenFail,
			post: func(t *testing.T, fs *FileSystem) {
				t.Helper()
				entries, err := os.ReadDir(filepath.Dir(fs.Path()))
				require.NoError(t, err)
				require.Len(t, entries, 1)
			},
		},
		{
			name: "copy error",
			setup: func(t *testing.T) (*FileSystem, func()) {
				t.Helper()
				fs := newTempFS(t)
				origCopy := ioCopy
				ioCopy = func(io.Writer, io.Reader) (int64, error) { return 0, wantCopyFail }
				cleanup := func() { ioCopy = origCopy }
				return fs, cleanup
			},
			wantErr: wantCopyFail,
			post: func(t *testing.T, fs *FileSystem) {
				t.Helper()
				entries, err := os.ReadDir(filepath.Dir(fs.Path()))
				require.NoError(t, err)
				require.Len(t, entries, 1)
			},
		},
		{
			name: "close error",
			setup: func(t *testing.T) (*FileSystem, func()) {
				t.Helper()
				fs := newTempFS(t)
				require.NoError(t, fs.file.Close())
				return fs, nil
			},
		},
		{
			name: "shadow fs open error",
			setup: func(t *testing.T) (*FileSystem, func()) {
				t.Helper()
				fs := newTempFS(t)
				dir := filepath.Dir(fs.Path())
				require.NoError(t, fs.Close())
				require.NoError(t, os.RemoveAll(dir))
				return fs, nil
			},
		},
		{
			name: "src close error",
			setup: func(t *testing.T) (*FileSystem, func()) {
				t.Helper()
				fs := newTempFS(t)
				tmp, err := os.CreateTemp("", "src")
				require.NoError(t, err)
				require.NoError(t, tmp.Close())
				origOpen := osOpen
				origCopy := ioCopy
				osOpen = func(string) (*os.File, error) { return tmp, nil }
				ioCopy = func(io.Writer, io.Reader) (int64, error) { return 0, nil }
				cleanup := func() { osOpen = origOpen; ioCopy = origCopy; os.Remove(tmp.Name()) }
				return fs, cleanup
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			fs, cleanup := tt.setup(t)
			if cleanup != nil {
				defer cleanup()
			}
			tm := newTransactionManager()
			txn, err := tm.begin(ctx, fs)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.Error(t, err)
			}
			require.Nil(t, txn)
			if tt.post != nil {
				tt.post(t, fs)
			}
		})
	}
}

func TestTransactionManager_WriteAndCommit(t *testing.T) {
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

func TestTransactionManager_WritePartial(t *testing.T) {
	fs := newTempFS(t)
	tm := newTransactionManager()
	txn, err := tm.begin(context.Background(), fs)
	require.NoError(t, err)

	origWrite := fsWrite
	defer func() { fsWrite = origWrite }()
	first := true
	fsWrite = func(fsys *FileSystem, p []byte) (int, error) {
		if first {
			first = false
			half := len(p) / 2
			return origWrite(fsys, p[:half])
		}
		return origWrite(fsys, p)
	}

	data := []byte("partial")
	n, err := txn.write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.NoError(t, txn.commit(context.Background()))

	read, err := os.ReadFile(fs.Path())
	require.NoError(t, err)
	require.Equal(t, data, read)
}

func TestTransactionManager_RollbackRestoresExistingData(t *testing.T) {
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

func TestTransactionManager_WriteAfterCommit(t *testing.T) {
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

func TestTransactionManager_ConcurrentBegin(t *testing.T) {
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

func TestTransactionManager_ConcurrentWrite(t *testing.T) {
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

func TestTransactionManager_CommitRollbackConcurrency(t *testing.T) {
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

func TestTransactionManager_CommitErrors(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		setup func(txn *transaction) (func(), error)
	}{
		{
			name: "sync error",
			setup: func(txn *transaction) (func(), error) {
				wantErr := errors.New("sync fail")
				origSync := fsSync
				fsSync = func(*FileSystem) error { return wantErr }
				return func() { fsSync = origSync }, wantErr
			},
		},
		{
			name: "log close error",
			setup: func(txn *transaction) (func(), error) {
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
				return func() { fsSync = origSync; fsClose = origClose }, wantErr
			},
		},
		{
			name: "rename error",
			setup: func(txn *transaction) (func(), error) {
				wantErr := errors.New("rename fail")
				origRename := osRename
				osRename = func(_, _ string) error { return wantErr }
				return func() { osRename = origRename }, wantErr
			},
		},
		{
			name: "target close error",
			setup: func(txn *transaction) (func(), error) {
				wantErr := errors.New("target close fail")
				origClose := fsClose
				fsClose = func(f *FileSystem) error {
					if f == txn.target {
						return wantErr
					}
					return origClose(f)
				}
				return func() { fsClose = origClose }, wantErr
			},
		},
		{
			name: "open existing error",
			setup: func(txn *transaction) (func(), error) {
				wantErr := errors.New("open existing fail")
				origOpenExisting := fsOpenExisting
				fsOpenExisting = func(*FileSystem, context.Context) error { return wantErr }
				return func() { fsOpenExisting = origOpenExisting }, wantErr
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			fs := newTempFS(t)
			tm := newTransactionManager()
			txn, err := tm.begin(ctx, fs)
			require.NoError(t, err)
			cleanup, wantErr := tt.setup(txn)
			if cleanup != nil {
				defer cleanup()
			}
			err = txn.commit(ctx)
			require.ErrorIs(t, err, wantErr)
		})
	}
}

func TestTransactionManager_RollbackErrors(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		setup func(txn *transaction) (func(), error)
	}{
		{
			name: "log close error",
			setup: func(txn *transaction) (func(), error) {
				wantErr := errors.New("log close fail")
				origClose := fsClose
				fsClose = func(f *FileSystem) error {
					if f == txn.log {
						return wantErr
					}
					return origClose(f)
				}
				return func() { fsClose = origClose }, wantErr
			},
		},
		{
			name: "remove error",
			setup: func(txn *transaction) (func(), error) {
				wantErr := errors.New("remove fail")
				origRemove := osRemove
				osRemove = func(string) error { return wantErr }
				return func() { osRemove = origRemove }, wantErr
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			fs := newTempFS(t)
			tm := newTransactionManager()
			txn, err := tm.begin(ctx, fs)
			require.NoError(t, err)
			cleanup, wantErr := tt.setup(txn)
			if cleanup != nil {
				defer cleanup()
			}
			err = txn.rollback(ctx)
			require.ErrorIs(t, err, wantErr)
		})
	}
}
