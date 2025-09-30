package rindb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logEntry struct {
	level   string
	message string
}

type capturingLogger struct {
	mu      sync.Mutex
	entries []logEntry
}

func (l *capturingLogger) record(level string, msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, logEntry{level: level, message: fmt.Sprintf(msg, args...)})
}

func (l *capturingLogger) Debug(_ context.Context, msg string, args ...any) {
	l.record("debug", msg, args...)
}

func (l *capturingLogger) Info(_ context.Context, msg string, args ...any) {
	l.record("info", msg, args...)
}

func (l *capturingLogger) Warn(_ context.Context, msg string, args ...any) {
	l.record("warn", msg, args...)
}

func (l *capturingLogger) Error(_ context.Context, msg string, args ...any) {
	l.record("error", msg, args...)
}

func (l *capturingLogger) contains(level, substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, entry := range l.entries {
		if entry.level == level && strings.Contains(entry.message, substr) {
			return true
		}
	}
	return false
}

//nolint:funlen
func TestFileSystem_BasicOperations(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Check file must be opened before doing other actions",
			test: func(t *testing.T) {
				fss, closer := initTempFileSystems(t, 1, nil)
				defer closer()

				fs := fss[0]
				emptyFs := FileSystem{filePath: ":path:"}

				assert.NoError(t, fs.Sync())
				assert.ErrorIs(t, emptyFs.Sync(), ErrFileNotOpened)

				assert.NoError(t, fs.Clean())
				assert.ErrorIs(t, emptyFs.Clean(), ErrFileNotOpened)

				_, err := fs.CursorPos()
				assert.NoError(t, err)
				_, err = emptyFs.CursorPos()
				assert.ErrorIs(t, err, ErrFileNotOpened)

				_, err = fs.Read(nil)
				assert.NoError(t, err)
				_, err = emptyFs.Read(nil)
				assert.ErrorIs(t, err, ErrFileNotOpened)

				_, err = fs.Write(nil)
				assert.NoError(t, err)
				_, err = emptyFs.Write(nil)
				assert.ErrorIs(t, err, ErrFileNotOpened)

				assert.NoError(t, fs.Close())
				assert.NoError(t, emptyFs.Close())
			},
		},
		{
			name: "Rename file",
			test: func(t *testing.T) {
				fss, closer := initTempFileSystems(t, 1, nil)
				defer closer()

				fs := fss[0]
				newPath := fs.Path() + "-renamed"

				assert.NoError(t, fs.Rename(newPath))
				assert.Equal(t, newPath, fs.Path())
			},
		},
		{
			name: "Rename and append content",
			test: func(t *testing.T) {
				fss, closer := initTempFileSystems(t, 1, nil)
				defer closer()
				fs := fss[0]
				ctx := context.Background()

				_, err := fs.Write([]byte("A"))
				require.NoError(t, err)
				newPath := filepath.Join(filepath.Dir(fs.Path()), "file")
				require.NoError(t, fs.Rename(newPath))
				require.NoError(t, fs.Open(ctx))
				_, err = fs.Seek(0, io.SeekEnd)
				require.NoError(t, err)
				_, err = fs.Write([]byte("B"))
				require.NoError(t, err)
				require.NoError(t, fs.Close())
				data, err := os.ReadFile(newPath)
				require.NoError(t, err)
				require.Equal(t, []byte("AB"), data)
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, tt.test)
	}
}

func TestFileSystem_Errors(t *testing.T) {
	t.Parallel()
	t.Run("OpenFS with invalid path (directory)", func(t *testing.T) {
		tempDir, err := os.MkdirTemp(os.TempDir(), "testdir-*")
		assert.NoError(t, err)
		defer os.RemoveAll(tempDir) // clean up

		fs, err := OpenFS(context.Background(), tempDir)
		assert.Error(t, err) // Expect an error because it's a directory
		assert.Nil(t, fs)
		assert.Contains(t, err.Error(), "failed to open file") // Check for specific error type if possible/needed
	})
}

func TestFileSystem_Clean_Errors(t *testing.T) {
	t.Parallel()
	t.Run("Clean fails due to permissions error during reopen/truncate", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1, [][]byte{[]byte("initial data")})
		defer closer()
		fs := fss[0]
		filePath := fs.Path()

		err := fs.Close()
		assert.NoError(t, err)

		err = fs.Open(context.Background())
		assert.NoError(t, err)

		// Make the file read-only after initial creation/opening
		err = os.Chmod(filePath, 0o444)
		assert.NoError(t, err)

		// Attempt to clean - this should fail when trying to reopen with O_RDWR | O_TRUNC
		err = fs.Clean()
		if os.Geteuid() == 0 {
			// Running as root may bypass permission errors
			t.Skip("skipping permission error check when running as root")
		}
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open/truncate file")
		assert.Contains(t, err.Error(), "permission denied") // Check for the underlying OS error

		// Clean up: Make writable again so defer closer() can remove it
		_ = os.Chmod(filePath, 0o600)
	})
}

func TestFileSystem_MmapFallback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()

	fs := fss[0]
	_, err := fs.Write([]byte("a"))
	require.NoError(t, err)
	_, err = fs.Seek(0, io.SeekStart)
	require.NoError(t, err)

	fs.configureMmap(ctx, false, newScopedLogger(nopLogger{}, LogLevelWarn))

	buf := make([]byte, 1)
	n, err := fs.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []byte("a"), buf)
	require.Nil(t, fs.mmapBytes(0, len(buf)))
}

func TestFileSystem_MmapEnabled(t *testing.T) {
	t.Parallel()

	switch runtime.GOOS {
	case "linux", "darwin", "windows":
	default:
		t.Skip("mmap not supported on this platform")
	}

	ctx := context.Background()
	payload := []byte("hello")
	fss, closer := initTempFileSystems(t, 1, [][]byte{payload})
	defer closer()

	fs := fss[0]
	cfg := testConfig(t)
	cfg.enableSSTableMmap = true
	fs.configureMmap(ctx, true, cfg.scopedLogger())

	view := fs.mmapBytes(0, len(payload))
	require.NotNil(t, view)
	require.Equal(t, payload, view)
	require.NotNil(t, fs.mmap)

	buf := make([]byte, len(payload))
	n, err := fs.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.Equal(t, payload, buf)
}

//nolint:funlen
func TestFileSystem_CursorPos(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		setup func(t *testing.T, fs *FileSystem)
		want  int64
	}{
		{
			name:  "beginning",
			setup: func(t *testing.T, fs *FileSystem) {},
			want:  0,
		},
		{
			name: "middle",
			setup: func(t *testing.T, fs *FileSystem) {
				_, err := fs.Write([]byte("hello"))
				require.NoError(t, err)
				require.NoError(t, fs.Sync())
				_, err = fs.Seek(-3, io.SeekEnd)
				require.NoError(t, err)
			},
			want: 2,
		},
		{
			name: "end",
			setup: func(t *testing.T, fs *FileSystem) {
				_, err := fs.Write([]byte("hello"))
				require.NoError(t, err)
				require.NoError(t, fs.Sync())
				_, err = io.ReadAll(fs)
				require.NoError(t, err)
			},
			want: 5,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			fss, closer := initTempFileSystems(t, 1, nil)
			defer closer()

			fs := fss[0]
			tt.setup(t, fs)

			pos, err := fs.CursorPos()
			assert.NoError(t, err)
			assert.Equal(t, tt.want, pos)
		})
	}
}

func TestOpenExistingFS_FilePresence(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		prepare     func(t *testing.T, path string)
		assertErr   require.ErrorAssertionFunc
		expectExist bool
	}{
		{
			name:        "missing file returns error and remains absent",
			prepare:     func(t *testing.T, path string) {},
			assertErr:   require.Error,
			expectExist: false,
		},
		{
			name: "opens existing file",
			prepare: func(t *testing.T, path string) {
				f, err := os.Create(path)
				require.NoError(t, err)
				require.NoError(t, f.Close())
			},
			assertErr:   require.NoError,
			expectExist: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			filePath := filepath.Join(dir, "test.sst")

			tt.prepare(t, filePath)

			fs, err := OpenExistingFS(ctx, filePath)
			tt.assertErr(t, err)
			if tt.expectExist {
				require.NotNil(t, fs)
				require.NoError(t, fs.Close())
			} else {
				require.Nil(t, fs)
			}

			if tt.expectExist {
				assert.FileExists(t, filePath)
			} else {
				assert.NoFileExists(t, filePath)
			}
		})
	}
}

func TestFileSystem_Open_ReusesExistingFileMmap(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	filePath := filepath.Join(dir, "existing.sst")
	file, err := os.Create(filePath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })

	fs := newFileSystem(filePath)
	fs.file = file
	fs.mmapEnabled = true

	var calls atomic.Int32
	restore := withMapFileStub(func(*os.File) (*mmapHandle, error) {
		calls.Add(1)
		return nil, nil
	})
	t.Cleanup(restore)

	require.NoError(t, fs.Open(ctx))
	require.Equal(t, int32(1), calls.Load())
	require.NoError(t, fs.Close())
}

func TestFileSystem_OpenExisting_ReusesExistingFileMmap(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	filePath := filepath.Join(dir, "existing.sst")
	file, err := os.Create(filePath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })

	fs := newFileSystem(filePath)
	fs.file = file
	fs.mmapEnabled = true

	var calls atomic.Int32
	restore := withMapFileStub(func(*os.File) (*mmapHandle, error) {
		calls.Add(1)
		return nil, nil
	})
	t.Cleanup(restore)

	require.NoError(t, fs.OpenExisting(ctx))
	require.Equal(t, int32(1), calls.Load())
	require.NoError(t, fs.Close())
}

func TestFileSystem_maybeMmapLocked_LogsErrors(t *testing.T) {
	t.Run("unsupported", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		filePath := filepath.Join(dir, "file.sst")
		file, err := os.Create(filePath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = file.Close() })

		fs := newFileSystem(filePath)
		fs.file = file
		fs.mmapEnabled = true
		logger := &capturingLogger{}
		fs.log = newScopedLogger(logger, LogLevelDebug)

		restore := withMapFileStub(func(*os.File) (*mmapHandle, error) {
			return nil, newMmapUnsupportedError(errors.New("unsupported mmap"))
		})
		t.Cleanup(restore)

		fs.maybeMmapLocked(ctx)
		require.True(t, logger.contains("debug", "unsupported mmap"))
		require.Nil(t, fs.mmap)
	})

	t.Run("generic error", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		filePath := filepath.Join(dir, "file.sst")
		file, err := os.Create(filePath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = file.Close() })

		fs := newFileSystem(filePath)
		fs.file = file
		fs.mmapEnabled = true
		logger := &capturingLogger{}
		fs.log = newScopedLogger(logger, LogLevelDebug)

		restore := withMapFileStub(func(*os.File) (*mmapHandle, error) {
			return nil, errors.New("boom")
		})
		t.Cleanup(restore)

		fs.maybeMmapLocked(ctx)
		require.True(t, logger.contains("warn", "boom"))
		require.Nil(t, fs.mmap)
	})
}

func TestFileSystem_mmapBytesZeroLength(t *testing.T) {
	payload := []byte("abcdef")
	fs := newFileSystem("/tmp/test")
	fs.mmap = &mmapHandle{data: payload}

	view := fs.mmapBytes(2, 0)
	require.NotNil(t, view)
	require.Len(t, view, 0)
}
