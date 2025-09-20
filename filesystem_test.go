package rindb

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
