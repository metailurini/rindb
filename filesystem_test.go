package rindb

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

//nolint:funlen
func TestFileSystem(t *testing.T) {
	t.Run("Check file must be opened before doing other actions", func(t *testing.T) {
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
		assert.ErrorIs(t, emptyFs.Close(), ErrFileNotOpened)
	})
}

func TestFileSystem_Errors(t *testing.T) {
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
	t.Run("Get first position", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()

		fs := fss[0]

		position, err := fs.CursorPos()
		assert.NoError(t, err)
		assert.Zero(t, position)
	})

	t.Run("Get mid position", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()

		fs := fss[0]

		_, err := fs.Write([]byte("hello"))
		assert.NoError(t, err)

		err = fs.Sync()
		assert.NoError(t, err)

		_, err = fs.file.Seek(-3, io.SeekEnd)
		assert.NoError(t, err)

		position, err := fs.CursorPos()
		assert.NoError(t, err)
		assert.Equal(t, int64(2), position)
	})

	t.Run("Get end position", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()

		fs := fss[0]

		_, err := fs.Write([]byte("hello"))
		assert.NoError(t, err)

		err = fs.Sync()
		assert.NoError(t, err)

		_, err = io.ReadAll(fs)
		assert.NoError(t, err)

		position, err := fs.CursorPos()
		assert.NoError(t, err)
		assert.Equal(t, int64(5), position)
	})
}
