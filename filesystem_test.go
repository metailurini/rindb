package rindb

import (
	"io"
	"os"
	"strings"
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
