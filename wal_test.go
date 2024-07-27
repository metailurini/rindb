package main

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_wal(t *testing.T) {
	t.Run("Clean WAL", func(t *testing.T) {
		// init file
		file, err := os.CreateTemp(os.TempDir(), "*")
		assert.NoError(t, err)

		// write content to file
		_, err = file.Write([]byte("hello"))
		assert.NoError(t, err)

		// commit content to disk file
		err = file.Sync()
		assert.NoError(t, err)

		// jump back to head of file
		_, err = file.Seek(0, io.SeekStart)
		assert.NoError(t, err)

		// make sure that content is already in file
		b1 := make([]byte, 5)
		_, err = file.Read(b1)
		assert.NoError(t, err)
		assert.Equal(t, "hello", string(b1))

		// init wal
		w := newWAL(file)

		// clean wal
		err = w.clean()
		assert.NoError(t, err)

		// make sure that the old wal is already close
		b2 := make([]byte, 5)
		_, err = file.Read(b2)
		assert.NotNil(t, err)

		// make sure that wal is empty
		b3 := make([]byte, 5)
		_, err = w.file.Read(b3)
		assert.ErrorIs(t, io.EOF, err)

		// close wal
		err = w.close()
		assert.NoError(t, err)
	})
}
