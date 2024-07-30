package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

//nolint:funlen
func Test_wal(t *testing.T) {
	t.Run("clean WAL", func(t *testing.T) {
		// init file
		file, err := os.CreateTemp(os.TempDir(), "*")
		assert.NoError(t, err)

		// init wal
		w := newWAL(file)

		// add some data
		err = w.write(Bytes("key"), Bytes("value"))
		assert.NoError(t, err)

		// clean wal
		err = w.clean()
		assert.NoError(t, err)

		// make sure that the old wal is already close
		b2 := make(Bytes, 5)
		_, err = file.Read(b2)
		assert.NotNil(t, err)

		// make sure that wal is empty
		mem, err := w.load()
		assert.NoError(t, err)
		assert.Empty(t, mem.data.len())

		// close wal
		err = w.close()
		assert.NoError(t, err)
	})

	t.Run("write and load WAL", func(t *testing.T) {
		file, err := os.CreateTemp(os.TempDir(), "*")
		assert.NoError(t, err)
		fmt.Printf("file.Name(): %v\n", file.Name())

		w := newWAL(file)
		defer func() {
			err := w.close()
			assert.NoError(t, err)
		}()

		for i := 0; i < 1_000; i++ {
			key := Bytes(fmt.Sprintf("key.%d", i))
			value := Bytes(fmt.Sprintf("value.%d", i))
			err = w.append(key, value)
			assert.NoError(t, err)
		}

		err = w.sync()
		assert.NoError(t, err)

		mem, err := w.load()
		assert.NoError(t, err)

		for i := 0; i < 1_000; i++ {
			key := Bytes(fmt.Sprintf("key.%d", i))
			expectedValue := Bytes(fmt.Sprintf("value.%d", i))

			got, err := mem.get(key)
			assert.NoError(t, err)
			assert.Equal(t, expectedValue, got)
		}

		err = mem.flush("/tmp/tmp")
		assert.NoError(t, err)
	})
}
