package rindb

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func validateWALFormat(t *testing.T, file io.ReadSeeker) {
	_, err := file.Seek(0, io.SeekStart)
	assert.NoError(t, err)

	for {
		keyLenBytes := [mdByteSize]byte{}
		_, err := file.Read(keyLenBytes[:])
		if errors.Is(err, io.EOF) {
			break
		}
		assert.NoError(t, err)

		valueLenBytes := [mdByteSize]byte{}
		_, err = file.Read(valueLenBytes[:])
		assert.NoError(t, err)

		keyLen := byteOrder.Uint64(keyLenBytes[:])
		valueLen := byteOrder.Uint64(valueLenBytes[:])

		_, err = file.Seek(int64(keyLen+valueLen), io.SeekCurrent)
		assert.NoError(t, err)
	}
}

//nolint:funlen
func Test_wal(t *testing.T) {
	t.Run("Clean WAL", func(t *testing.T) {
		// init file
		fss, closer := initTempFileSystems(t, 1)
		defer closer()

		// init wal
		fs := fss[0]
		w := NewWAL(fs)

		// add some data

		err := w.Append(RecordImpl{Key: Bytes("key"), Value: Bytes("value")})
		assert.NoError(t, err)

		// clean wal
		err = w.Clean()
		assert.NoError(t, err)

		// make sure that the old wal is already close
		b2 := make(Bytes, 5)
		_, err = fs.Read(b2)
		assert.NotNil(t, err)

		// make sure that wal is empty
		mem, err := w.Load()
		assert.NoError(t, err)
		assert.Empty(t, mem.data.Len())

		// close wal
		err = w.Close()
		assert.NoError(t, err)
	})

	t.Run("Write and load WAL", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1)
		defer closer()

		fs := fss[0]
		w := NewWAL(fs)

		err := w.Append(RecordImpl{Bytes("single_key"), Bytes("single_value")})
		assert.NoError(t, err)

		recordsSize := 1_000
		records := make([]Record, 0, recordsSize)
		for i := 0; i < recordsSize; i++ {
			key := Bytes(fmt.Sprintf("key.%d", i))
			value := Bytes(fmt.Sprintf("value.%d", i))
			records = append(records, RecordImpl{key, value})
		}

		err = w.AppendMany(records)
		assert.NoError(t, err)

		validateWALFormat(t, w.file)

		mem, err := w.Load()
		assert.NoError(t, err)

		for i := 0; i < recordsSize; i++ {
			key := Bytes(fmt.Sprintf("key.%d", i))
			expectedValue := Bytes(fmt.Sprintf("value.%d", i))

			got, err := mem.Get(key)
			assert.NoError(t, err)
			assert.Equal(t, expectedValue, got)
		}

		got, err := mem.Get(Bytes("single_key"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("single_value"), got)
	})
}
