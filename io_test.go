package rindb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
)

// errorReader simulates an io.Reader that returns an error.
type errorReader struct {
	err error
}

func (er *errorReader) Read(p []byte) (n int, err error) {
	return 0, er.err
}

func Test_rw(t *testing.T) {
	t.Run("Write key with size = 0", func(t *testing.T) {
		tx := newTransactionManager().begin()

		testKey := Bytes(nil)
		testValue := Bytes("value")

		err := writeRecord(tx, newRecord(testKey, testValue, 0))
		assert.NoError(t, err)

		record, err := ReadRecord(tx.buffer)
		assert.NoError(t, err)
		assert.Equal(t, Bytes(nil), record.GetKey())
		assert.Equal(t, testValue, record.GetValue())
	})

	t.Run("Write key and value with size = 0", func(t *testing.T) {
		tx := newTransactionManager().begin()

		testKey := Bytes(nil)
		testValue := Bytes(nil)

		err := writeRecord(tx, newRecord(testKey, testValue, 0))
		assert.NoError(t, err)

		record, err := ReadRecord(tx.buffer)
		assert.NoError(t, err)
		assert.Equal(t, testKey, record.GetKey())
		assert.Equal(t, testValue, record.GetValue())
	})

	t.Run("Write key and value with size > 255", func(t *testing.T) {
		tx := newTransactionManager().begin()

		testKey := ""
		testValue := ""
		for i := 0; i < 500; i++ {
			testKey += fmt.Sprintf("test key %d ", i)
			testValue += fmt.Sprintf("test value %d ", i)
		}

		err := writeRecord(tx, newRecord(Bytes(testKey), Bytes(testValue), 0))
		assert.NoError(t, err)

		record, err := ReadRecord(tx.buffer)
		assert.NoError(t, err)
		assert.Equal(t, testKey, string(record.GetKey()))
		assert.Equal(t, testValue, string(record.GetValue()))
	})

	t.Run("Write key and value with size < 255", func(t *testing.T) {
		tx := newTransactionManager().begin()

		err := writeRecord(tx, newRecord(Bytes("key"), Bytes("value"), 0))
		assert.NoError(t, err)

		record, err := ReadRecord(tx.buffer)
		assert.NoError(t, err)
		assert.Equal(t, "key", string(record.GetKey()))
		assert.Equal(t, "value", string(record.GetValue()))
	})
}

func TestReadRecord_Errors(t *testing.T) {
	t.Run("Error reading internal key length", func(t *testing.T) {
		reader := &errorReader{err: errors.New("read internal key length failed")}
		_, err := ReadRecord(reader)
		assert.ErrorContains(t, err, "failed to read internal key length")
		assert.ErrorContains(t, err, "read internal key length failed")
	})

	t.Run("Error reading value length", func(t *testing.T) {
		var buf bytes.Buffer
		writeNumber(&transaction{buffer: &buf, state: "active"}, 5) // Internal key length
		reader := io.MultiReader(&buf, &errorReader{err: errors.New("read value length failed")})
		_, err := ReadRecord(reader)
		assert.ErrorContains(t, err, "failed to read value length")
		assert.ErrorContains(t, err, "read value length failed")
	})

	t.Run("Error reading internal key bytes", func(t *testing.T) {
		var buf bytes.Buffer
		writeNumber(&transaction{buffer: &buf, state: "active"}, 5) // Internal key length
		writeNumber(&transaction{buffer: &buf, state: "active"}, 5) // Value length
		buf.Write([]byte("key"))                                    // only 3 bytes of internal key
		_, err := ReadRecord(&buf)
		assert.ErrorContains(t, err, "failed to read internal key bytes")
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("Error reading value bytes (EOF)", func(t *testing.T) {
		var buf bytes.Buffer
		writeNumber(&transaction{buffer: &buf, state: "active"}, 3+internalKeySuffixLen) // Internal key length for "key"
		writeNumber(&transaction{buffer: &buf, state: "active"}, 5)                      // Value length
		buf.Write(EncodeInternalKey(Bytes("key"), 0, TypeValue))
		buf.Write([]byte("val")) // only 3 bytes of value
		_, err := ReadRecord(&buf)
		assert.ErrorContains(t, err, "failed to read value bytes")
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("Error reading value bytes (iotest)", func(t *testing.T) {
		var buf bytes.Buffer
		writeNumber(&transaction{buffer: &buf, state: "active"}, 3+internalKeySuffixLen) // Internal key length
		writeNumber(&transaction{buffer: &buf, state: "active"}, 5)                      // Value length
		buf.Write(EncodeInternalKey(Bytes("key"), 0, TypeValue))
		reader := io.MultiReader(&buf, iotest.ErrReader(errors.New("read value bytes failed")))
		_, err := ReadRecord(reader)
		assert.ErrorContains(t, err, "failed to read value bytes")
		assert.ErrorContains(t, err, "read value bytes failed")
	})

	t.Run("Error reading checksum", func(t *testing.T) {
		var buf bytes.Buffer
		ikey := EncodeInternalKey(Bytes("key"), 0, TypeValue)
		writeNumber(&transaction{buffer: &buf, state: "active"}, uint64(len(ikey)))
		writeNumber(&transaction{buffer: &buf, state: "active"}, 5)
		buf.Write(ikey)
		buf.Write([]byte("value"))
		_, err := ReadRecord(&buf)
		assert.ErrorContains(t, err, "failed to read checksum")
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("Checksum mismatch", func(t *testing.T) {
		var buf bytes.Buffer
		ikey := EncodeInternalKey(Bytes("key"), 0, TypeValue)
		writeNumber(&transaction{buffer: &buf, state: "active"}, uint64(len(ikey)))
		writeNumber(&transaction{buffer: &buf, state: "active"}, 5)
		buf.Write(ikey)
		buf.Write([]byte("value"))
		buf.Write([]byte{0, 0, 0, 0})
		_, err := ReadRecord(&buf)
		assert.ErrorIs(t, err, ErrChecksumMismatch)
	})
}
