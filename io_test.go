package rindb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
)

// errorWriter simulates an io.Writer that returns an error.
type errorWriter struct {
	err error
}

func (ew *errorWriter) Write(p []byte) (n int, err error) {
	return 0, ew.err
}

// errorReader simulates an io.Reader that returns an error.
type errorReader struct {
	err error
}

func (er *errorReader) Read(p []byte) (n int, err error) {
	return 0, er.err
}

func Test_rw(t *testing.T) {
	t.Run("Write key with size = 0", func(t *testing.T) {
		tx := NewTransactionManager(context.Background()).Begin()

		testKey := Bytes(nil)
		testValue := Bytes("value")

		err := WriteRecord(tx, NewRecord(testKey, testValue, 0))
		assert.NoError(t, err)

		record, err := ReadRecord(tx.buffer)
		assert.NoError(t, err)
		assert.Equal(t, Bytes(nil), record.GetKey())
		assert.Equal(t, testValue, record.GetValue())
	})

	t.Run("Write key and value with size = 0", func(t *testing.T) {
		tx := NewTransactionManager(context.Background()).Begin()

		testKey := Bytes(nil)
		testValue := Bytes(nil)

		err := WriteRecord(tx, NewRecord(testKey, testValue, 0))
		assert.NoError(t, err)

		record, err := ReadRecord(tx.buffer)
		assert.NoError(t, err)
		assert.Equal(t, testKey, record.GetKey())
		assert.Equal(t, testValue, record.GetValue())
	})

	t.Run("Write key and value with size > 255", func(t *testing.T) {
		tx := NewTransactionManager(context.Background()).Begin()

		testKey := ""
		testValue := ""
		for i := 0; i < 500; i++ {
			testKey += fmt.Sprintf("test key %d ", i)
			testValue += fmt.Sprintf("test value %d ", i)
		}

		err := WriteRecord(tx, NewRecord(Bytes(testKey), Bytes(testValue), 0))
		assert.NoError(t, err)

		record, err := ReadRecord(tx.buffer)
		assert.NoError(t, err)
		assert.Equal(t, testKey, string(record.GetKey()))
		assert.Equal(t, testValue, string(record.GetValue()))
	})

	t.Run("Write key and value with size < 255", func(t *testing.T) {
		tx := NewTransactionManager(context.Background()).Begin()

		err := WriteRecord(tx, NewRecord(Bytes("key"), Bytes("value"), 0))
		assert.NoError(t, err)

		record, err := ReadRecord(tx.buffer)
		assert.NoError(t, err)
		assert.Equal(t, "key", string(record.GetKey()))
		assert.Equal(t, "value", string(record.GetValue()))
	})
}

func TestReadRecord_Errors(t *testing.T) {
	t.Run("Error reading key length", func(t *testing.T) {
		reader := &errorReader{err: errors.New("read key length failed")}
		_, err := ReadRecord(reader)
		assert.ErrorContains(t, err, "failed to read key length")
		assert.ErrorContains(t, err, "read key length failed")
	})

	t.Run("Error reading value length", func(t *testing.T) {
		// Provide valid key length bytes, then error
		var buf bytes.Buffer
		WriteNumber(&Transaction{buffer: &buf}, 5) // Write a dummy key length
		reader := io.MultiReader(&buf, &errorReader{err: errors.New("read key length failed")})
		_, err := ReadRecord(reader)
		assert.ErrorContains(t, err, "failed to read key length")
		assert.ErrorContains(t, err, "read key length failed")
	})

	t.Run("Error reading value length (EOF)", func(t *testing.T) {
		var buf bytes.Buffer
		WriteNumber(&Transaction{buffer: &buf}, 5) // Key length = 5
		WriteNumber(&Transaction{buffer: &buf}, 5) // Value length = 5
		buf.Write([]byte("key"))                   // Only 3 bytes of key data
		_, err := ReadRecord(&buf)
		assert.ErrorContains(t, err, "failed to read value length")
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("Error reading key length (iotest)", func(t *testing.T) {
		var buf bytes.Buffer
		WriteNumber(&Transaction{buffer: &buf}, 5) // Key length = 5
		WriteNumber(&Transaction{buffer: &buf}, 5) // Value length = 5
		// Use iotest.ErrReader to simulate read error after header
		reader := io.MultiReader(&buf, iotest.ErrReader(errors.New("read key length failed")))
		_, err := ReadRecord(reader)
		assert.ErrorContains(t, err, "failed to read key length")
		assert.ErrorContains(t, err, "read key length failed")
	})

	t.Run("Error reading value length (EOF)", func(t *testing.T) {
		var buf bytes.Buffer
		WriteNumber(&Transaction{buffer: &buf}, 3) // Key length = 3
		WriteNumber(&Transaction{buffer: &buf}, 5) // Value length = 5
		buf.Write([]byte("key"))                   // Correct key bytes
		buf.Write([]byte("val"))                   // Only 3 bytes of value data
		_, err := ReadRecord(&buf)
		assert.ErrorContains(t, err, "failed to read value length")
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("Error reading value bytes (iotest)", func(t *testing.T) {
		var buf bytes.Buffer
		WriteNumber(&Transaction{buffer: &buf}, 3) // Key length = 3
		WriteNumber(&Transaction{buffer: &buf}, 5) // Value length = 5
		buf.Write([]byte("key"))                   // Correct key bytes
		// Use iotest.ErrReader to simulate read error after key
		reader := io.MultiReader(&buf, iotest.ErrReader(errors.New("read value bytes failed")))
		_, err := ReadRecord(reader)
		assert.ErrorContains(t, err, "failed to read value length")
		assert.ErrorContains(t, err, "read value bytes failed")
	})
}
