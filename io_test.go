package rindb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errorReader simulates an io.Reader that returns an error.
type errorReader struct {
	err error
}

func (er *errorReader) Read(p []byte) (n int, err error) {
	return 0, er.err
}

func newFileTx(t *testing.T) (*transaction, func()) {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "txn.log")
	fs, err := OpenFS(context.Background(), p)
	require.NoError(t, err)
	tx := &transaction{log: fs, state: "active"}
	return tx, func() {
		require.NoError(t, fs.Close())
	}
}

func writeNumberBuf(buf *bytes.Buffer, n uint64) {
	var b [mdByteSize]byte
	byteOrder.PutUint64(b[:], n)
	buf.Write(b[:])
}

func Test_rw(t *testing.T) {
	t.Run("Write key with size = 0", func(t *testing.T) {
		tx, cleanup := newFileTx(t)
		defer cleanup()
		path := tx.log.Path()
		testKey := Bytes(nil)
		testValue := Bytes("value")

		err := writeRecord(tx, newRecord(testKey, testValue, 0))
		assert.NoError(t, err)

		data, err := os.ReadFile(path)
		assert.NoError(t, err)
		record, err := readRecord(bytes.NewReader(data))
		assert.NoError(t, err)
		assert.Equal(t, Bytes(nil), record.GetKey())
		assert.Equal(t, testValue, record.GetValue())
	})

	t.Run("Write key and value with size = 0", func(t *testing.T) {
		tx, cleanup := newFileTx(t)
		defer cleanup()
		path := tx.log.Path()
		testKey := Bytes(nil)
		testValue := Bytes(nil)

		err := writeRecord(tx, newRecord(testKey, testValue, 0))
		assert.NoError(t, err)

		data, err := os.ReadFile(path)
		assert.NoError(t, err)
		record, err := readRecord(bytes.NewReader(data))
		assert.NoError(t, err)
		assert.Equal(t, testKey, record.GetKey())
		assert.Equal(t, testValue, record.GetValue())
	})

	t.Run("Write key and value with size > 255", func(t *testing.T) {
		tx, cleanup := newFileTx(t)
		defer cleanup()
		path := tx.log.Path()
		testKey := ""
		testValue := ""
		for i := 0; i < 500; i++ {
			testKey += fmt.Sprintf("test key %d ", i)
			testValue += fmt.Sprintf("test value %d ", i)
		}

		err := writeRecord(tx, newRecord(Bytes(testKey), Bytes(testValue), 0))
		assert.NoError(t, err)

		data, err := os.ReadFile(path)
		assert.NoError(t, err)
		record, err := readRecord(bytes.NewReader(data))
		assert.NoError(t, err)
		assert.Equal(t, testKey, string(record.GetKey()))
		assert.Equal(t, testValue, string(record.GetValue()))
	})

	t.Run("Write key and value with size < 255", func(t *testing.T) {
		tx, cleanup := newFileTx(t)
		defer cleanup()
		path := tx.log.Path()
		err := writeRecord(tx, newRecord(Bytes("key"), Bytes("value"), 0))
		assert.NoError(t, err)

		data, err := os.ReadFile(path)
		assert.NoError(t, err)
		record, err := readRecord(bytes.NewReader(data))
		assert.NoError(t, err)
		assert.Equal(t, "key", string(record.GetKey()))
		assert.Equal(t, "value", string(record.GetValue()))
	})
}

func TestReadRecord_Errors(t *testing.T) {
	t.Run("Error reading internal key length", func(t *testing.T) {
		reader := &errorReader{err: errors.New("read internal key length failed")}
		_, err := readRecord(reader)
		assert.ErrorContains(t, err, "failed to read internal key length")
		assert.ErrorContains(t, err, "read internal key length failed")
	})

	t.Run("Error reading value length", func(t *testing.T) {
		var buf bytes.Buffer
		writeNumberBuf(&buf, 5)
		reader := io.MultiReader(&buf, &errorReader{err: errors.New("read value length failed")})
		_, err := readRecord(reader)
		assert.ErrorContains(t, err, "failed to read value length")
		assert.ErrorContains(t, err, "read value length failed")
	})

	t.Run("Error reading internal key bytes", func(t *testing.T) {
		var buf bytes.Buffer
		writeNumberBuf(&buf, 5)
		writeNumberBuf(&buf, 5)
		buf.Write([]byte("key"))
		_, err := readRecord(&buf)
		assert.ErrorContains(t, err, "failed to read internal key bytes")
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("Error reading value bytes (EOF)", func(t *testing.T) {
		var buf bytes.Buffer
		writeNumberBuf(&buf, 3+internalKeySuffixLen)
		writeNumberBuf(&buf, 5)
		buf.Write(EncodeInternalKey(Bytes("key"), 0, TypeValue))
		buf.Write([]byte("val"))
		_, err := readRecord(&buf)
		assert.ErrorContains(t, err, "failed to read value bytes")
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("Error reading value bytes (iotest)", func(t *testing.T) {
		var buf bytes.Buffer
		writeNumberBuf(&buf, 3+internalKeySuffixLen)
		writeNumberBuf(&buf, 5)
		buf.Write(EncodeInternalKey(Bytes("key"), 0, TypeValue))
		reader := io.MultiReader(&buf, iotest.ErrReader(errors.New("read value bytes failed")))
		_, err := readRecord(reader)
		assert.ErrorContains(t, err, "failed to read value bytes")
		assert.ErrorContains(t, err, "read value bytes failed")
	})

	t.Run("Error reading checksum", func(t *testing.T) {
		var buf bytes.Buffer
		ikey := EncodeInternalKey(Bytes("key"), 0, TypeValue)
		writeNumberBuf(&buf, uint64(len(ikey)))
		writeNumberBuf(&buf, 5)
		buf.Write(ikey)
		buf.Write([]byte("value"))
		_, err := readRecord(&buf)
		assert.ErrorContains(t, err, "failed to read checksum")
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("Checksum mismatch", func(t *testing.T) {
		var buf bytes.Buffer
		ikey := EncodeInternalKey(Bytes("key"), 0, TypeValue)
		writeNumberBuf(&buf, uint64(len(ikey)))
		writeNumberBuf(&buf, 5)
		buf.Write(ikey)
		buf.Write([]byte("value"))
		buf.Write([]byte{0, 0, 0, 0})
		_, err := readRecord(&buf)
		assert.ErrorIs(t, err, ErrChecksumMismatch)
	})
}

func BenchmarkReadRecord(b *testing.B) {
	key := Bytes("my-key")
	value := Bytes("my-value")
	var buf bytes.Buffer

	// Manually construct the record to write to the buffer.
	ikey := EncodeInternalKey(key, 1, TypeValue)
	val := value

	// Write lengths
	writeNumberBuf(&buf, uint64(len(ikey)))
	writeNumberBuf(&buf, uint64(len(val)))

	// Write data
	buf.Write(ikey)
	buf.Write(val)

	// Write checksum
	var checksumBytes [checksumSize]byte
	chk := checksum(ikey, val)
	byteOrder.PutUint32(checksumBytes[:], chk)
	buf.Write(checksumBytes[:])

	recordBytes := buf.Bytes()

	b.SetBytes(int64(len(recordBytes)))

	for b.Loop() {
		_, err := readRecord(bytes.NewReader(recordBytes))
		if err != nil {
			b.Fatal(err)
		}
	}
}
