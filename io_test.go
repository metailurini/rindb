package rindb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	tx := &transaction{log: fs, state: "active", manager: newTransactionManager(newScopedLogger(nopLogger{}, LogLevelWarn))}
	return tx, func() {
		require.NoError(t, fs.Close())
	}
}

func writeNumberBuf(buf *bytes.Buffer, n uint64) {
	var b [mdByteSize]byte
	byteOrder.PutUint64(b[:], n)
	buf.Write(b[:])
}

func TestRecord_WriteRead(t *testing.T) {
	t.Parallel()
	largeKey, largeValue := func() (Bytes, Bytes) {
		var k strings.Builder
		var v strings.Builder
		for i := 0; i < 500; i++ {
			fmt.Fprintf(&k, "test key %d ", i)
			fmt.Fprintf(&v, "test value %d ", i)
		}
		return Bytes(k.String()), Bytes(v.String())
	}()

	cases := []struct {
		name       string
		key, value Bytes
	}{
		{"Write key with size = 0", nil, Bytes("value")},
		{"Write key and value with size = 0", nil, nil},
		{"Write key and value with size > 255", largeKey, largeValue},
		{"Write key and value with size < 255", Bytes("key"), Bytes("value")},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tx, cleanup := newFileTx(t)
			defer cleanup()
			path := tx.log.Path()

			rec := newRecord(tt.key, tt.value, 0)
			err := writeRecord(tx, rec)
			assert.NoError(t, err)

			data, err := os.ReadFile(path)
			assert.NoError(t, err)
			record, size, err := readRecord(bytes.NewReader(data))
			assert.NoError(t, err)
			assert.Equal(t, CalOnDiskSize(rec), size)
			assert.Equal(t, tt.key, record.GetKey())
			assert.Equal(t, tt.value, record.GetValue())
		})
	}
}

func TestReadRecord_OffsetReaderZeroCopy(t *testing.T) {
	t.Parallel()

	key := Bytes("key")
	value := Bytes("value")
	seq := uint64(42)

	rec := newRecord(key, value, seq)
	tx, cleanup := newFileTx(t)
	defer cleanup()

	require.NoError(t, writeRecord(tx, rec))

	data, err := os.ReadFile(tx.log.Path())
	require.NoError(t, err)
	totalSize := CalOnDiskSize(rec)
	require.Len(t, data, totalSize)

	ctx := context.Background()

	t.Run("zero-copy mmap", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1, [][]byte{data})
		defer closer()

		fs := fss[0]
		fs.configureMmap(ctx, true, newScopedLogger(nopLogger{}, LogLevelWarn))

		r := newOffsetReader(fs, 0)
		record, size, err := readRecord(r)
		require.NoError(t, err)
		assert.Equal(t, totalSize, size)
		assert.Equal(t, key, record.GetKey())
		assert.Equal(t, value, record.GetValue())
		assert.Equal(t, seq, record.GetSequenceNumber())
		assert.Equal(t, TypeValue, record.GetType())
		assert.Equal(t, int64(totalSize), r.Offset())
	})

	t.Run("fallback when mmap disabled", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1, [][]byte{data})
		defer closer()

		fs := fss[0]
		fs.configureMmap(ctx, false, newScopedLogger(nopLogger{}, LogLevelWarn))

		r := newOffsetReader(fs, 0)
		record, size, err := readRecord(r)
		require.NoError(t, err)
		assert.Equal(t, totalSize, size)
		assert.Equal(t, key, record.GetKey())
		assert.Equal(t, value, record.GetValue())
		assert.Equal(t, seq, record.GetSequenceNumber())
		assert.Equal(t, TypeValue, record.GetType())
		assert.Equal(t, int64(totalSize), r.Offset())
	})
}

func TestReadRecord_Errors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		setup       func() io.Reader
		errIs       error
		errContains []string
	}{
		{
			name: "Error reading internal key length",
			setup: func() io.Reader {
				return &errorReader{err: errors.New("read internal key length failed")}
			},
			errContains: []string{"failed to read internal key length", "read internal key length failed"},
		},
		{
			name: "Error reading value length",
			setup: func() io.Reader {
				var buf bytes.Buffer
				writeNumberBuf(&buf, 5)
				return io.MultiReader(&buf, &errorReader{err: errors.New("read value length failed")})
			},
			errContains: []string{"failed to read value length", "read value length failed"},
		},
		{
			name: "Error reading internal key bytes",
			setup: func() io.Reader {
				var buf bytes.Buffer
				writeNumberBuf(&buf, 5)
				writeNumberBuf(&buf, 5)
				buf.Write([]byte("key"))
				return &buf
			},
			errContains: []string{"failed to read internal key bytes"},
			errIs:       io.ErrUnexpectedEOF,
		},
		{
			name: "Error reading value bytes (EOF)",
			setup: func() io.Reader {
				var buf bytes.Buffer
				writeNumberBuf(&buf, 3+internalKeySuffixLen)
				writeNumberBuf(&buf, 5)
				buf.Write(EncodeInternalKey(Bytes("key"), 0, TypeValue))
				buf.Write([]byte("val"))
				return &buf
			},
			errContains: []string{"failed to read value bytes"},
			errIs:       io.ErrUnexpectedEOF,
		},
		{
			name: "Error reading value bytes (iotest)",
			setup: func() io.Reader {
				var buf bytes.Buffer
				writeNumberBuf(&buf, 3+internalKeySuffixLen)
				writeNumberBuf(&buf, 5)
				buf.Write(EncodeInternalKey(Bytes("key"), 0, TypeValue))
				return io.MultiReader(&buf, iotest.ErrReader(errors.New("read value bytes failed")))
			},
			errContains: []string{"failed to read value bytes", "read value bytes failed"},
		},
		{
			name: "Error reading checksum",
			setup: func() io.Reader {
				var buf bytes.Buffer
				ikey := EncodeInternalKey(Bytes("key"), 0, TypeValue)
				writeNumberBuf(&buf, uint64(len(ikey)))
				writeNumberBuf(&buf, 5)
				buf.Write(ikey)
				buf.Write([]byte("value"))
				return &buf
			},
			errContains: []string{"failed to read checksum"},
			errIs:       io.EOF,
		},
		{
			name: "Checksum mismatch",
			setup: func() io.Reader {
				var buf bytes.Buffer
				ikey := EncodeInternalKey(Bytes("key"), 0, TypeValue)
				writeNumberBuf(&buf, uint64(len(ikey)))
				writeNumberBuf(&buf, 5)
				buf.Write(ikey)
				buf.Write([]byte("value"))
				buf.Write([]byte{0, 0, 0, 0})
				return &buf
			},
			errIs: ErrChecksumMismatch,
		},
		{
			name: "Error reading size trailer",
			setup: func() io.Reader {
				var buf bytes.Buffer
				ikey := EncodeInternalKey(Bytes("key"), 0, TypeValue)
				value := Bytes("value")
				writeNumberBuf(&buf, uint64(len(ikey)))
				writeNumberBuf(&buf, uint64(len(value)))
				buf.Write(ikey)
				buf.Write(value)
				var checksumBytes [checksumSize]byte
				chk := checksum(ikey, value)
				byteOrder.PutUint32(checksumBytes[:], chk)
				buf.Write(checksumBytes[:])
				return &buf
			},
			errContains: []string{"failed to read record size trailer"},
			errIs:       io.EOF,
		},
		{
			name: "Record size mismatch",
			setup: func() io.Reader {
				var buf bytes.Buffer
				ikey := EncodeInternalKey(Bytes("key"), 0, TypeValue)
				value := Bytes("value")
				writeNumberBuf(&buf, uint64(len(ikey)))
				writeNumberBuf(&buf, uint64(len(value)))
				buf.Write(ikey)
				buf.Write(value)
				var checksumBytes [checksumSize]byte
				chk := checksum(ikey, value)
				byteOrder.PutUint32(checksumBytes[:], chk)
				buf.Write(checksumBytes[:])
				writeNumberBuf(&buf, 1)
				return &buf
			},
			errContains: []string{"record size mismatch"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := tt.setup()
			_, _, err := readRecord(r)
			for _, msg := range tt.errContains {
				assert.ErrorContains(t, err, msg)
			}
			if tt.errIs != nil {
				assert.ErrorIs(t, err, tt.errIs)
			}
		})
	}
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

	baseLen := buf.Len()
	writeNumberBuf(&buf, uint64(baseLen+mdByteSize))

	recordBytes := buf.Bytes()

	b.SetBytes(int64(len(recordBytes)))

	for b.Loop() {
		_, _, err := readRecord(bytes.NewReader(recordBytes))
		if err != nil {
			b.Fatal(err)
		}
	}
}
