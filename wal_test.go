package rindb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validateWALFormat validates that the WAL records are well-formed according to
// the record structure used by WriteRecord/ReadRecord. It checks that each
// record contains the internal key (including sequence number and type) and the
// associated value bytes.
func validateWALFormat(t *testing.T, file io.ReadSeeker) {
	_, err := file.Seek(0, io.SeekStart)
	assert.NoError(t, err)

	for {
		// Read the internal key length. io.ReadFull ensures we either
		// read the full length or get an EOF/ErrUnexpectedEOF.
		keyLenBytes := [mdByteSize]byte{}
		if _, err := io.ReadFull(file, keyLenBytes[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(t, err)
		}

		// Read the value length.
		valueLenBytes := [mdByteSize]byte{}
		_, err = io.ReadFull(file, valueLenBytes[:])
		assert.NoError(t, err)

		keyLen := byteOrder.Uint64(keyLenBytes[:])
		valueLen := byteOrder.Uint64(valueLenBytes[:])

		// The internal key must at least contain the sequence number
		// and type information.
		assert.GreaterOrEqual(t, keyLen, uint64(internalKeySuffixLen))

		// Read and decode the internal key bytes to ensure the
		// sequence number and type are encoded correctly.
		keyBytes := make([]byte, keyLen)
		_, err = io.ReadFull(file, keyBytes)
		assert.NoError(t, err)

		userKey, seq, typ, err := DecodeInternalKey(keyBytes)
		assert.NoError(t, err)
		// The encoded length should match user key length plus the
		// suffix for sequence number and type.
		assert.Equal(t, int(keyLen), len(userKey)+internalKeySuffixLen)
		// Ensure the record type is one of the valid constants.
		assert.Contains(t, []RecordType{TypeValue, TypeDeletion, TypeMerge}, typ)
		// Sequence number can be any uint64, but decoding should not
		// return a negative value or overflow. Since seq is uint64, no
		// additional check is needed beyond successful decoding.

		// Read value bytes to move the file cursor and ensure correct
		// length.
		valueBytes := make([]byte, valueLen)
		_, err = io.ReadFull(file, valueBytes)
		assert.NoError(t, err)

		// Read checksum and verify
		checksumBytes := [checksumSize]byte{}
		_, err = io.ReadFull(file, checksumBytes[:])
		assert.NoError(t, err)
		expected := byteOrder.Uint32(checksumBytes[:])
		actual := checksum(keyBytes, valueBytes)
		assert.Equal(t, expected, actual)

		_ = seq // silence unused warning if seq not used otherwise
	}
}

// TestWAL_Clean tests cleaning the WAL.
func TestWAL_Clean(t *testing.T) {
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]
	w := NewWAL(cfg, fs)
	err := w.Append(context.Background(), newRecord(Bytes("key"), Bytes("value"), 0)) // SequenceNumber can be 0 if not relevant for the test
	assert.NoError(t, err)
	err = w.Clean(context.Background(), 1)
	assert.NoError(t, err)
	b2 := make(Bytes, 5)
	_, err = fs.Read(b2)
	assert.NotNil(t, err)
	mem, err := w.Load(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, mem.data.Len())
	err = w.Close()
	assert.NoError(t, err)
}

func TestWAL_CleanErrors(t *testing.T) {
	cfg := testConfig()

	t.Run("ChecksumMismatch", func(t *testing.T) {
		ctx := context.Background()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]
		w := NewWAL(cfg, fs)
		require.NoError(t, w.Append(ctx, newRecord(Bytes("k"), Bytes("v"), 1)))

		info, err := fs.file.Stat()
		require.NoError(t, err)
		_, err = fs.file.WriteAt([]byte{0}, info.Size()-1)
		require.NoError(t, err)

		err = w.Clean(ctx, 0)
		assert.ErrorIs(t, err, ErrChecksumMismatch)
	})

	t.Run("WriteRecordFailure", func(t *testing.T) {
		ctx := context.Background()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]
		w := NewWAL(cfg, fs)
		require.NoError(t, w.Append(ctx, newRecord(Bytes("k"), Bytes("v"), 1)))

		w.writeRecord = func(tx *transaction, rec Record) error { return errors.New("write fail") }

		err := w.Clean(ctx, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to write record")
	})

	t.Run("CommitFailure", func(t *testing.T) {
		ctx := context.Background()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]
		w := NewWAL(cfg, fs)
		require.NoError(t, w.Append(ctx, newRecord(Bytes("k"), Bytes("v"), 1)))

		w.txCommit = func(tx *transaction, ctx context.Context) error {
			return errors.New("commit fail")
		}

		err := w.Clean(ctx, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to commit")
	})
}

// TestWAL_AppendAndLoad tests appending and loading records from the WAL.
func TestWAL_AppendAndLoad(t *testing.T) {
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]
	t.Run("SingleRecord", func(t *testing.T) {
		w := NewWAL(cfg, fs)
		err := w.Append(context.Background(), newRecord(Bytes("single_key"), Bytes("single_value"), 1))
		assert.NoError(t, err)
		mem, err := w.Load(context.Background())
		assert.NoError(t, err)
		got, err := mem.Get(Bytes("single_key"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("single_value"), got)
	})
	t.Run("MultipleRecords", func(t *testing.T) {
		// Reopen filesystem to reset state
		_ = fs.Close()
		var err error
		fs, err = OpenFS(context.Background(), fs.Path())
		assert.NoError(t, err)
		w := NewWAL(cfg, fs)

		recordsSize := 1_000
		records := make([]Record, 0, recordsSize)
		for i := range recordsSize {
			records = append(records, newRecord(Bytes(fmt.Sprintf("key.%d", i)), Bytes(fmt.Sprintf("value.%d", i)), uint64(i)))
		}
		err = w.AppendMany(context.Background(), records)
		assert.NoError(t, err)
		validateWALFormat(t, w.file)
		mem, err := w.Load(context.Background())
		assert.NoError(t, err)
		for i := range recordsSize {
			key := Bytes(fmt.Sprintf("key.%d", i))
			expectedValue := Bytes(fmt.Sprintf("value.%d", i))
			got, err := mem.Get(key)
			assert.NoError(t, err)
			assert.Equal(t, expectedValue, got)
		}
	})
}

// TestWALCrashRecovery tests WAL recovery after a crash.
func TestWALCrashRecovery(t *testing.T) {
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]
	w := NewWAL(cfg, fs)
	k1 := randStringBytes(10)
	k2 := randStringBytes(10)
	records := []Record{
		newRecord(k1, Bytes("v1"), 1),
		newRecord(k2, Bytes("v2"), 2),
	}
	for _, r := range records {
		assert.NoError(t, w.Append(context.Background(), r))
	}
	assert.NoError(t, fs.Close())
	fs, err := OpenFS(context.Background(), fs.Path())
	assert.NoError(t, err)
	w = NewWAL(cfg, fs)
	mem, err := w.Load(context.Background())
	assert.NoError(t, err)
	v1, err := mem.Get(k1)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v1"), v1)
	v2, err := mem.Get(k2)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v2"), v2)
}

func TestDefaultNewWALFunc_ReadDirError(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "notadir")
	assert.NoError(t, os.WriteFile(file, []byte(""), 0o644))

	cfg := testConfig()
	cfg.databaseDir = file

	_, err := DefaultNewWALFunc(context.Background(), cfg)
	assert.Error(t, err)
}

// TestWALCrashRecovery_PartialWrite tests WAL recovery after a crash during a partial write.
func TestWALCrashRecovery_PartialWrite(t *testing.T) {
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]
	w := NewWAL(cfg, fs)

	// Write a few complete records
	record1 := newRecord(Bytes("key1"), Bytes("value1"), 1)
	record2 := newRecord(Bytes("key2"), Bytes("value2"), 2)

	assert.NoError(t, w.Append(context.Background(), record1))
	assert.NoError(t, w.Append(context.Background(), record2))

	// Simulate a crash during the write of a third record
	record3 := newRecord(Bytes("key3"), Bytes("value3"), 3)
	tx, err := w.tm.begin(w.FileSystem)
	require.NoError(t, err)
	defer tx.rollback(context.Background())

	// Write only the internal key length and value length of the third record
	assert.NoError(t, writeNumber(tx, uint64(len(record3.GetKey())+internalKeySuffixLen)))
	assert.NoError(t, writeNumber(tx, uint64(len(record3.GetValue()))))

	// Commit the partial transaction to the file
	assert.NoError(t, tx.commit(context.Background()))
	assert.NoError(t, w.Sync())

	// Truncate the file to simulate a crash before writing key/value bytes
	fileInfo, err := w.file.Stat()
	assert.NoError(t, err)
	currentSize := fileInfo.Size()
	// Truncate after writing the lengths, before writing the key
	truncateSize := currentSize // Keep the lengths, but no key/value
	assert.NoError(t, w.file.Truncate(truncateSize))
	assert.NoError(t, w.Sync()) // Ensure truncation is synced

	// Close and re-open the file system to simulate recovery
	assert.NoError(t, fs.Close())
	fs, err = OpenFS(context.Background(), fs.Path())
	assert.NoError(t, err)
	w = NewWAL(cfg, fs)

	// Load the WAL and verify that only the complete records are loaded
	mem, err := w.Load(context.Background())
	assert.NoError(t, err) // Load should ideally not return an error for partial records

	// Verify records 1 and 2 are present
	v1, err := mem.Get(Bytes("key1"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value1"), v1)

	v2, err := mem.Get(Bytes("key2"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value2"), v2)

	// Verify record 3 is NOT present
	_, err = mem.Get(Bytes("key3"))
	assert.Error(t, err) // Should return an error as key3 was not fully written
}

// TestWAL_LoadChecksumMismatch ensures checksum errors surface when loading WAL.
func TestWAL_LoadChecksumMismatch(t *testing.T) {
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]
	w := NewWAL(cfg, fs)
	assert.NoError(t, w.Append(context.Background(), newRecord(Bytes("k"), Bytes("v"), 1)))

	info, err := fs.file.Stat()
	assert.NoError(t, err)
	_, err = fs.WriteAt([]byte{0}, info.Size()-1)
	assert.NoError(t, err)

	_, err = w.Load(context.Background())
	assert.ErrorIs(t, err, ErrChecksumMismatch)
}
