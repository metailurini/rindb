package rindb

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

// initTempFileSystems creates n temporary FileSystem instances for testing and returns a cleanup function.
func initTempFileSystems(t *testing.T, n int) ([]*FileSystem, func()) {
	fss := make([]*FileSystem, 0, n)
	tempDir := t.TempDir()
	for i := 0; i < n; i++ {
		fs, err := OpenFS(fmt.Sprintf("%s/test-%d", tempDir, i))
		assert.NoError(t, err)
		fss = append(fss, fs)
	}
	return fss, func() {
		for _, fs := range fss {
			_ = fs.Close()
		}
	}
}

// randStringBytes generates a random string of length n as Bytes.
func randStringBytes(n int) Bytes {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

// populateMemtable creates a Memtable and populates it with the given key-value pairs.
func populateMemtable(cfg Config, pairs ...[2]Bytes) Memtable {
	mem := InitMemtable(cfg)
	for _, pair := range pairs {
		mem.Put(pair[0], pair[1])
	}
	return mem
}

// createSSTable is a helper function to create an SSTable for testing.
// It populates a memtable with the given pairs and flushes it to the provided FileSystem.
func createSSTable(t *testing.T, cfg Config, fs *FileSystem, pairs ...[2]Bytes) SStable {
	t.Helper() // Mark this as a test helper function
	mem := populateMemtable(cfg, pairs...)
	sstable, err := Flush(cfg, mem, fs)
	assert.NoError(t, err, "Failed to flush memtable to create SSTable")
	return sstable
}

// debugSkipList prints the contents of a SkipList for debugging.
func debugSkipList[K Comparable, V any](list *SkipList[K, V]) {
	DEBUG("--header--: %v", list.headNote)
	r := list.headNote.Next()
	for r != nil {
		DEBUG("[%v<>%v] ", r.Key, r.Value)
		for _, v := range r.forwards {
			if v == nil {
				continue
			}
			DEBUG("[%v<>%v] ", v.Key, v.Value)
		}
		fmt.Println()
		r = r.Next()
	}
}

// assertIteratorRecords iterates through an Iterator[Record] and asserts that the records match the expected slice.
func assertIteratorRecords(t *testing.T, iter Iterator[Record], expected []Record) {
	t.Helper()
	idx := 0
	for iter.HasNext() {
		record, err := iter.Next()
		assert.NoError(t, err, "Iterator Next() returned an unexpected error at index %d", idx)
		if idx >= len(expected) {
			assert.Failf(t, "Iterator returned more records than expected", "Got extra record: Key=%s, Value=%s", record.GetKey(), record.GetValue())
			return // Stop further checks if lengths mismatch
		}
		assert.Equal(t, expected[idx].GetKey(), record.GetKey(), "Key mismatch at index %d", idx)
		assert.Equal(t, expected[idx].GetValue(), record.GetValue(), "Value mismatch at index %d for key %s", idx, record.GetKey())
		idx++
	}
	assert.Equal(t, len(expected), idx, "Number of records iterated does not match expected count")
	_, err := iter.Next()
	assert.ErrorIs(t, err, EOI, "Iterator should return EOI after iterating through all expected records")
}

// assertIteratorValues iterates through a generic Iterator[T] and asserts that the values match the expected slice.
func assertIteratorValues[T comparable](t *testing.T, iter Iterator[T], expected []T) {
	t.Helper()
	idx := 0
	for iter.HasNext() {
		value, err := iter.Next()
		assert.NoError(t, err, "Iterator Next() returned an unexpected error at index %d", idx)
		if idx >= len(expected) {
			assert.Failf(t, "Iterator returned more values than expected", "Got extra value: %v", value)
			return // Stop further checks if lengths mismatch
		}
		assert.Equal(t, expected[idx], value, "Value mismatch at index %d", idx)
		idx++
	}
	assert.Equal(t, len(expected), idx, "Number of values iterated does not match expected count")
	_, err := iter.Next()
	assert.ErrorIs(t, err, EOI, "Iterator should return EOI after iterating through all expected values")
}
