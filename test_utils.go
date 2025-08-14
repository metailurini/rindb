package rindb

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"io"

	"github.com/stretchr/testify/assert"
)

// testRindbSetup encapsulates setup and cleanup logic for rindb tests.
type testRindbSetup struct {
	T            *testing.T
	Manager      *SSTableManager
	TempDir      string
	Levels       []*LinkedList[*FileSystem]
	CleanupFuncs []func()
}

// newTestRindbSetup initializes a new test setup for rindb with a temporary directory and manager.
func newTestRindbSetup(t *testing.T, cfg *Config) *testRindbSetup {
	tempDir := t.TempDir()
	var finalCfg Config           // Use a value type to copy
	defaultCfg := DefaultConfig() // Get default values

	if cfg == nil {
		// If no config provided, use the default one with the temp dir
		finalCfg = defaultCfg
		finalCfg.databaseDir = tempDir
	} else {
		// If a config is provided, copy it and override the databaseDir
		finalCfg = *cfg                // Copy the provided config
		finalCfg.databaseDir = tempDir // Override the directory

		// Ensure essential default values are applied if the provided config missed them
		// (e.g., if a user created a Config struct manually without using NewConfig)
		// Check against zero values and assign defaults if necessary.
		if finalCfg.maxMemtableSize == 0 {
			finalCfg.maxMemtableSize = defaultCfg.maxMemtableSize
		}
		if finalCfg.level0CompactionThreshold == 0 {
			finalCfg.level0CompactionThreshold = defaultCfg.level0CompactionThreshold
		}
		if finalCfg.baseCompactionSizeMB == 0 {
			finalCfg.baseCompactionSizeMB = defaultCfg.baseCompactionSizeMB
		}
		if finalCfg.levelSizeMultiplier == 0 {
			finalCfg.levelSizeMultiplier = defaultCfg.levelSizeMultiplier
		}
		if finalCfg.bloomFalsePositiveRate == 0.0 {
			finalCfg.bloomFalsePositiveRate = defaultCfg.bloomFalsePositiveRate
		}
		if finalCfg.skipListDefaultLevel == 0 {
			finalCfg.skipListDefaultLevel = defaultCfg.skipListDefaultLevel
		}
		if finalCfg.skipListMaxLevel == 0 {
			finalCfg.skipListMaxLevel = defaultCfg.skipListMaxLevel
		}
		if finalCfg.skipListP == 0.0 {
			finalCfg.skipListP = defaultCfg.skipListP
		}
	}

	manager, err := InitSSTableManager(finalCfg)
	assert.NoError(t, err)

	// Ensure at least 3 levels exist for common test requirements.
	// This loop correctly handles cases where manager.levels might be initialized
	// with some levels already loaded from disk.
	minLevels := 3
	if len(manager.levels) < minLevels {
		needed := minLevels - len(manager.levels)
		for i := 0; i < needed; i++ {
			manager.levels = append(manager.levels, InitLinkedList[*FileSystem]())
		}
	}
	// Ensure existing levels up to minLevels are not nil
	for i := 0; i < minLevels && i < len(manager.levels); i++ {
		if manager.levels[i] == nil {
			manager.levels[i] = InitLinkedList[*FileSystem]()
		}
	}

	cleanup := func() {
		manager.Close()
	}

	return &testRindbSetup{
		T:            t,
		Manager:      manager, // Manager now has the correct config
		TempDir:      tempDir,
		Levels:       manager.levels,
		CleanupFuncs: []func(){cleanup},
	}
}

// AddCleanup adds a cleanup function to be called at the end of the test.
func (ts *testRindbSetup) AddCleanup(f func()) {
	ts.CleanupFuncs = append(ts.CleanupFuncs, f)
}

// Cleanup runs all deferred cleanup functions.
func (ts *testRindbSetup) Cleanup() {
	for i := len(ts.CleanupFuncs) - 1; i >= 0; i-- {
		ts.CleanupFuncs[i]()
	}
}

// newSSTableFS creates a new FileSystem for a given level with automatic cleanup.
func (ts *testRindbSetup) newSSTableFS(level int) *FileSystem {
	fs, err := ts.Manager.NewSSTableFS(level)
	assert.NoError(ts.T, err)
	ts.AddCleanup(func() { fs.Close() })
	return fs
}

// createSSTable creates an SSTable with the given key-value pairs.
func (ts *testRindbSetup) createSSTable(level int, kvs map[string]string) *SStable {
	fs := ts.newSSTableFS(level)
	mem := InitMemtable(ts.Manager.config)
	var seqNum uint64 = 0
	for k, v := range kvs {
		seqNum++
		mem.Put(RecordImpl{Key: Bytes(k), Value: Bytes(v), SequenceNumber: seqNum})
	}
	sstable, err := Flush(ts.Manager.config, mem, fs)
	assert.NoError(ts.T, err)
	return &sstable
}

// createSSTableWithSequence creates an SSTable with the given key-value pairs and a starting sequence number.
func (ts *testRindbSetup) createSSTableWithSequence(level int, kvs map[string]string, startSeqNum uint64) *SStable {
	fs := ts.newSSTableFS(level)
	mem := InitMemtable(ts.Manager.config)
	seqNum := startSeqNum
	for k, v := range kvs {
		mem.Put(RecordImpl{Key: Bytes(k), Value: Bytes(v), SequenceNumber: seqNum})
		seqNum++
	}
	sstable, err := Flush(ts.Manager.config, mem, fs)
	assert.NoError(ts.T, err)
	return &sstable
}

// AddSSTableToLevel adds an SSTable to the specified level.
func (ts *testRindbSetup) AddSSTableToLevel(level int, sstable *SStable) {
	fs := sstable.FileSystem
	ts.Levels[level].PushBack(fs)
}

// createDummyFile creates a dummy file of specified size in MB for testing compaction.
func createDummyFile(t *testing.T, dir, name string, sizeMB int) string {
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	assert.NoError(t, err)
	defer f.Close()
	data := make([]byte, sizeMB*1024*1024)
	_, err = f.Write(data)
	assert.NoError(t, err)
	return path
}

// initTempFileSystems creates n temporary FileSystem instances for testing and returns a cleanup function.
// initialContents, if provided, must have length n. A nil entry means no initial content for that file.
func initTempFileSystems(t *testing.T, n int, initialContents [][]byte) ([]*FileSystem, func()) {
	t.Helper()
	if initialContents != nil && len(initialContents) != n {
		assert.FailNow(t, "initialContents length must match n or be nil")
	}

	fss := make([]*FileSystem, 0, n)
	tempDir := t.TempDir()
	for i := 0; i < n; i++ {
		fs, err := OpenFS(fmt.Sprintf("%s/test-%d", tempDir, i))
		assert.NoError(t, err)

		// Write initial content if provided for this index
		if initialContents != nil && initialContents[i] != nil {
			_, writeErr := fs.Write(initialContents[i])
			assert.NoError(t, writeErr, "Failed to write initial content to temp file %d", i)
			_, seekErr := fs.file.Seek(0, io.SeekStart) // Reset cursor to beginning
			assert.NoError(t, seekErr, "Failed to seek to start after writing initial content to temp file %d", i)
		}

		fss = append(fss, fs)
	}
	return fss, func() {
		for _, fs := range fss {
			// Attempt to close, ignore error as it's cleanup
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

// generateKeyValuePairs creates n random key-value pairs with specified sizes.
func generateKeyValuePairs(n int, keySize, valueSize int) [][2]Bytes {
	pairs := make([][2]Bytes, n)
	for i := 0; i < n; i++ {
		pairs[i] = [2]Bytes{
			randStringBytes(keySize),
			randStringBytes(valueSize),
		}
	}
	return pairs
}

// populateMemtable creates a Memtable and populates it with the given key-value pairs.
func populateMemtable(cfg Config, pairs ...[2]Bytes) Memtable {
	mem := InitMemtable(cfg)
	var seqNum uint64 = 0
	for _, pair := range pairs {
		seqNum++
		mem.Put(RecordImpl{Key: pair[0], Value: pair[1], SequenceNumber: seqNum})
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

// initRinDBWithCleanup initializes a RinDB instance for testing and returns it along with a cleanup function.
// The cleanup function closes the database and removes its directory.
func initRinDBWithCleanup(t *testing.T, opts ...Option) (*Rindb, func()) {
	t.Helper()

	// Apply default test options if none are provided, especially the temp dir
	finalOpts := opts
	hasDirOpt := false
	var dbDir string // Declare dbDir here to be accessible later

	// Check if WithDatabaseDir is already provided
	tempCfgCheck := DefaultConfig() // Create a temporary config to check options
	for _, opt := range opts {
		opt(&tempCfgCheck)
	}
	if tempCfgCheck.databaseDir != DefaultConfig().databaseDir {
		hasDirOpt = true
		dbDir = tempCfgCheck.databaseDir // Use the explicitly provided dir
	}

	if !hasDirOpt {
		dbDir = t.TempDir() // Create a unique temp dir for this test run
		finalOpts = append(finalOpts, WithDatabaseDir(dbDir))
	} else {
		// Ensure the provided options are used, including the explicit dir
		finalOpts = opts
	}

	rin, err := InitRinDB(finalOpts...)
	assert.NoError(t, err, "Failed to initialize RinDB")

	// Ensure the config used for cleanup matches the one RinDB was initialized with
	// If an explicit dir was passed, rin.config.databaseDir should match dbDir
	if hasDirOpt {
		assert.Equal(t, dbDir, rin.config.databaseDir, "Database directory in config does not match provided option")
	}

	cleanup := func() {
		// Use the database directory from the initialized RinDB's config for cleanup
		closeErr := rin.Close()
		// Allow ErrDatabaseClosed because cleanup might be called multiple times by defer + explicit call
		if closeErr != nil && !errors.Is(closeErr, ErrDatabaseClosed) {
			assert.NoError(t, closeErr, "Failed to close RinDB")
		}

	}

	return &rin, cleanup
}

// assertFileExists checks if a file exists at the given path and fails the test if not.
func assertFileExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	assert.NoError(t, err, "Expected file '%s' to exist, but got error: %v", path, err)
}

// assertFileNotExists checks if a file does not exist at the given path and fails the test if it does.
func assertFileNotExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	assert.True(t, os.IsNotExist(err), "Expected file '%s' to not exist, but it does (or another error occurred: %v)", path, err)
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

// assertLinkedListContents checks if the contents of a LinkedList match the expected slice.
func assertLinkedListContents[T comparable](t *testing.T, l *LinkedList[T], expected []T) {
	t.Helper()
	assert.Equal(t, len(expected), l.Len(), "LinkedList length does not match expected length")
	assertIteratorValues(t, l.Iterator(), expected)
}
