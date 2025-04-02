package rindb

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// createDummyFile creates a file of the specified size in MB.
func createDummyFile(t *testing.T, dir string, fileName string, sizeMB int) string {
	t.Helper()
	filePath := filepath.Join(dir, fileName)
	sizeBytes := int64(sizeMB) * 1024 * 1024
	file, err := os.Create(filePath)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, file.Close()) }()

	// Seek to the desired size - 1 and write a single byte
	if sizeBytes > 0 {
		_, err = file.Seek(sizeBytes-1, 0)
		assert.NoError(t, err)
		_, err = file.Write([]byte{0})
		assert.NoError(t, err)
	} else {
		// Ensure the file is empty if sizeMB is 0
		err = file.Truncate(0)
		assert.NoError(t, err)
	}

	return filePath
}

func testOptions() []Option {
	return []Option{
		WithDatabaseDir("testdata"),
	}
}

func testConfig() Config {
	return NewConfig(testOptions()...)
}

// TestRindb_Init tests the initialization of the Rindb database.
func TestRindb_Init(t *testing.T) {
	_, err := InitRinDB(testOptions()...)
	assert.NoError(t, err)
}

// TestRindb_Put tests the Put operation of Rindb.
func TestRindb_Put(t *testing.T) {
	rin, err := InitRinDB(testOptions()...)
	assert.NoError(t, err)
	t.Run("BasicPut", func(t *testing.T) {
		err := rin.Put(Bytes("key"), Bytes("value"))
		assert.NoError(t, err)
	})
	t.Run("Tombstone", func(t *testing.T) {
		err := rin.Put(Bytes("rm-key"), nil)
		assert.NoError(t, err)
	})
}

// TestRindb_Get tests the Get operation of Rindb.
func TestRindb_Get(t *testing.T) {
	rin, err := InitRinDB(testOptions()...)
	rin.config = testConfig()
	assert.NoError(t, err)
	key := Bytes("key")
	err = rin.Put(key, Bytes("value"))
	assert.NoError(t, err)
	value, err := rin.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value"), value)
}

// TestRindb_Remove tests the Remove operation of Rindb.
func TestRindb_Remove(t *testing.T) {
	rin, err := InitRinDB(testOptions()...)
	rin.config = testConfig()
	assert.NoError(t, err)
	key := Bytes("rm-key")
	err = rin.Put(key, Bytes("value"))
	assert.NoError(t, err)
	err = rin.Remove(key)
	assert.NoError(t, err)
	value, err := rin.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, Bytes(nil), value)
}

// TestRindb_FlushMemtable tests flushing the memtable to an SSTable.
func TestRindb_FlushMemtable(t *testing.T) {
	rin, err := InitRinDB(testOptions()...)
	rin.config = testConfig()
	assert.NoError(t, err)
	err = rin.Put(Bytes("key"), Bytes("value"))
	assert.NoError(t, err)
	err = rin.Put(Bytes("rm-key"), Bytes("value"))
	assert.NoError(t, err)
	err = rin.Remove(Bytes("rm-key"))
	assert.NoError(t, err)
	// SSTableManager is now part of rin, no need to init separately
	// We still need a new FS for the flush operation itself
	newSSTableFS, err := rin.ssTableManager.NewSSTableFS(0)
	assert.NoError(t, err)
	defer func() { _ = newSSTableFS.Close() }() // Ensure the FS used for flushing is closed
	newSStable, err := Flush(rin.config, rin.memtable, newSSTableFS)
	assert.NoError(t, err)
	err = rin.wal.Clean()
	assert.NoError(t, err)
	value, err := newSStable.GetValue(Bytes("rm-key"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes(nil), value)
	value, err = newSStable.GetValue(Bytes("key"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value"), value)
}

// TestRindb_GetPrecedence tests that Get prioritizes Memtable over SSTables.
func TestRindb_GetPrecedence(t *testing.T) {
	rin, err := InitRinDB(testOptions()...)
	rin.config = testConfig() // Ensure config is set if needed by test logic beyond Init
	assert.NoError(t, err)
	// SSTableManager is now part of rin
	fs, err := rin.ssTableManager.NewSSTableFS(0) // Create FS for the initial SSTable
	assert.NoError(t, err)
	// Defer close for the FS used in the test setup
	defer func() {
		if fs.IsOpened() {
			assert.NoError(t, fs.Close())
		}
	}()
	mem := InitMemtable(rin.config)
	mem.Put(Bytes("k1"), Bytes("v1-sst"))
	_, err = Flush(rin.config, mem, fs) // Flush memtable to the new FS
	assert.NoError(t, err)
	// Ensure level 0 exists before pushing back
	if len(rin.ssTableManager.levels) == 0 {
		rin.ssTableManager.levels = append(rin.ssTableManager.levels, InitLinkedList[*FileSystem]())
	} else if rin.ssTableManager.levels[0] == nil {
		rin.ssTableManager.levels[0] = InitLinkedList[*FileSystem]()
	}
	rin.ssTableManager.levels[0].PushBack(fs)   // Add the newly created SSTable FS to the manager
	err = rin.Put(Bytes("k1"), Bytes("v1-mem")) // Put the value into the memtable
	assert.NoError(t, err)
	v, err := rin.Get(Bytes("k1"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v1-mem"), v)
}

// TestRindb_ConcurrentCRUD tests concurrent CRUD operations on Rindb.
func TestRindb_ConcurrentCRUD(t *testing.T) {
	rin, err := InitRinDB(testOptions()...)
	rin.config = testConfig()
	assert.NoError(t, err)
	var wg sync.WaitGroup
	const numGoroutines = 10
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			key := Bytes(fmt.Sprintf("k%d", i))
			value := Bytes(fmt.Sprintf("v%d", i))
			assert.NoError(t, rin.Put(key, value))
			v, err := rin.Get(key)
			assert.NoError(t, err)

			assert.Equal(t, value, v)
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			value = Bytes(fmt.Sprintf("v%d-updated", i))
			assert.NoError(t, rin.Put(key, value))

			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			v, err = rin.Get(key)
			assert.NoError(t, err)
			assert.Equal(t, value, v)
			assert.NoError(t, rin.Remove(key))
		}(i)
	}
	wg.Wait()
}

// TestRindb_Put_FlushOnMaxSize tests that the memtable is flushed when maxMemtableSize is reached.
func TestRindb_Put_FlushOnMaxSize(t *testing.T) {
	t.Skip()
	tempDir := t.TempDir()
	maxSize := uint(3) // Set a small memtable size for testing
	opts := []Option{
		WithDatabaseDir(tempDir),
		WithMaxMemtableSize(maxSize),
	}
	cfg := NewConfig(opts...)

	rin, err := InitRinDB(opts...)
	assert.NoError(t, err)
	// Note: InitRinDB loads WAL, so memtable might not be empty initially if WAL existed.
	// We'll rely on the Put logic to trigger the flush regardless of initial state.

	// Insert items up to the max size
	keys := make([]Bytes, 0, maxSize+1)
	for i := 0; i < int(maxSize); i++ {
		key := Bytes(fmt.Sprintf("key%d", i))
		value := Bytes(fmt.Sprintf("value%d", i))
		keys = append(keys, key)
		err = rin.Put(key, value)
		assert.NoError(t, err)
		// Memtable size should increase until flush
		assert.LessOrEqual(t, rin.memtable.data.Len(), maxSize, "Memtable size should be <= maxSize before flush")
	}

	// At this point, memtable should be full (or close if WAL loaded some)
	assert.Equal(t, maxSize, rin.memtable.data.Len(), "Memtable should be full before the triggering Put")

	// Insert one more item to trigger the flush
	triggerKey := Bytes(fmt.Sprintf("key%d", maxSize))
	triggerValue := Bytes(fmt.Sprintf("value%d", maxSize))
	keys = append(keys, triggerKey)
	err = rin.Put(triggerKey, triggerValue)
	assert.NoError(t, err)

	// Memtable should be cleared after flush
	assert.Equal(t, uint(0), rin.memtable.data.Len(), "Memtable should be empty after flush")

	// Verify an SSTable file was created in level 0
	files, err := os.ReadDir(cfg.databaseDir)
	assert.NoError(t, err)
	foundSSTable := false
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), "l00_") && strings.HasSuffix(file.Name(), ".sst") {
			foundSSTable = true
			// Check if the SSTable is not empty (basic check)
			info, statErr := file.Info()
			assert.NoError(t, statErr)
			assert.Greater(t, info.Size(), int64(0), "SSTable file should not be empty")
			break
		}
	}
	assert.True(t, foundSSTable, "Level 0 SSTable file should exist after flush")

	// Verify all keys can still be retrieved (from the new SSTable)
	for i, key := range keys {
		expectedValue := Bytes(fmt.Sprintf("value%d", i))
		value, getErr := rin.Get(key)
		assert.NoError(t, getErr, "Error getting key %s after flush", string(key))
		assert.Equal(t, expectedValue, value, "Value mismatch for key %s after flush", string(key))
	}

	// Verify WAL was cleaned (optional but good)
	walInfo, err := os.Stat(filepath.Join(cfg.databaseDir, "WAL"))
	assert.NoError(t, err)
	// Check if WAL size is 0 or very small (metadata only) after clean
	// This threshold might need adjustment based on WAL implementation details
	assert.LessOrEqual(t, walInfo.Size(), int64(16), "WAL file should be empty or very small after flush and clean")

	// Close the database after test
	assert.NoError(t, rin.Close())
}

// TestRindb_Close tests the Close operation of Rindb.
func TestRindb_Close(t *testing.T) {
	rin, err := InitRinDB(testOptions()...)
	assert.NoError(t, err)

	// Add some data to ensure WAL and potentially SSTables are involved
	err = rin.Put(Bytes("key1"), Bytes("value1"))
	assert.NoError(t, err)

	// Close the database
	err = rin.Close()
	assert.NoError(t, err)

	// Verify WAL file is closed (attempting to use it should fail or indicate closed state)
	// FileSystem.IsOpened() can be used if WAL exposes its FileSystem
	assert.False(t, rin.wal.IsOpened(), "WAL file system should be closed")

	// Verify SSTableManager resources are closed (e.g., check IsOpened on managed FS)
	// SSTableManager.Close iterates and closes, we assume it works internally.
	// A more robust test could involve mocking or checking file handles if possible.
	// For now, we rely on the Close method being called and assume it functions correctly.
	// If SSTableManager held references to open files, we'd check those.
	// Since SSTableManager.Close() logs closures, we trust it for now.

	// Verify operations fail after close with the correct error
	_, err = rin.Get(Bytes("key1"))
	assert.ErrorIs(t, err, ErrDatabaseClosed, "Get should fail with ErrDatabaseClosed after Close")

	err = rin.Put(Bytes("key2"), Bytes("value2"))
	assert.ErrorIs(t, err, ErrDatabaseClosed, "Put should fail with ErrDatabaseClosed after Close")

	err = rin.Remove(Bytes("key1"))
	assert.ErrorIs(t, err, ErrDatabaseClosed, "Remove should fail with ErrDatabaseClosed after Close")
}
