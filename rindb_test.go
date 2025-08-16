package rindb

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testOptions() []Option {
	return []Option{
		WithDatabaseDir("testdata"),
	}
}

func testConfig() Config {
	return NewConfig(testOptions()...)
}

// TestRindb_Init tests the initialization of the Rindb database using the helper.
func TestRindb_Init(t *testing.T) {
	// Use the helper, which includes initialization and cleanup
	_, cleanup := initRinDBWithCleanup(t, testOptions()...)
	defer cleanup()
	// The assertion is implicitly handled by initRinDBWithCleanup
}

// TestRindb_Put tests the Put operation of Rindb.
func TestRindb_Put(t *testing.T) {
	rin, cleanup := initRinDBWithCleanup(t, testOptions()...)
	defer cleanup()
	t.Run("BasicPut", func(t *testing.T) {
		err := rin.Put(context.Background(), Bytes("key"), Bytes("value"))
		assert.NoError(t, err)
	})
	t.Run("Tombstone", func(t *testing.T) {
		err := rin.Put(context.Background(), Bytes("rm-key"), nil)
		assert.NoError(t, err)
	})
}

// TestRindb_Get tests the Get operation of Rindb.
func TestRindb_Get(t *testing.T) {
	rin, cleanup := initRinDBWithCleanup(t, testOptions()...)
	defer cleanup()
	key := Bytes("key")
	err := rin.Put(context.Background(), key, Bytes("value"))
	assert.NoError(t, err)
	value, err := rin.Get(context.Background(), key)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value"), value)
}

// TestRindb_Remove tests the Remove operation of Rindb.
func TestRindb_Remove(t *testing.T) {
	rin, cleanup := initRinDBWithCleanup(t, testOptions()...)
	defer cleanup()
	key := Bytes("rm-key")
	err := rin.Put(context.Background(), key, Bytes("value"))
	assert.NoError(t, err)
	err = rin.Remove(context.Background(), key)
	assert.NoError(t, err)
	value, err := rin.Get(context.Background(), key)
	assert.NoError(t, err)
	assert.Equal(t, Bytes(nil), value)
}

// TestRindb_FlushMemtable tests flushing the memtable to an SSTable.
func TestRindb_FlushMemtable(t *testing.T) {
	rin, cleanup := initRinDBWithCleanup(t, testOptions()...)
	defer cleanup()
	err := rin.Put(context.Background(), Bytes("key"), Bytes("value"))
	assert.NoError(t, err)
	err = rin.Put(context.Background(), Bytes("rm-key"), Bytes("value"))
	assert.NoError(t, err)
	err = rin.Remove(context.Background(), Bytes("rm-key"))
	assert.NoError(t, err)
	// SSTableManager is now part of rin, no need to init separately
	// We still need a new FS for the flush operation itself
	newSSTableFS, err := rin.ssTableManager.NewSSTableFS(context.Background(), 0)
	assert.NoError(t, err)
	defer func() { _ = newSSTableFS.Close() }() // Ensure the FS used for flushing is closed
	newSStable, err := Flush(context.Background(), rin.config, rin.memtable, newSSTableFS)
	assert.NoError(t, err)
	err = rin.wal.Clean()
	assert.NoError(t, err)
	value, err := newSStable.GetValue(context.Background(), Bytes("rm-key"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes(nil), value)
	value, err = newSStable.GetValue(context.Background(), Bytes("key"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value"), value)
}

// TestRindb_GetPrecedence tests that Get prioritizes Memtable over SSTables.
func TestRindb_GetPrecedence(t *testing.T) {
	rin, cleanup := initRinDBWithCleanup(t, testOptions()...)
	defer cleanup()
	// SSTableManager is now part of rin
	fs, err := rin.ssTableManager.NewSSTableFS(context.Background(), 0) // Create FS for the initial SSTable
	assert.NoError(t, err)
	// Defer close for the FS used in the test setup
	defer func() {
		if fs.IsOpened() {
			assert.NoError(t, fs.Close())
		}
	}()
	_ = createSSTable(t, rin.config, fs, [2]Bytes{Bytes("k1"), Bytes("v1-sst")})
	// Ensure level 0 exists before pushing back
	if len(rin.ssTableManager.levels) == 0 {
		rin.ssTableManager.levels = append(rin.ssTableManager.levels, InitLinkedList[*FileSystem]())
	} else if rin.ssTableManager.levels[0] == nil {
		rin.ssTableManager.levels[0] = InitLinkedList[*FileSystem]()
	}
	rin.ssTableManager.levels[0].PushBack(fs)                         // Add the newly created SSTable FS to the manager
	err = rin.Put(context.Background(), Bytes("k1"), Bytes("v1-mem")) // Put the value into the memtable
	assert.NoError(t, err)
	v, err := rin.Get(context.Background(), Bytes("k1"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v1-mem"), v)
}

// TestRindb_ConcurrentCRUD tests concurrent CRUD operations on Rindb.
func TestRindb_ConcurrentCRUD(t *testing.T) {
	rin, cleanup := initRinDBWithCleanup(t, testOptions()...)
	defer cleanup()
	var wg sync.WaitGroup
	const numGoroutines = 10
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			key := Bytes(fmt.Sprintf("k%d", i))
			value := Bytes(fmt.Sprintf("v%d", i))
			assert.NoError(t, rin.Put(context.Background(), key, value))
			v, err := rin.Get(context.Background(), key)
			assert.NoError(t, err)

			assert.Equal(t, value, v)
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			value = Bytes(fmt.Sprintf("v%d-updated", i))
			assert.NoError(t, rin.Put(context.Background(), key, value))

			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			v, err = rin.Get(context.Background(), key)
			assert.NoError(t, err)
			assert.Equal(t, value, v)
			assert.NoError(t, rin.Remove(context.Background(), key))
		}(i)
	}
	wg.Wait()
}

// TestRindb_Close tests the Close operation of Rindb using the helper.
func TestRindb_Close(t *testing.T) {
	rin, cleanup := initRinDBWithCleanup(t, testOptions()...)
	err := rin.Put(context.Background(), Bytes("key1"), Bytes("value1"))
	assert.NoError(t, err)

	// Explicitly call cleanup to test the closing part
	cleanup() // This calls rin.Close() and asserts no error

	// Re-check the closed state (assuming rin instance is still valid memory-wise)
	assert.True(t, rin.closed, "Rindb instance should be marked as closed")

	// Verify operations fail after close
	_, getErr := rin.Get(context.Background(), Bytes("key1"))
	assert.ErrorIs(t, getErr, ErrDatabaseClosed, "Get should fail with ErrDatabaseClosed after Close")

	putErr := rin.Put(context.Background(), Bytes("key2"), Bytes("value2"))
	assert.ErrorIs(t, putErr, ErrDatabaseClosed, "Put should fail with ErrDatabaseClosed after Close")

	removeErr := rin.Remove(context.Background(), Bytes("key1"))
	assert.ErrorIs(t, removeErr, ErrDatabaseClosed, "Remove should fail with ErrDatabaseClosed after Close")

	// We don't need the manual checks for WAL/SSTable closure here anymore,
	// as the helper's cleanup function handles rin.Close(), which should manage them.
	// The assertions within cleanup cover the success of rin.Close().
}

// TestRindb_Put_FlushMemtableOnSizeLimit tests that the memtable is flushed
// when its estimated byte size exceeds the configured limit during a Put operation.
func TestRindb_Put_FlushMemtableOnSizeLimit(t *testing.T) {
	// Configure a small maxMemtableSize (in bytes) to trigger the flush easily.
	// The estimated size is calculated as len(key) + len(value) + 16 bytes overhead per entry.
	// key1 ("key1", 4 bytes) + value1 ("value1-loooooooooong", 20 bytes) + 16 = 40 bytes
	// key2 ("key2", 4 bytes) + value2 ("value2-loooooooooong", 20 bytes) + 16 = 40 bytes
	// Total estimated size after key1 and key2 = 40 + 40 = 80 bytes.
	// key3 ("key3", 4 bytes) + value3 ("value3-loooooooooong", 20 bytes) + 16 = 40 bytes
	// Total estimated size after key3 = 80 + 40 = 120 bytes.
	// Set maxMemtableSize to 80. The memtable will reach its limit after key2 is added.
	// The Put operation for key3 should then trigger the flush.
	smallMemtableOpts := append(testOptions(), WithMaxMemtableSize(80))
	rin, cleanup := initRinDBWithCleanup(t, smallMemtableOpts...)
	defer cleanup()

	// Add data that will exceed the small memtable size limit
	err := rin.Put(context.Background(), Bytes("key1"), Bytes("value1-loooooooooong"))
	assert.NoError(t, err)
	err = rin.Put(context.Background(), Bytes("key2"), Bytes("value2-loooooooooong"))
	assert.NoError(t, err)

	// Check size before the Put that should trigger the flush
	sizeBeforeFlush := rin.memtable.ByteSize()
	assert.LessOrEqual(t, uint(sizeBeforeFlush), rin.config.maxMemtableSize, "Size should be below threshold before triggering put")

	// This Put should trigger the flush
	err = rin.Put(context.Background(), Bytes("key3"), Bytes("value3-loooooooooong"))
	assert.NoError(t, err)

	// Assertions after the flush should have occurred
	// 1. Memtable should be cleared (check estimated size)
	assert.Zero(t, rin.memtable.ByteSize(), "Memtable estimated size should be zero after flush")

	// 2. An L0 SSTable should have been created. Check if L0 exists and is not empty.
	//    Don't assert exact count=1, as compaction might run concurrently in a real scenario
	//    or if the test setup triggers it indirectly.
	rin.ssTableManager.mu.RLock() // Lock needed to safely access levels
	assert.True(t, len(rin.ssTableManager.levels) > 0 && rin.ssTableManager.levels[0] != nil, "Level 0 should exist after flush")
	assert.GreaterOrEqual(t, rin.ssTableManager.levels[0].Len(), 1, "Level 0 should contain at least one SSTable after flush")
	rin.ssTableManager.mu.RUnlock()

	// 3. Verify data exists and is retrievable (implicitly checks SSTable content)
	// We can Get the keys back to ensure they were persisted correctly
	val1, err := rin.Get(context.Background(), Bytes("key1"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value1-loooooooooong"), val1)

	val2, err := rin.Get(context.Background(), Bytes("key2"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value2-loooooooooong"), val2)

	val3, err := rin.Get(context.Background(), Bytes("key3"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value3-loooooooooong"), val3)
}

// TestInitRinDB_MaxSequenceNumber tests the sequence number initialization logic.
func TestInitRinDB_MaxSequenceNumber(t *testing.T) {
	t.Run("EmptyDatabase", func(t *testing.T) {
		databaseDir := t.TempDir()
		defer func() { _ = os.RemoveAll(databaseDir) }()

		opts := testOptions()
		opts = append(opts, WithDatabaseDir(databaseDir))
		rin, cleanup := initRinDBWithCleanup(t, opts...)
		defer cleanup()

		assert.Equal(t, rin.sequenceNumber, uint64(0), "Expected sequence number to be at least 1 for empty database")
	})

	t.Run("MemtableHasHighestSequence", func(t *testing.T) {
		databaseDir := t.TempDir()
		defer func() { _ = os.RemoveAll(databaseDir) }()

		// Manually create WAL and add records to simulate memtable content
		walPath := path.Join(databaseDir, "WAL")
		_ = os.RemoveAll(walPath) // delete WAL directory if it exists
		fs, err := OpenFS(context.Background(), walPath)
		assert.NoError(t, err)
		wal := NewWAL(DefaultConfig(), fs)
		defer func() { _ = wal.Close() }()

		// Add records with sequence numbers
		assert.NoError(t, wal.Append(context.Background(), NewRecord(Bytes("k1"), Bytes("v1"), 10)))
		assert.NoError(t, wal.Append(context.Background(), NewRecord(Bytes("k2"), Bytes("v2"), 20)))
		assert.NoError(t, wal.Append(context.Background(), NewRecord(Bytes("k3"), Bytes("v3"), 30)))

		opts := testOptions()
		opts = append(opts, WithDatabaseDir(databaseDir))
		rin, cleanup := initRinDBWithCleanup(t, opts...)
		defer cleanup()

		assert.Equal(t, uint64(30), rin.sequenceNumber, "Expected sequence number from memtable")
	})

	t.Run("SSTableHasHighestSequence", func(t *testing.T) {
		databaseDir := t.TempDir()
		defer func() { _ = os.RemoveAll(databaseDir) }()

		opts := testOptions()
		opts = append(opts, WithDatabaseDir(databaseDir))
		defer func() { _ = os.RemoveAll(databaseDir) }()
		rin, cleanup := initRinDBWithCleanup(t, opts...)
		defer cleanup()

		// Manually create an SSTable with a high sequence number
		// Use rin.ssTableManager directly
		fs, err := rin.ssTableManager.NewSSTableFS(context.Background(), 0)
		assert.NoError(t, err)
		defer func() { _ = fs.Close() }()

		// Create an SSTable with records, ensuring a high sequence number
		mem := InitMemtable(rin.config)
		mem.Put(NewRecord(Bytes("sk1"), Bytes("sv1"), 50))
		mem.Put(NewRecord(Bytes("sk2"), Bytes("sv2"), 60))
		_, err = Flush(context.Background(), rin.config, mem, fs)
		assert.NoError(t, err)
		assert.NoError(t, rin.ssTableManager.AddSSTable(context.Background(), 0, fs))

		// Also create a WAL with a lower sequence number to ensure SSTable takes precedence
		walPath := path.Join(rin.config.databaseDir, "WAL")
		walFs, err := OpenFS(context.Background(), walPath)
		assert.NoError(t, err)
		wal := NewWAL(rin.config, walFs)
		defer func() { _ = wal.Close() }()
		assert.NoError(t, wal.Append(context.Background(), NewRecord(Bytes("wk1"), Bytes("wv1"), 5)))

		assert.NoError(t, rin.Close(context.Background()))
		rin, cleanup = initRinDBWithCleanup(t, opts...)
		defer cleanup()

		assert.Equal(t, uint64(60), rin.sequenceNumber, "Expected sequence number from SSTable")
	})

	t.Run("EqualMaxSequenceNumbers", func(t *testing.T) {
		databaseDir := t.TempDir()
		defer func() { _ = os.RemoveAll(databaseDir) }()

		opts := testOptions()
		opts = append(opts, WithDatabaseDir(databaseDir))
		defer func() { _ = os.RemoveAll(databaseDir) }()
		rin, cleanup := initRinDBWithCleanup(t, opts...)
		defer cleanup()

		// Manually create an SSTable with a high sequence number
		// Use rin.ssTableManager directly
		fs, err := rin.ssTableManager.NewSSTableFS(context.Background(), 0)
		assert.NoError(t, err)
		defer func() { _ = fs.Close() }()

		mem := InitMemtable(rin.config)
		mem.Put(NewRecord(Bytes("sk1"), Bytes("sv1"), 70))
		_, err = Flush(context.Background(), rin.config, mem, fs)
		assert.NoError(t, err)
		assert.NoError(t, rin.ssTableManager.AddSSTable(context.Background(), 0, fs))

		// Create a WAL with the same highest sequence number
		// The database directory is already created by initRinDBWithCleanup
		walPath := path.Join(rin.config.databaseDir, "WAL")
		walFs, err := OpenFS(context.Background(), walPath)
		assert.NoError(t, err)
		wal := NewWAL(rin.config, walFs)
		defer func() { _ = wal.Close() }()
		assert.NoError(t, wal.Append(context.Background(), NewRecord(Bytes("wk1"), Bytes("wv1"), 70)))

		assert.NoError(t, rin.Close(context.Background()))
		rin, cleanup = initRinDBWithCleanup(t, opts...)
		defer cleanup()

		assert.Equal(t, uint64(70), rin.sequenceNumber, "Expected sequence number to be the common max")
	})
}
