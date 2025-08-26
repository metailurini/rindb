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
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, testOptions()...)
	defer cleanup()
	key := Bytes("key")
	err := rin.Put(ctx, key, Bytes("value"))
	assert.NoError(t, err)
	err = rin.Put(ctx, key, Bytes("value2"))
	assert.NoError(t, err)

	value, err := rin.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value2"), value)

	value, err = rin.Get(ctx, key, 1)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value"), value)
}

func TestRindb_IRange(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	ts := newTestRindbSetup(t, ctx, &cfg)
	defer ts.Cleanup()

	// Create SSTable with older records
	mem1 := InitMemtable(cfg)
	mem1.Put(newRecord(Bytes("a"), Bytes("sstA"), 1))
	mem1.Put(newRecord(Bytes("b"), Bytes("sstB"), 2))
	sst1, err := flush(ctx, cfg, mem1, ts.newSSTableFS(0))
	assert.NoError(t, err)
	ts.AddSSTableToLevel(0, &sst1)

	// Create SSTable with tombstone and another record
	mem2 := InitMemtable(cfg)
	mem2.Put(newRecord(Bytes("k"), Bytes(""), 3)) // tombstone
	mem2.Put(newRecord(Bytes("z"), Bytes("sstZ"), 4))
	sst2, err := flush(ctx, cfg, mem2, ts.newSSTableFS(0))
	assert.NoError(t, err)
	ts.AddSSTableToLevel(0, &sst2)

	// Memtable with latest updates
	mem := ts.RinDB.memtable
	mem.Put(newRecord(Bytes("a"), Bytes("memA"), 5))
	mem.Put(newRecord(Bytes("b"), nil, 6)) // delete b
	mem.Put(newRecord(Bytes("k"), Bytes("memK"), 7))

	recA := newRecord(Bytes("a"), Bytes("memA"), 5)
	recK := newRecord(Bytes("k"), Bytes("memK"), 7)
	recZ := newRecord(Bytes("z"), Bytes("sstZ"), 4)

	tests := []struct {
		name     string
		start    Bytes
		end      Bytes
		expected []Record
	}{
		{
			name:     "full range",
			start:    Bytes("a"),
			end:      Bytes("z"),
			expected: []Record{recA, recK, recZ},
		},
		{
			name:     "range excludes deleted key",
			start:    Bytes("a"),
			end:      Bytes("b"),
			expected: []Record{recA},
		},
		{
			name:     "range after deletion",
			start:    Bytes("b"),
			end:      Bytes("z"),
			expected: []Record{recK, recZ},
		},
		{
			name:     "middle range",
			start:    Bytes("c"),
			end:      Bytes("y"),
			expected: []Record{recK},
		},
		{
			name:     "single key",
			start:    Bytes("a"),
			end:      Bytes("a"),
			expected: []Record{recA},
		},
		{
			name:     "tombstoned key only",
			start:    Bytes("b"),
			end:      Bytes("b"),
			expected: nil,
		},
		{
			name:     "range before first key",
			start:    Bytes("0"),
			end:      Bytes("a0"),
			expected: []Record{recA},
		},
		{
			name:     "range with no records",
			start:    Bytes("m"),
			end:      Bytes("n"),
			expected: nil,
		},
		{
			name:     "start greater than end",
			start:    Bytes("z"),
			end:      Bytes("a"),
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter, err := ts.RinDB.IRange(ctx, tt.start, tt.end)
			assert.NoError(t, err)
			assertIteratorRecords(t, iter, tt.expected)
		})
	}
}

// TestRindb_Remove tests the Remove operation of Rindb.
func TestRindb_Remove(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, testOptions()...)
	defer cleanup()
	key := Bytes("rm-key")
	err := rin.Put(ctx, key, Bytes("value"))
	assert.NoError(t, err)
	err = rin.Remove(ctx, key)
	assert.NoError(t, err)
	value, err := rin.Get(ctx, key)
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Nil(t, value)
}

// TestRindb_FlushMemtable tests flushing the memtable to an SSTable.
func TestRindb_FlushMemtable(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, testOptions()...)
	defer cleanup()
	err := rin.Put(ctx, Bytes("key"), Bytes("value"))
	assert.NoError(t, err)
	err = rin.Put(ctx, Bytes("rm-key"), Bytes("value"))
	assert.NoError(t, err)
	err = rin.Remove(ctx, Bytes("rm-key"))
	assert.NoError(t, err)
	// SSTableManager is now part of rin, no need to init separately
	// We still need a new FS for the flush operation itself
	newSSTableFS, err := rin.ssTableManager.NewSSTableFS(ctx, 0)
	assert.NoError(t, err)
	defer func() { _ = newSSTableFS.Close() }() // Ensure the FS used for flushing is closed
	newSStable, err := flush(ctx, rin.config, rin.memtable, newSSTableFS)
	assert.NoError(t, err)
	err = rin.wal.Clean()
	assert.NoError(t, err)
	value, err := newSStable.GetValue(ctx, Bytes("rm-key"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes(nil), value)
	value, err = newSStable.GetValue(ctx, Bytes("key"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value"), value)
}

// TestRindb_GetPrecedence tests that Get prioritizes Memtable over SSTables.
func TestRindb_GetPrecedence(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, testOptions()...)
	defer cleanup()
	// SSTableManager is now part of rin
	fs, err := rin.ssTableManager.NewSSTableFS(ctx, 0) // Create FS for the initial SSTable
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
	rin.ssTableManager.levels[0].PushBack(fs)        // Add the newly created SSTable FS to the manager
	err = rin.Put(ctx, Bytes("k1"), Bytes("v1-mem")) // Put the value into the memtable
	assert.NoError(t, err)
	v, err := rin.Get(ctx, Bytes("k1"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v1-mem"), v)
}

// TestRindb_ConcurrentCRUD tests concurrent CRUD operations on Rindb.
func TestRindb_ConcurrentCRUD(t *testing.T) {
	ctx := context.Background()
	opts := append(testOptions(), WithMaxMemtableSize(10000))
	rin, cleanup := initRinDBWithCleanup(t, opts...)
	defer cleanup()
	var wg sync.WaitGroup
	const numGoroutines = 10
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			key := Bytes(fmt.Sprintf("k%d", i))
			value := Bytes(fmt.Sprintf("v%d", i))
			assert.NoError(t, rin.Put(ctx, key, value))
			v, err := rin.Get(ctx, key)
			assert.NoError(t, err)

			assert.Equal(t, value, v)
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			value = Bytes(fmt.Sprintf("v%d-updated", i))
			assert.NoError(t, rin.Put(ctx, key, value))

			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			v, err = rin.Get(ctx, key)
			assert.NoError(t, err)
			assert.Equal(t, value, v)
			assert.NoError(t, rin.Remove(ctx, key))
		}(i)
	}
	wg.Wait()
}

// TestRindb_Close tests the Close operation of Rindb using the helper.
func TestRindb_Close(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, testOptions()...)
	err := rin.Put(ctx, Bytes("key1"), Bytes("value1"))
	assert.NoError(t, err)

	// Explicitly call cleanup to test the closing part
	cleanup() // This calls rin.Close() and asserts no error

	// Re-check the closed state (assuming rin instance is still valid memory-wise)
	assert.True(t, rin.closed, "Rindb instance should be marked as closed")

	// Verify operations fail after close
	_, getErr := rin.Get(ctx, Bytes("key1"))
	assert.ErrorIs(t, getErr, ErrDatabaseClosed, "Get should fail with ErrDatabaseClosed after Close")

	putErr := rin.Put(ctx, Bytes("key2"), Bytes("value2"))
	assert.ErrorIs(t, putErr, ErrDatabaseClosed, "Put should fail with ErrDatabaseClosed after Close")

	removeErr := rin.Remove(ctx, Bytes("key1"))
	assert.ErrorIs(t, removeErr, ErrDatabaseClosed, "Remove should fail with ErrDatabaseClosed after Close")

	// We don't need the manual checks for WAL/SSTable closure here anymore,
	// as the helper's cleanup function handles rin.Close(), which should manage them.
	// The assertions within cleanup cover the success of rin.Close().
}

// TestRindb_Put_FlushMemtableOnSizeLimit tests that the memtable is flushed
// when its estimated byte size exceeds the configured limit during a Put operation.
func TestRindb_Put_FlushMemtableOnSizeLimit(t *testing.T) {
	ctx := context.Background()
	// Configure a small maxMemtableSize (in bytes) to trigger the flush easily.
	// The estimated size is calculated as len(key) + len(value) + 16 bytes overhead per entry.
	// key1 ("key1", 4 bytes) + value1 ("value1-loooooooooong", 20 bytes) + 16 = 40 bytes
	// key2 ("key2", 4 bytes) + value2 ("value2-loooooooooong", 20 bytes) + 16 = 40 bytes
	// Total estimated size after key1 and key2 = 40 + 40 = 80 bytes.
	// key3 ("key3", 4 bytes) + value3 ("value3-loooooooooong", 20 bytes) + 16 = 40 bytes
	// Total estimated size after key3 = 80 + 40 = 120 bytes.
	// Set maxMemtableSize to 80. The memtable will reach its limit after key2 is added.
	// The Put operation for key3 should then trigger the flush.
	smallMemtableOpts := []Option{WithDatabaseDir(t.TempDir()), WithMaxMemtableSize(80)}
	rin, cleanup := initRinDBWithCleanup(t, smallMemtableOpts...)
	defer cleanup()

	// Add data that will exceed the small memtable size limit
	err := rin.Put(ctx, Bytes("key1"), Bytes("value1-loooooooooong"))
	assert.NoError(t, err)
	err = rin.Put(ctx, Bytes("key2"), Bytes("value2-loooooooooong"))
	assert.NoError(t, err)

	// Check size before the Put that should trigger the flush
	sizeBeforeFlush := rin.memtable.ByteSize()
	assert.LessOrEqual(t, uint(sizeBeforeFlush), rin.config.maxMemtableSize, "Size should be below threshold before triggering put")

	// This Put should trigger the flush
	err = rin.Put(ctx, Bytes("key3"), Bytes("value3-loooooooooong"))
	assert.NoError(t, err)

	// Assertions after the flush should have occurred
	// 1. Memtable should be cleared (check estimated size)
	assert.Zero(t, rin.memtable.ByteSize(), "Memtable estimated size should be zero after flush")

	// 2. At least one SSTable should have been created on disk.
	//    Compaction may move flushed SSTables to higher levels, so we count across all levels
	//    instead of asserting on a specific level.
	rin.ssTableManager.mu.RLock() // Lock needed to safely access levels
	total := 0
	for _, lvl := range rin.ssTableManager.levels {
		if lvl != nil {
			total += lvl.Len()
		}
	}
	rin.ssTableManager.mu.RUnlock()
	assert.GreaterOrEqual(t, total, 1, "There should be at least one SSTable after flush")

	// 3. Verify data exists and is retrievable (implicitly checks SSTable content)
	// We can Get the keys back to ensure they were persisted correctly
	val1, err := rin.Get(ctx, Bytes("key1"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value1-loooooooooong"), val1)

	val2, err := rin.Get(ctx, Bytes("key2"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value2-loooooooooong"), val2)

	val3, err := rin.Get(ctx, Bytes("key3"))
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

		ctx := context.Background()
		// Manually create WAL and add records to simulate memtable content
		walPath := path.Join(databaseDir, "WAL")
		_ = os.RemoveAll(walPath) // delete WAL directory if it exists
		fs, err := OpenFS(ctx, walPath)
		assert.NoError(t, err)
		wal := NewWAL(DefaultConfig(), fs)
		defer func() { _ = wal.Close() }()

		// Add records with sequence numbers
		assert.NoError(t, wal.Append(ctx, newRecord(Bytes("k1"), Bytes("v1"), 10)))
		assert.NoError(t, wal.Append(ctx, newRecord(Bytes("k2"), Bytes("v2"), 20)))
		assert.NoError(t, wal.Append(ctx, newRecord(Bytes("k3"), Bytes("v3"), 30)))

		opts := testOptions()
		opts = append(opts, WithDatabaseDir(databaseDir))
		rin, cleanup := initRinDBWithCleanup(t, opts...)
		defer cleanup()

		assert.Equal(t, uint64(30), rin.sequenceNumber, "Expected sequence number from memtable")
	})

	t.Run("SSTableHasHighestSequence", func(t *testing.T) {
		databaseDir := t.TempDir()
		defer func() { _ = os.RemoveAll(databaseDir) }()

		ctx := context.Background()
		opts := testOptions()
		opts = append(opts, WithDatabaseDir(databaseDir))
		defer func() { _ = os.RemoveAll(databaseDir) }()
		rin, cleanup := initRinDBWithCleanup(t, opts...)
		defer cleanup()

		// Manually create an SSTable with a high sequence number
		// Use rin.ssTableManager directly
		fs, err := rin.ssTableManager.NewSSTableFS(ctx, 0)
		assert.NoError(t, err)
		defer func() { _ = fs.Close() }()

		// Create an SSTable with records, ensuring a high sequence number
		mem := InitMemtable(rin.config)
		mem.Put(newRecord(Bytes("sk1"), Bytes("sv1"), 50))
		mem.Put(newRecord(Bytes("sk2"), Bytes("sv2"), 60))
		_, err = flush(ctx, rin.config, mem, fs)
		assert.NoError(t, err)
		assert.NoError(t, rin.ssTableManager.AddSSTable(ctx, 0, fs))

		// Also create a WAL with a lower sequence number to ensure SSTable takes precedence
		walPath := path.Join(rin.config.databaseDir, "WAL")
		walFs, err := OpenFS(ctx, walPath)
		assert.NoError(t, err)
		wal := NewWAL(rin.config, walFs)
		defer func() { _ = wal.Close() }()
		assert.NoError(t, wal.Append(ctx, newRecord(Bytes("wk1"), Bytes("wv1"), 5)))

		assert.NoError(t, rin.Close())
		rin, cleanup = initRinDBWithCleanup(t, opts...)
		defer cleanup()

		assert.Equal(t, uint64(60), rin.sequenceNumber, "Expected sequence number from SSTable")
	})

	t.Run("EqualMaxSequenceNumbers", func(t *testing.T) {
		databaseDir := t.TempDir()
		defer func() { _ = os.RemoveAll(databaseDir) }()

		ctx := context.Background()
		opts := testOptions()
		opts = append(opts, WithDatabaseDir(databaseDir))
		defer func() { _ = os.RemoveAll(databaseDir) }()
		rin, cleanup := initRinDBWithCleanup(t, opts...)
		defer cleanup()

		// Manually create an SSTable with a high sequence number
		// Use rin.ssTableManager directly
		fs, err := rin.ssTableManager.NewSSTableFS(ctx, 0)
		assert.NoError(t, err)
		defer func() { _ = fs.Close() }()

		mem := InitMemtable(rin.config)
		mem.Put(newRecord(Bytes("sk1"), Bytes("sv1"), 70))
		_, err = flush(ctx, rin.config, mem, fs)
		assert.NoError(t, err)
		assert.NoError(t, rin.ssTableManager.AddSSTable(ctx, 0, fs))

		// Create a WAL with the same highest sequence number
		// The database directory is already created by initRinDBWithCleanup
		walPath := path.Join(rin.config.databaseDir, "WAL")
		walFs, err := OpenFS(ctx, walPath)
		assert.NoError(t, err)
		wal := NewWAL(rin.config, walFs)
		defer func() { _ = wal.Close() }()
		assert.NoError(t, wal.Append(ctx, newRecord(Bytes("wk1"), Bytes("wv1"), 70)))

		assert.NoError(t, rin.Close())
		rin, cleanup = initRinDBWithCleanup(t, opts...)
		defer cleanup()

		assert.Equal(t, uint64(70), rin.sequenceNumber, "Expected sequence number to be the common max")
	})
}
