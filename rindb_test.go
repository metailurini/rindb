package rindb

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestNoDeadlockConcurrentPutAndCompaction(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, WithDatabaseDir(t.TempDir()), WithLevel0CompactionThreshold(1), WithMaxMemtableSize(20))
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, rin.SSTableManager.Compact(ctx))
	}()

	for i := 0; i < 20; i++ {
		key := Bytes(fmt.Sprintf("k%d", i))
		require.NoError(t, rin.Put(ctx, key, Bytes("v")))
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock detected")
	}
}

// TestRindb_Get tests the Get operation of Rindb.
func TestRindb_Get(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, testOptions()...)
	defer cleanup()
	key := Bytes("key")
	err := rin.Put(ctx, key, Bytes("value"))
	assert.NoError(t, err)
	value, err := rin.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("value"), value)
}

func TestRindb_GetSequence(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, WithDatabaseDir(t.TempDir()))
	defer cleanup()
	key := Bytes("key")
	assert.NoError(t, rin.Put(ctx, key, Bytes("v1")))
	assert.NoError(t, rin.Put(ctx, key, Bytes("v2")))
	value, err := rin.Get(ctx, key, 1)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v1"), value)
	value, err = rin.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v2"), value)
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
	sst1, meta1, err := flush(ctx, cfg, mem1, ts.newSSTableFS())
	assert.NoError(t, err)
	require.NotZero(t, meta1.Number)
	ts.AddSSTable(0, &sst1)

	// Create SSTable with tombstone and another record
	mem2 := InitMemtable(cfg)
	mem2.Put(newRecord(Bytes("k"), nil, 3)) // tombstone
	mem2.Put(newRecord(Bytes("z"), Bytes("sstZ"), 4))
	sst2, meta2, err := flush(ctx, cfg, mem2, ts.newSSTableFS())
	assert.NoError(t, err)
	require.NotZero(t, meta2.Number)
	ts.AddSSTable(0, &sst2)

	// Memtable with latest updates
	mem := ts.RinDB.Memtable
	mem.Put(newRecord(Bytes("a"), Bytes("memA"), 5))
	mem.Put(newRecord(Bytes("b"), nil, 6)) // delete b
	mem.Put(newRecord(Bytes("k"), Bytes("memK"), 7))

	recA := newRecord(Bytes("a"), Bytes("memA"), 5)
	recAOld := newRecord(Bytes("a"), Bytes("sstA"), 1)
	recB := newRecord(Bytes("b"), Bytes("sstB"), 2)
	recK := newRecord(Bytes("k"), Bytes("memK"), 7)
	recZ := newRecord(Bytes("z"), Bytes("sstZ"), 4)

	tests := []struct {
		name     string
		start    Bytes
		end      Bytes
		seq      uint64
		expected []Record
	}{
		{
			name:     "full range",
			start:    Bytes("a"),
			end:      Bytes("z"),
			seq:      0,
			expected: []Record{recA, recK, recZ},
		},
		{
			name:     "range excludes deleted key",
			start:    Bytes("a"),
			end:      Bytes("b"),
			seq:      0,
			expected: []Record{recA},
		},
		{
			name:     "range after deletion",
			start:    Bytes("b"),
			end:      Bytes("z"),
			seq:      0,
			expected: []Record{recK, recZ},
		},
		{
			name:     "middle range",
			start:    Bytes("c"),
			end:      Bytes("y"),
			seq:      0,
			expected: []Record{recK},
		},
		{
			name:     "single key",
			start:    Bytes("a"),
			end:      Bytes("a"),
			seq:      0,
			expected: []Record{recA},
		},
		{
			name:     "tombstoned key only",
			start:    Bytes("b"),
			end:      Bytes("b"),
			seq:      0,
			expected: nil,
		},
		{
			name:     "range before first key",
			start:    Bytes("0"),
			end:      Bytes("a0"),
			seq:      0,
			expected: []Record{recA},
		},
		{
			name:     "range with no records",
			start:    Bytes("m"),
			end:      Bytes("n"),
			seq:      0,
			expected: nil,
		},
		{
			name:     "start greater than end",
			start:    Bytes("z"),
			end:      Bytes("a"),
			seq:      0,
			expected: nil,
		},
		{
			name:     "snapshot before memtable",
			start:    Bytes("a"),
			end:      Bytes("z"),
			seq:      4,
			expected: []Record{recAOld, recB, recZ},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				iter *RangeIterator
				err  error
			)
			if tt.seq == 0 {
				iter, err = ts.RinDB.IRange(ctx, tt.start, tt.end)
			} else {
				iter, err = ts.RinDB.IRange(ctx, tt.start, tt.end, tt.seq)
			}
			assert.NoError(t, err)
			assertIteratorRecords(t, iter, tt.expected)
		})
	}
}

func TestRindb_IRangeReverse(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	ts := newTestRindbSetup(t, ctx, &cfg)
	defer ts.Cleanup()

	// Create SSTable with older records
	mem1 := InitMemtable(cfg)
	mem1.Put(newRecord(Bytes("a"), Bytes("sstA"), 1))
	mem1.Put(newRecord(Bytes("b"), Bytes("sstB"), 2))
	sst1, meta1, err := flush(ctx, cfg, mem1, ts.newSSTableFS())
	assert.NoError(t, err)
	require.NotZero(t, meta1.Number)
	ts.AddSSTable(0, &sst1)

	// Memtable with latest update
	mem := ts.RinDB.Memtable
	mem.Put(newRecord(Bytes("c"), Bytes("memC"), 3))

	iter, err := ts.RinDB.IRangeReverse(ctx, Bytes("a"), Bytes("c"))
	assert.NoError(t, err)
	defer iter.Close()

	expected := []struct{ k, v string }{
		{"c", "memC"},
		{"b", "sstB"},
		{"a", "sstA"},
	}

	for _, exp := range expected {
		assert.True(t, iter.HasNext())
		rec, err := iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, exp.k, string(rec.GetKey()))
		assert.Equal(t, exp.v, string(rec.GetValue()))
	}

	assert.False(t, iter.HasNext())
	_, err = iter.Next()
	assert.ErrorIs(t, err, EOI)
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
	newSSTableFS, err := rin.SSTableManager.newSSTableFS(ctx)
	assert.NoError(t, err)
	defer func() { _ = newSSTableFS.Close() }() // Ensure the FS used for flushing is closed
	newSStable, _, err := flush(ctx, rin.config, rin.Memtable, newSSTableFS)
	assert.NoError(t, err)
	err = rin.WAL.Clean(ctx, math.MaxUint64)
	assert.NoError(t, err)
	value, err := newSStable.GetValue(ctx, Bytes("rm-key"))
	assert.ErrorIs(t, err, ErrTombstoneFound)
	assert.Nil(t, value)
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
	fs, err := rin.SSTableManager.newSSTableFS(ctx)
	assert.NoError(t, err)
	defer func() {
		if fs.IsOpened() {
			assert.NoError(t, fs.Close())
		}
	}()
	sst := createSSTable(t, rin.config, fs, [2]Bytes{Bytes("k1"), Bytes("v1-sst")})
	num, err := fileNum(fs.Path())
	assert.NoError(t, err)
	info, err := os.Stat(fs.Path())
	assert.NoError(t, err)
	small, large := sst.GetKeyRange()
	seqHi, err := sst.MaxSequenceNumber()
	assert.NoError(t, err)
	meta := fileMeta{Number: num, Level: 0, Smallest: InternalKey{UserKey: small}, Largest: InternalKey{UserKey: large}, Size: uint64(info.Size()), SeqHi: seqHi}
	assert.NoError(t, rin.SSTableManager.addSSTable(ctx, meta, seqHi))
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

// TestConcurrentGetPut ensures concurrent Get and Put operations do not panic.
func TestConcurrentGetPut(t *testing.T) {
	ctx := context.Background()
	opts := append(testOptions(), WithMaxMemtableSize(1<<20))
	rin, cleanup := initRinDBWithCleanup(t, opts...)
	defer cleanup()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			key := Bytes(fmt.Sprintf("k%d", i))
			require.NoError(t, rin.Put(ctx, key, Bytes("v")))
		}(i)
		go func(i int) {
			defer wg.Done()
			key := Bytes(fmt.Sprintf("k%d", i))
			_, _ = rin.Get(ctx, key)
		}(i)
	}
	wg.Wait()
}

// TestRindb_Put_FlushMemtableOnSizeLimit tests that the memtable is flushed
// when its estimated byte size exceeds the configured limit during a Put operation.
func TestRindb_Put_FlushMemtableOnSizeLimit(t *testing.T) {
	ctx := context.Background()

	// Each memtable entry adds len(key) + len(value) bytes plus metadata
	// from the skiplist node and internal key suffix.
	entryOverhead := slNodeOverhead + internalKeySuffixLen
	key1, val1 := Bytes("key1"), Bytes("value1-loooooooooong")
	key2, val2 := Bytes("key2"), Bytes("value2-loooooooooong")
	key3, val3 := Bytes("key3"), Bytes("value3-loooooooooong")
	entrySize := len(key1) + len(val1) + entryOverhead

	// Configure a maxMemtableSize slightly above two entries so the third
	// Put exceeds the limit and triggers a flush.
	maxMemtableSize := uint(entrySize*2 + 1)
	smallMemtableOpts := []Option{WithDatabaseDir(t.TempDir()), WithMaxMemtableSize(maxMemtableSize)}
	rin, cleanup := initRinDBWithCleanup(t, smallMemtableOpts...)
	defer cleanup()

	// Add data that will exceed the small memtable size limit
	err := rin.Put(ctx, key1, val1)
	assert.NoError(t, err)
	err = rin.Put(ctx, key2, val2)
	assert.NoError(t, err)

	// Check size before the Put that should trigger the flush
	sizeBeforeFlush := rin.Memtable.ByteSize()
	assert.LessOrEqual(t, uint(sizeBeforeFlush), rin.config.maxMemtableSize, "Size should be below threshold before triggering put")

	// This Put should trigger the flush
	err = rin.Put(ctx, key3, val3)
	assert.NoError(t, err)

	// Assertions after the flush should have occurred
	// 1. Memtable should be cleared (check estimated size)
	assert.Zero(t, rin.Memtable.ByteSize(), "Memtable estimated size should be zero after flush")

	// 2. At least one SSTable should have been created on disk.
	//    Compaction may move flushed SSTables to higher levels, so we count across all levels
	//    instead of asserting on a specific level.
	//    versionSet modifications are guarded by rin.mu, so use the same lock here.
	rin.mu.RLock()
	total := 0
	for _, lvl := range rin.versionSet.Levels {
		total += len(lvl)
	}
	rin.mu.RUnlock()
	assert.GreaterOrEqual(t, total, 1, "There should be at least one SSTable after flush")

	// 3. Verify data exists and is retrievable (implicitly checks SSTable content)
	// We can Get the keys back to ensure they were persisted correctly
	got1, err := rin.Get(ctx, key1)
	assert.NoError(t, err)
	assert.Equal(t, val1, got1)

	got3, err := rin.Get(ctx, key3)
	assert.NoError(t, err)
	assert.Equal(t, val3, got3)
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
		wp := path.Join(databaseDir, walPath(1))
		_ = os.RemoveAll(wp) // delete WAL directory if it exists
		fs, err := OpenFS(ctx, wp)
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
		fs, err := rin.SSTableManager.newSSTableFS(ctx)
		assert.NoError(t, err)
		defer func() { _ = fs.Close() }()

		// Create an SSTable with records, ensuring a high sequence number
		mem := InitMemtable(rin.config)
		mem.Put(newRecord(Bytes("sk1"), Bytes("sv1"), 50))
		mem.Put(newRecord(Bytes("sk2"), Bytes("sv2"), 60))
		_, meta, err := flush(ctx, rin.config, mem, fs)
		assert.NoError(t, err)
		assert.NoError(t, rin.SSTableManager.addSSTable(ctx, meta, meta.SeqHi))
		assert.NoError(t, fs.Close())

		// Also create a WAL with a lower sequence number to ensure SSTable takes precedence
		wp := path.Join(rin.config.databaseDir, walPath(1))
		walFs, err := OpenFS(ctx, wp)
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
		fs, err := rin.SSTableManager.newSSTableFS(ctx)
		assert.NoError(t, err)
		defer func() { _ = fs.Close() }()

		mem := InitMemtable(rin.config)
		mem.Put(newRecord(Bytes("sk1"), Bytes("sv1"), 70))
		_, meta, err := flush(ctx, rin.config, mem, fs)
		assert.NoError(t, err)
		assert.NoError(t, rin.SSTableManager.addSSTable(ctx, meta, meta.SeqHi))
		assert.NoError(t, fs.Close())

		// Create a WAL with the same highest sequence number
		// The database directory is already created by initRinDBWithCleanup
		wp := path.Join(rin.config.databaseDir, walPath(1))
		walFs, err := OpenFS(ctx, wp)
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
