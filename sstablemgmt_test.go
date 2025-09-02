package rindb

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSTableManager_SearchKeyPrevIteration(t *testing.T) {
	cfg := testConfig()
	ctx := context.Background()
	ts := newTestRindbSetup(t, ctx, &cfg)
	defer ts.Cleanup()

	// Create SSTables
	older := ts.createSSTable(0, map[string]string{"targetKey": "targetVal"})
	newer := ts.createSSTable(0, map[string]string{"otherKey": "otherVal"})

	// Add to level 0 (older first, then newer)
	ts.AddSSTableToLevel(0, older)
	ts.AddSSTableToLevel(0, newer)

	// Verify search finds the key in older SSTable
	result, err := ts.Manager.searchKey(ctx, Bytes("targetKey"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("targetVal"), result)
}

func TestInitSSTableManagerRepairMode(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := testConfig()
	cfg.databaseDir = dir

	// Create two valid SSTable files on disk.
	mem1 := InitMemtable(cfg)
	mem1.Put(newRecord(Bytes("a"), Bytes("1"), 1))
	fs1, err := OpenFS(ctx, path.Join(dir, sstPath(1)))
	require.NoError(t, err)
	_, _, err = flush(ctx, cfg, mem1, fs1)
	require.NoError(t, err)
	require.NoError(t, fs1.Close())

	mem2 := InitMemtable(cfg)
	mem2.Put(newRecord(Bytes("b"), Bytes("2"), 1))
	fs2, err := OpenFS(ctx, path.Join(dir, sstPath(2)))
	require.NoError(t, err)
	_, _, err = flush(ctx, cfg, mem2, fs2)
	require.NoError(t, err)
	require.NoError(t, fs2.Close())

	vs := &VersionSet{Levels: [][]FileMeta{{{Number: 1, Level: 0}}}}

	t.Run("normal startup uses manifest", func(t *testing.T) {
		sm, err := InitSSTableManager(ctx, cfg, vs, nil)
		assert.NoError(t, err)
		defer sm.Close(ctx)

		if assert.Len(t, sm.levels, 1) {
			iter := sm.levels[0].Iterator()
			var names []string
			for iter.HasNext() {
				fs, err := iter.Next()
				assert.NoError(t, err)
				names = append(names, path.Base(fs.Path()))
			}
			assert.Equal(t, []string{sstPath(1)}, names)
		}
	})

	t.Run("repair mode scans directory", func(t *testing.T) {
		cfg.repairMode = true
		sm, err := InitSSTableManager(ctx, cfg, nil, nil)
		assert.NoError(t, err)
		defer sm.Close(ctx)

		if assert.Len(t, sm.levels, 1) {
			iter := sm.levels[0].Iterator()
			var names []string
			for iter.HasNext() {
				fs, err := iter.Next()
				assert.NoError(t, err)
				names = append(names, path.Base(fs.Path()))
			}
			assert.Equal(t, []string{sstPath(1), sstPath(2)}, names)
		}

		if assert.Len(t, sm.versionSet.Levels, 1) {
			var nums []uint64
			for _, fm := range sm.versionSet.Levels[0] {
				nums = append(nums, fm.Number)
			}
			assert.ElementsMatch(t, []uint64{1, 2}, nums)
		}
	})
}

func TestBuildVersionSetFromDisk(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()

	t.Run("collects metadata from sstable files", func(t *testing.T) {
		dir := t.TempDir()
		cfg.databaseDir = dir

		// Create two valid SSTables.
		mem1 := InitMemtable(cfg)
		mem1.Put(newRecord(Bytes("a"), Bytes("1"), 1))
		fs1, err := OpenFS(ctx, path.Join(dir, sstPath(1)))
		require.NoError(t, err)
		_, _, err = flush(ctx, cfg, mem1, fs1)
		require.NoError(t, err)
		require.NoError(t, fs1.Close())

		mem2 := InitMemtable(cfg)
		mem2.Put(newRecord(Bytes("b"), Bytes("2"), 1))
		fs2, err := OpenFS(ctx, path.Join(dir, sstPath(2)))
		require.NoError(t, err)
		_, _, err = flush(ctx, cfg, mem2, fs2)
		require.NoError(t, err)
		require.NoError(t, fs2.Close())

		// Add a non-sstable file to ensure it is ignored.
		nonSSTPath := filepath.Join(dir, "ignore.txt")
		require.NoError(t, os.WriteFile(nonSSTPath, []byte("junk"), 0o644))

		vs, err := buildVersionSetFromDisk(ctx, cfg)
		require.NoError(t, err)
		require.Len(t, vs.Levels, 1)
		var nums []uint64
		for _, fm := range vs.Levels[0] {
			nums = append(nums, fm.Number)
		}
		assert.ElementsMatch(t, []uint64{1, 2}, nums)
	})

	t.Run("empty directory", func(t *testing.T) {
		dir := t.TempDir()
		cfg.databaseDir = dir

		vs, err := buildVersionSetFromDisk(ctx, cfg)
		require.NoError(t, err)
		require.Len(t, vs.Levels, 1)
		assert.Len(t, vs.Levels[0], 0)
	})

	t.Run("invalid sstable filename", func(t *testing.T) {
		dir := t.TempDir()
		cfg.databaseDir = dir

		badPath := filepath.Join(dir, "bad.sst")
		require.NoError(t, os.WriteFile(badPath, []byte("junk"), 0o644))
		_, err := buildVersionSetFromDisk(ctx, cfg)
		assert.Error(t, err)
	})

	t.Run("malformed sstable file", func(t *testing.T) {
		dir := t.TempDir()
		cfg.databaseDir = dir

		badContent := filepath.Join(dir, sstPath(3))
		require.NoError(t, os.WriteFile(badContent, []byte("garbage"), 0o644))
		_, err := buildVersionSetFromDisk(ctx, cfg)
		assert.Error(t, err)
	})
}

func Test_getMaxSequenceNumberFromSSTables(t *testing.T) {
	cfg := testConfig()

	t.Run("Empty or Non-existent Level 0", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// Case 1: no levels exist
		ts.Manager.levels = nil
		if ts.Manager.versionSet != nil {
			ts.Manager.versionSet.Levels = nil
		}
		maxSeqNum, err := getMaxSequenceNumberFromSSTables(ctx, ts.Manager)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), maxSeqNum)

		// Case 2: empty slice of levels
		ts.Manager.levels = []*LinkedList[*FileSystem]{}
		if ts.Manager.versionSet != nil {
			ts.Manager.versionSet.Levels = [][]FileMeta{}
		}
		maxSeqNum, err = getMaxSequenceNumberFromSSTables(ctx, ts.Manager)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), maxSeqNum)

		// Case 3: Level 0 exists but is empty
		ts.Manager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		if ts.Manager.versionSet != nil {
			ts.Manager.versionSet.Levels = [][]FileMeta{[]FileMeta{}}
		}
		maxSeqNum, err = getMaxSequenceNumberFromSSTables(ctx, ts.Manager)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), maxSeqNum)
	})

	t.Run("Single SSTable in Level 0", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// Create an SSTable with a specific sequence number
		sstable := ts.createSSTableWithSequence(0, map[string]string{"key1": "val1"}, 100)
		ts.AddSSTableToLevel(0, sstable)

		maxSeqNum, err := getMaxSequenceNumberFromSSTables(ctx, ts.Manager)
		assert.NoError(t, err)
		assert.Equal(t, uint64(100), maxSeqNum)
	})

	t.Run("Multiple SSTables in Level 0", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// Add SSTables with various sequence numbers
		ts.AddSSTableToLevel(0, ts.createSSTableWithSequence(0, map[string]string{"k1": "v1"}, 50))
		ts.AddSSTableToLevel(0, ts.createSSTableWithSequence(0, map[string]string{"k2": "v2"}, 150))
		ts.AddSSTableToLevel(0, ts.createSSTableWithSequence(0, map[string]string{"k3": "v3"}, 75))
		ts.AddSSTableToLevel(0, ts.createSSTableWithSequence(0, map[string]string{"k4": "v4"}, 200)) // Max

		maxSeqNum, err := getMaxSequenceNumberFromSSTables(ctx, ts.Manager)
		assert.NoError(t, err)
		assert.Equal(t, uint64(200), maxSeqNum)
	})

	t.Run("SSTables with Zero Sequence Numbers", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		ts.AddSSTableToLevel(0, ts.createSSTableWithSequence(0, map[string]string{"k1": "v1"}, 0))
		ts.AddSSTableToLevel(0, ts.createSSTableWithSequence(0, map[string]string{"k2": "v2"}, 0))

		maxSeqNum, err := getMaxSequenceNumberFromSSTables(ctx, ts.Manager)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), maxSeqNum)
	})

	t.Run("Error during fs.Open", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// Inject metadata pointing to a missing SSTable file
		if ts.Manager.versionSet == nil {
			ts.Manager.versionSet = &VersionSet{}
		}
		ts.Manager.versionSet.Levels = [][]FileMeta{{{Number: 999}}}

		maxSeqNum, err := getMaxSequenceNumberFromSSTables(ctx, ts.Manager)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "malformed sstable")
		assert.Equal(t, uint64(0), maxSeqNum)
	})

	// Note: Simulating errors for NewSSTable and sstable.MaxSequenceNumber
	// would require mocking the SStable interface or modifying the NewSSTable
	// function, which is beyond the scope of a typical unit test for this function.
	// The current setup tests the happy path and file system errors.
}

func TestSSTableManager_MergeSSTables(t *testing.T) {
	// Configure the manager to trigger compaction after 3 files in level 0
	cfg := NewConfig(WithLevel0CompactionThreshold(3))

	t.Run("Merging sstables via compaction", func(t *testing.T) {
		ctx := context.Background()
		// Use the config with the modified threshold
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// Create SSTables and add them to Level 0 with increasing sequence numbers
		sstable1 := ts.createSSTableWithSequence(0, map[string]string{
			"1": "2", // Will be overridden by sstable2
			"2": "3", // Will be tombstoned by sstable2
			"3": "4",
		}, 1)
		ts.AddSSTableToLevel(0, sstable1)

		sstable2 := ts.createSSTableWithSequence(0, map[string]string{
			"1": "3", // Overrides sstable1
			"2": "",  // Tombstone overrides sstable1
			"4": "5",
		}, 10)
		ts.AddSSTableToLevel(0, sstable2)

		sstable3 := ts.createSSTableWithSequence(0, map[string]string{
			"5": "6",
		}, 20)
		ts.AddSSTableToLevel(0, sstable3)

		// Verify initial state: Level 0 has 3 files
		assert.Equal(t, 3, ts.Manager.levels[0].Len(), "Level 0 should have 3 SSTables before compaction")

		// Trigger compaction (Level 0 -> Level 1)
		err := ts.Manager.Compact(ctx)
		assert.NoError(t, err, "Compaction failed")

		// Verify state after compaction
		assert.Equal(t, 0, ts.Manager.levels[0].Len(), "Level 0 should be empty after compaction")
		assert.GreaterOrEqual(t, len(ts.Manager.levels), 2, "Manager should have at least 2 levels after compaction")
		assert.NotNil(t, ts.Manager.levels[1], "Level 1 list should exist")
		assert.Equal(t, 1, ts.Manager.levels[1].Len(), "Level 1 should have exactly one merged SSTable")

		// Retrieve the merged SSTable from Level 1
		mergedFs, err := ts.Manager.levels[1].Iterator().Next()
		assert.NoError(t, err, "Failed to get merged FS from Level 1")
		err = mergedFs.Open(ctx) // Ensure it's open if closed previously
		assert.NoError(t, err, "Failed to open merged FS")

		mergedSSTable, err := NewSSTable(ctx, cfg, mergedFs)
		assert.NoError(t, err, "Failed to create SStable object from merged FS")

		// Verify the content of the merged SSTable
		assert.Equal(t, 5, len(mergedSSTable.SparseIndex), "Merged SSTable sparse index length mismatch")

		sstableIterator, err := mergedSSTable.Iterator()
		assert.NoError(t, err, "Failed to get iterator for merged SSTable")

		// mem.Put(NewRecord(v.key, v.value, uint64(i)))
		expectedRecords := []Record{
			newRecord(Bytes("1"), Bytes("3"), 10), // sstable2 value
			newRecord(Bytes("2"), nil, 11),        // sstable2 tombstone
			newRecord(Bytes("3"), Bytes("4"), 3),  // sstable1 value
			newRecord(Bytes("4"), Bytes("5"), 12), // sstable2 value
			newRecord(Bytes("5"), Bytes("6"), 20), // sstable3 value
		}
		assertIteratorRecords(t, sstableIterator, expectedRecords)
	})
}

func TestSSTableManager_SearchKey(t *testing.T) {
	cfg := testConfig()
	t.Run("Key absent in empty SSTables", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// Ensure levels are initialized but empty (as done by NewTestRindbSetup)
		assert.NotNil(t, ts.Manager, "Manager should not be nil")
		assert.NotNil(t, ts.Manager.levels, "Manager levels slice should not be nil")
		// Setup ensures at least 3 levels exist and are non-nil lists
		assert.GreaterOrEqual(t, len(ts.Manager.levels), 3, "Manager should have at least 3 levels")
		assert.NotNil(t, ts.Manager.levels[0], "Manager level 0 list should not be nil")
		assert.Equal(t, 0, ts.Manager.levels[0].Len(), "Level 0 should be empty initially")

		// Search for a random key in an empty manager
		key := randStringBytes(10)
		result, err := ts.Manager.searchKey(ctx, key)

		// Assertions remain the same: expect key not found
		assert.ErrorIs(t, err, ErrKeyNotFound, "Expected ErrKeyNotFound when searching empty manager")
		assert.Nil(t, result, "Expected nil result when key is not found")
	})

	t.Run("Key in level 0 only", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		key := Bytes("level0-key")
		value := Bytes("level0-value")
		sst := ts.createSSTable(0, map[string]string{string(key): string(value)})
		ts.AddSSTableToLevel(0, sst)

		result, err := ts.Manager.searchKey(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, value, result)
	})

	t.Run("Key in level 1 overridden by level 0", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		key := randStringBytes(10)
		oldValue := Bytes("old-value")
		sst1 := ts.createSSTable(1, map[string]string{string(key): string(oldValue)})
		ts.AddSSTableToLevel(1, sst1)

		newValue := Bytes("new-value")
		sst0 := ts.createSSTable(0, map[string]string{string(key): string(newValue)})
		ts.AddSSTableToLevel(0, sst0)

		result, err := ts.Manager.searchKey(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, newValue, result)
	})

	t.Run("Key not found in any level", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		sst := ts.createSSTable(0, map[string]string{"some-key": "some-value"})
		ts.AddSSTableToLevel(0, sst)

		missingKey := randStringBytes(10)
		result, err := ts.Manager.searchKey(ctx, missingKey)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Empty SSTableManager", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		ts.Manager.levels = nil
		ts.Manager.versionSet.Levels = nil

		result, err := ts.Manager.searchKey(ctx, Bytes("any-key"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Single SSTable in level 0", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		key := Bytes("single-key")
		value := Bytes("single-value")
		sst := ts.createSSTable(0, map[string]string{string(key): string(value)})
		ts.AddSSTableToLevel(0, sst)

		result, err := ts.Manager.searchKey(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, value, result)
	})

	t.Run("Tombstone in level 0 overrides level 1", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		key := Bytes("tombstone-key")
		value := Bytes("original-value")
		sst1 := ts.createSSTable(1, map[string]string{string(key): string(value)})
		ts.AddSSTableToLevel(1, sst1)

		fs0 := ts.newSSTableFS(0)
		sst0 := createSSTable(t, cfg, fs0, [2]Bytes{key, nil})
		ts.AddSSTableToLevel(0, &sst0)

		result, err := ts.Manager.searchKey(ctx, key)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Bloom filter skips irrelevant SSTables", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		sst := ts.createSSTable(0, map[string]string{"present-key": "present-value"})
		ts.AddSSTableToLevel(0, sst)

		result, err := ts.Manager.searchKey(ctx, Bytes("absent-key"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Missing SSTable is not recreated", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		key := Bytes("lost-key")
		value := Bytes("lost-value")
		sst := ts.createSSTable(0, map[string]string{string(key): string(value)})
		ts.AddSSTableToLevel(0, sst)

		path := sst.Path()
		err := os.Remove(path)
		assert.NoError(t, err)

		result, err := ts.Manager.searchKey(ctx, key)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)

		_, statErr := os.Stat(path)
		assert.True(t, os.IsNotExist(statErr))
	})
}

func TestSSTableManager_CompactThreshold(t *testing.T) {
	t.Run("level 0 file count triggers compaction", func(t *testing.T) {
		cfg := NewConfig(WithLevel0CompactionThreshold(4))
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// Create 4 SSTables in level 0 using the setup helper
		for i := 0; i < 4; i++ {
			_ = ts.createSSTable(0, map[string]string{
				fmt.Sprintf("key%d", i): "value",
			})
			sstable := ts.createSSTable(0, map[string]string{fmt.Sprintf("key%d", i): "value"})
			ts.AddSSTableToLevel(0, sstable) // Add the created sstable's FS to the level
		}

		assert.True(t, ts.Manager.shouldCompact(ctx, 0, ts.Manager.versionSet.Levels[0]))
		assert.NoError(t, ts.Manager.Compact(ctx))
		assert.Equal(t, 0, ts.Manager.levels[0].Len(), "Level 0 should be empty after compaction")
		assert.GreaterOrEqual(t, len(ts.Manager.levels), 2, "Should have created level 1")
		assert.NotNil(t, ts.Manager.levels[1], "Level 1 list should exist")
		assert.Equal(t, 1, ts.Manager.levels[1].Len(), "Level 1 should have merged SSTable")
	})

	t.Run("levels below threshold dont compact", func(t *testing.T) {
		threshold := 4
		cfg := NewConfig(WithLevel0CompactionThreshold(threshold))
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// --- Test Level 0 ---
		level0FileCount := threshold - 1
		initialLevel0Files := make([]*FileSystem, level0FileCount)
		for i := 0; i < level0FileCount; i++ {
			sstable := ts.createSSTable(0, map[string]string{fmt.Sprintf("l0-key%d", i): "value"})
			ts.AddSSTableToLevel(0, sstable)
			initialLevel0Files[i] = sstable.FileSystem // Keep track for assertion
		}
		assert.Equal(t, level0FileCount, ts.Manager.levels[0].Len(), "Pre-check: Level 0 should have %d files", level0FileCount)

		// --- Test Level 1 ---
		level1FileCount := 1
		initialLevel1Files := make([]*FileSystem, level1FileCount)
		sstable1 := ts.createSSTable(1, map[string]string{"l1-key": "small-value"})
		ts.AddSSTableToLevel(1, sstable1)
		initialLevel1Files[0] = sstable1.FileSystem
		assert.Equal(t, level1FileCount, ts.Manager.levels[1].Len(), "Pre-check: Level 1 should have %d file", level1FileCount)

		// --- Act ---
		err := ts.Manager.Compact(ctx)
		assert.NoError(t, err)

		// --- Assert ---
		// Level 0 should be unchanged
		assert.Equal(t, level0FileCount, ts.Manager.levels[0].Len(), "Level 0 count should remain %d after compact", level0FileCount)
		currentLevel0Files := make([]*FileSystem, 0, ts.Manager.levels[0].Len())
		iter0 := ts.Manager.levels[0].Iterator()
		for iter0.HasNext() {
			f, err := iter0.Next()
			assert.NoError(t, err)
			currentLevel0Files = append(currentLevel0Files, f)
		}
		assert.ElementsMatch(t, initialLevel0Files, currentLevel0Files, "Level 0 files should be the same instances")

		// Level 1 should be unchanged
		assert.Equal(t, level1FileCount, ts.Manager.levels[1].Len(), "Level 1 count should remain %d after compact", level1FileCount)
		currentLevel1Files := make([]*FileSystem, 0, ts.Manager.levels[1].Len())
		iter1 := ts.Manager.levels[1].Iterator()
		for iter1.HasNext() {
			f, err := iter1.Next()
			assert.NoError(t, err)
			currentLevel1Files = append(currentLevel1Files, f)
		}
		assert.ElementsMatch(t, initialLevel1Files, currentLevel1Files, "Level 1 files should be the same instances")

		// No higher levels should have been created beyond the initial setup (usually 3 levels in setup)
		assert.LessOrEqual(t, len(ts.Manager.levels), 3, "No new levels beyond initial setup should be created")
	})

	t.Run("level 1 size above threshold triggers compaction (lowered threshold)", func(t *testing.T) {
		ctx := context.Background()
		// Configure low thresholds for easy testing
		cfg := NewConfig(
			WithLevel0CompactionThreshold(2), // Low L0 threshold
			WithBaseCompactionSizeMB(1),      // Low base size: 1MB
			WithLevelSizeMultiplier(2),       // Low multiplier: 2x per level -> L1 threshold = 1 * 2^1 = 2MB
		)
		ts := newTestRindbSetup(t, ctx, &cfg) // Setup will use a temp dir
		defer ts.Cleanup()

		numFiles := 2
		recordsPerFile := 1000
		valueSize := 1024 // 1KB values

		level1Paths := make([]string, numFiles)
		var totalSize int64

		INFO(ctx, "Creating SSTables for Level 1 (target > 2MB total)...")
		for i := 0; i < numFiles; i++ {
			// Use ts.CreateSSTable which uses the manager's config and FS creation
			kvs := make(map[string]string)
			pairs := generateKeyValuePairs(recordsPerFile, 10, valueSize)
			for _, p := range pairs {
				kvs[string(p[0])] = string(p[1])
			}
			sstable := ts.createSSTable(1, kvs) // Create in level 1
			ts.AddSSTableToLevel(1, sstable)

			level1Paths[i] = sstable.Path()
			info, statErr := os.Stat(sstable.Path())
			assert.NoError(t, statErr)
			totalSize += info.Size()
			INFO(ctx, "Created Level 1 SSTable %s, size: %d bytes", sstable.Path(), info.Size())
		}

		// Calculate the threshold used in this test
		level1ThresholdBytes := int64(cfg.baseCompactionSizeMB) * int64(math.Pow(float64(cfg.levelSizeMultiplier), 1.0)) * 1024 * 1024
		INFO(ctx, "Total size of Level 1 files: %d bytes (%.2f MB). Threshold: %d bytes (%.2f MB)",
			totalSize, float64(totalSize)/(1024*1024),
			level1ThresholdBytes, float64(level1ThresholdBytes)/(1024*1024))

		// Sanity check: Ensure total size exceeds the *lowered* threshold
		assert.Greater(t, totalSize, level1ThresholdBytes, "Total size should exceed the lowered threshold")
		assert.Equal(t, numFiles, ts.Manager.levels[1].Len(), "Pre-check: Level 1 should have %d files", numFiles)

		INFO(ctx, "Calling Compact()...")
		err := ts.Manager.Compact(ctx)
		assert.NoError(t, err)
		INFO(ctx, "Compact() finished.")

		// Assert:
		// 1. Level 1 should now be empty.
		assert.Equal(t, 0, ts.Manager.levels[1].Len(), "Level 1 should be empty after compaction")
		assert.GreaterOrEqual(t, len(ts.Manager.levels), 3, "Should have created level 2")
		assert.NotNil(t, ts.Manager.levels[2], "Level 2 list should exist")
		assert.Equal(t, 1, ts.Manager.levels[2].Len(), "Level 2 should have 1 merged SSTable")

		// 2. Level 2 should exist and contain exactly one merged SSTable.
		// 3. Check if the original Level 1 files were removed.
		for _, p := range level1Paths {
			assertFileNotExists(t, p)
		}

		// 4. (Optional) Verify the content/size of the merged Level 2 SSTable
		iter2 := ts.Manager.levels[2].Iterator()
		mergedFs, err := iter2.Next()
		assert.NoError(t, err)
		mergedInfo, err := os.Stat(mergedFs.Path())
		assert.NoError(t, err)
		INFO(ctx, "Merged Level 2 SSTable size: %d bytes", mergedInfo.Size())
		// Check if size is roughly the sum of originals (minus overhead/duplicates, should be close)
		assert.InDelta(t, totalSize, mergedInfo.Size(), float64(totalSize)*0.1, "Merged size should be close to original total")
	})
}

func TestSSTableManager_GetRelevantSSTables(t *testing.T) {
	cfg := testConfig()

	t.Run("Level 0 returns all SSTables in newest-first order", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		older := ts.createSSTable(0, map[string]string{"a": "1"})
		newer := ts.createSSTable(0, map[string]string{"b": "2"})
		ts.AddSSTableToLevel(0, older)
		ts.AddSSTableToLevel(0, newer)

		nOlder, _ := fileNum(older.Path())
		nNewer, _ := fileNum(newer.Path())

		nums := ts.Manager.GetRelevantSSTables(ctx, Bytes("a"), Bytes("z"))
		assert.Equal(t, []uint64{nNewer, nOlder}, nums)
	})

	t.Run("Level 1+ returns only overlapping SSTables in oldest-first order", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		older := ts.createSSTable(1, map[string]string{"b": "1"})
		newer := ts.createSSTable(1, map[string]string{"c": "2"})
		ts.AddSSTableToLevel(1, older)
		ts.AddSSTableToLevel(1, newer)

		nOlder, _ := fileNum(older.Path())
		nNewer, _ := fileNum(newer.Path())

		nums := ts.Manager.GetRelevantSSTables(ctx, Bytes("b"), Bytes("d"))
		assert.Equal(t, []uint64{nOlder, nNewer}, nums)
	})

	t.Run("No relevant SSTables found", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		sst := ts.createSSTable(1, map[string]string{"x": "1"})
		ts.AddSSTableToLevel(1, sst)

		nums := ts.Manager.GetRelevantSSTables(ctx, Bytes("a"), Bytes("b"))
		assert.Len(t, nums, 0)
	})

	t.Run("Empty SSTableManager", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		ts.Manager.versionSet = nil
		nums := ts.Manager.GetRelevantSSTables(ctx, Bytes("a"), Bytes("z"))
		assert.Nil(t, nums)
	})

	t.Run("Error opening SSTable", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		sst := ts.createSSTable(1, map[string]string{"b": "1"})
		ts.AddSSTableToLevel(1, sst)
		assert.NoError(t, os.Remove(sst.Path()))

		nums := ts.Manager.GetRelevantSSTables(ctx, Bytes("a"), Bytes("z"))
		assert.Len(t, nums, 0)
	})

	t.Run("Mixed levels with overlapping and non-overlapping", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// Level 0
		l0a := ts.createSSTable(0, map[string]string{"a": "1"}) // older
		l0b := ts.createSSTable(0, map[string]string{"b": "2"}) // newer
		ts.AddSSTableToLevel(0, l0a)
		ts.AddSSTableToLevel(0, l0b)

		// Level 1
		l1Overlap := ts.createSSTable(1, map[string]string{"b": "1", "c": "2"})
		l1Non := ts.createSSTable(1, map[string]string{"x": "1"})
		ts.AddSSTableToLevel(1, l1Overlap)
		ts.AddSSTableToLevel(1, l1Non)

		nL0b, _ := fileNum(l0b.Path())
		nL0a, _ := fileNum(l0a.Path())
		nL1, _ := fileNum(l1Overlap.Path())

		nums := ts.Manager.GetRelevantSSTables(ctx, Bytes("a"), Bytes("d"))
		assert.Equal(t, []uint64{nL0b, nL0a, nL1}, nums)
	})
}

func TestSSTableManager_DynamicShouldCompact(t *testing.T) {
	cfg := NewConfig(
		WithLevel0CompactionThreshold(1000),
		WithWriteRateTrigger(10),
		WithIOLoadMax(0.5),
	)

	ctx := context.Background()
	cases := []struct {
		name   string
		ioVal  uint64
		expect bool
	}{
		{"high write rate low io load triggers", 100, true},
		{"high write rate high io load defers", 900, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			current := time.Unix(0, 0)
			ioVal := uint64(0)

			sm := &SSTableManager{
				openedFs:       make(map[uint64]*FileSystem),
				levels:         []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()},
				versionSet:     &VersionSet{Levels: [][]FileMeta{{{Number: 1, Level: 0}}}},
				config:         cfg,
				now:            func() time.Time { return current },
				diskSampler:    func() (uint64, error) { return ioVal, nil },
				minSnapshotSeq: math.MaxUint64,
			}
			sm.levels[0].PushBack(&FileSystem{filePath: "dummy"})

			for i := 0; i < 100; i++ {
				sm.recordWrite()
			}
			current = current.Add(time.Second)
			sm.recordWrite()

			sm.sampleIOLoad()
			ioVal = tc.ioVal
			current = current.Add(time.Second)
			sm.sampleIOLoad()

			assert.Equal(t, tc.expect, sm.shouldCompact(ctx, 0, sm.versionSet.Levels[0]))
		})
	}
}

func TestSSTableManager_IOLoadSampler(t *testing.T) {
	t.Run("records io load", func(t *testing.T) {
		ctx := context.Background()
		cfg := testConfig()
		cfg.databaseDir = t.TempDir()
		sm, err := InitSSTableManager(ctx, cfg, &VersionSet{}, nil)
		assert.NoError(t, err)
		defer sm.Close(ctx)

		var total uint64
		sm.diskSampler = func() (uint64, error) {
			total += 100
			return total, nil
		}

		time.Sleep(2100 * time.Millisecond)

		sm.mu.RLock()
		load := sm.ioLoad
		sm.mu.RUnlock()

		assert.Greater(t, load, float64(0), "expected io load to be recorded")
	})

	t.Run("stops on close", func(t *testing.T) {
		ctx := context.Background()
		cfg := testConfig()
		cfg.databaseDir = t.TempDir()
		sm, err := InitSSTableManager(ctx, cfg, &VersionSet{}, nil)
		assert.NoError(t, err)

		var mu sync.Mutex
		var calls int
		sm.diskSampler = func() (uint64, error) {
			mu.Lock()
			defer mu.Unlock()
			calls++
			return uint64(calls), nil
		}

		time.Sleep(1100 * time.Millisecond)

		mu.Lock()
		first := calls
		mu.Unlock()
		assert.Greater(t, first, 0, "expected sampler to be invoked")

		sm.Close(ctx)

		time.Sleep(1100 * time.Millisecond)

		mu.Lock()
		final := calls
		mu.Unlock()
		assert.Equal(t, first, final, "sampler should stop after Close")

		done := make(chan struct{})
		go func() {
			sm.ioSamplerWG.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("sampler goroutine did not exit")
		}
	})
}

func Test_mergeSSTablesV2(t *testing.T) {
	cfg := testConfig()

	t.Run("keeps tombstones when not bottommost", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		mem1 := InitMemtable(*ts.Config)
		mem1.Put(newRecord(Bytes("a"), Bytes("1"), 1))
		mem1.Put(newRecord(Bytes("b"), Bytes("2"), 2))
		sst1, _, err := flush(ctx, *ts.Config, mem1, ts.newSSTableFS(0))
		assert.NoError(t, err)

		mem2 := InitMemtable(*ts.Config)
		mem2.Put(newRecord(Bytes("b"), nil, 3))
		mem2.Put(newRecord(Bytes("c"), Bytes("3"), 4))
		sst2, _, err := flush(ctx, *ts.Config, mem2, ts.newSSTableFS(0))
		assert.NoError(t, err)

		ts.AddSSTableToLevel(0, &sst1)
		ts.AddSSTableToLevel(0, &sst2)
		ts.Manager.levels[0] = InitLinkedList[*FileSystem]()

		target := ts.newSSTableFS(1)
		merged, _, err := mergeSSTablesV2(ctx, *ts.Config, target, []SStable{sst1, sst2}, false, math.MaxUint64)
		assert.NoError(t, err)
		assert.NotNil(t, merged)

		v, err := merged.GetValue(ctx, Bytes("a"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("1"), v)

		// Builder should have populated bloom filter
		assert.True(t, merged.Bloom.Lookup(Bytes("a")))
		assert.False(t, merged.Bloom.Lookup(Bytes("x")))

		v, err = merged.GetValue(ctx, Bytes("b"))
		assert.ErrorIs(t, err, ErrTombstoneFound)
		assert.Nil(t, v)

		v, err = merged.GetValue(ctx, Bytes("c"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("3"), v)
	})

	t.Run("gc tombstones at bottommost level", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		mem1 := InitMemtable(*ts.Config)
		mem1.Put(newRecord(Bytes("a"), Bytes("1"), 1))
		mem1.Put(newRecord(Bytes("b"), Bytes("2"), 2))
		sst1, _, err := flush(ctx, *ts.Config, mem1, ts.newSSTableFS(0))
		assert.NoError(t, err)

		mem2 := InitMemtable(*ts.Config)
		mem2.Put(newRecord(Bytes("b"), nil, 3))
		mem2.Put(newRecord(Bytes("c"), Bytes("3"), 4))
		sst2, _, err := flush(ctx, *ts.Config, mem2, ts.newSSTableFS(0))
		assert.NoError(t, err)

		target := ts.newSSTableFS(1)
		merged, _, err := mergeSSTablesV2(ctx, *ts.Config, target, []SStable{sst1, sst2}, true, math.MaxUint64)
		assert.NoError(t, err)
		assert.NotNil(t, merged)

		v, err := merged.GetValue(ctx, Bytes("a"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("1"), v)

		v, err = merged.GetValue(ctx, Bytes("c"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("3"), v)

		iter, err := merged.Iterator()
		assert.NoError(t, err)
		for iter.HasNext() {
			rec, err := iter.Next()
			assert.NoError(t, err)
			assert.NotEqual(t, Bytes("b"), rec.GetKey())
		}
	})

	t.Run("preserves records for active snapshot", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		mem1 := InitMemtable(*ts.Config)
		mem1.Put(newRecord(Bytes("b"), Bytes("v1"), 1))
		sst1, _, err := flush(ctx, *ts.Config, mem1, ts.newSSTableFS(0))
		assert.NoError(t, err)

		mem2 := InitMemtable(*ts.Config)
		mem2.Put(newRecord(Bytes("b"), Bytes("v2"), 2))
		sst2, _, err := flush(ctx, *ts.Config, mem2, ts.newSSTableFS(0))
		assert.NoError(t, err)

		mem3 := InitMemtable(*ts.Config)
		mem3.Put(newRecord(Bytes("b"), nil, 3))
		sst3, _, err := flush(ctx, *ts.Config, mem3, ts.newSSTableFS(0))
		assert.NoError(t, err)

		target := ts.newSSTableFS(1)
		merged, _, err := mergeSSTablesV2(ctx, *ts.Config, target, []SStable{sst1, sst2, sst3}, true, 2)
		assert.NoError(t, err)
		assert.NotNil(t, merged)

		v, err := merged.GetValue(ctx, Bytes("b"))
		assert.ErrorIs(t, err, ErrTombstoneFound)
		assert.Nil(t, v)

		iter, err := merged.Iterator()
		assert.NoError(t, err)
		var recs []Record
		for iter.HasNext() {
			rec, err := iter.Next()
			assert.NoError(t, err)
			recs = append(recs, rec)
		}
		assert.Equal(t, 2, len(recs))
		assert.Equal(t, uint64(3), recs[0].GetSequenceNumber())
		assert.Equal(t, TypeDeletion, recs[0].GetType())
		assert.Equal(t, uint64(2), recs[1].GetSequenceNumber())
		assert.Equal(t, TypeValue, recs[1].GetType())
	})

	t.Run("handles multiple versions of a key", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		mem1 := InitMemtable(*ts.Config)
		mem1.Put(newRecord(Bytes("b"), Bytes("v1"), 1))
		sst1, _, err := flush(ctx, *ts.Config, mem1, ts.newSSTableFS(0))
		assert.NoError(t, err)

		mem2 := InitMemtable(*ts.Config)
		mem2.Put(newRecord(Bytes("b"), Bytes("v2"), 2))
		sst2, _, err := flush(ctx, *ts.Config, mem2, ts.newSSTableFS(0))
		assert.NoError(t, err)

		mem3 := InitMemtable(*ts.Config)
		mem3.Put(newRecord(Bytes("b"), nil, 3))
		sst3, _, err := flush(ctx, *ts.Config, mem3, ts.newSSTableFS(0))
		assert.NoError(t, err)

		target := ts.newSSTableFS(1)
		merged, _, err := mergeSSTablesV2(ctx, *ts.Config, target, []SStable{sst1, sst2, sst3}, false, math.MaxUint64)
		assert.NoError(t, err)
		assert.NotNil(t, merged)

		v, err := merged.GetValue(ctx, Bytes("b"))
		assert.ErrorIs(t, err, ErrTombstoneFound)
		assert.Nil(t, v)

		target2 := ts.newSSTableFS(1)
		merged2, _, err := mergeSSTablesV2(ctx, *ts.Config, target2, []SStable{sst1, sst2, sst3}, true, math.MaxUint64)
		assert.NoError(t, err)
		assert.Nil(t, merged2)
	})

	t.Run("returns nil on empty sources", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		target := ts.newSSTableFS(1)
		merged, _, err := mergeSSTablesV2(ctx, *ts.Config, target, []SStable{}, false, math.MaxUint64)
		assert.NoError(t, err)
		assert.Nil(t, merged)
	})

	t.Run("returns nil when only tombstones and bottommost", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		mem := InitMemtable(*ts.Config)
		mem.Put(newRecord(Bytes("a"), nil, 1))
		sst, _, err := flush(ctx, *ts.Config, mem, ts.newSSTableFS(0))
		assert.NoError(t, err)

		target := ts.newSSTableFS(1)
		merged, _, err := mergeSSTablesV2(ctx, *ts.Config, target, []SStable{sst}, true, math.MaxUint64)
		assert.NoError(t, err)
		assert.Nil(t, merged)
	})

	t.Run("honors context cancellation", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		mem := InitMemtable(*ts.Config)
		mem.Put(newRecord(Bytes("a"), Bytes("1"), 1))
		sst, _, err := flush(ctx, *ts.Config, mem, ts.newSSTableFS(0))
		assert.NoError(t, err)

		target := ts.newSSTableFS(1)
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()
		merged, _, err := mergeSSTablesV2(cancelCtx, *ts.Config, target, []SStable{sst}, false, math.MaxUint64)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, merged)
	})
}
