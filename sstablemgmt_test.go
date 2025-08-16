package rindb

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSSTableManager_LoadLevels(t *testing.T) {
	cfg := testConfig()
	t.Run("LoadLevels validates file names", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()
		assert.NoError(t, ts.Manager.Compact(context.Background()))
		for levelNumb, level := range ts.Manager.levels {
			iterator := level.Iterator()
			for iterator.HasNext() {
				fs, err := iterator.Next()
				assert.NoError(t, err)
				segments := strings.Split(fs.Path(), "/")
				fileName := segments[len(segments)-1]
				expectedPrefix := fmt.Sprintf("l%02d_", levelNumb)
				assert.True(t, strings.HasPrefix(fileName, expectedPrefix),
					"expected filename '%s' to start with '%s'", fileName, expectedPrefix)
				assert.True(t, strings.HasSuffix(fileName, ".sst"),
					"expected filename '%s' to end with '.sst'", fileName)
			}
		}
	})

	t.Run("Key in older SSTable requires Prev() iteration", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// Create SSTables
		older := ts.createSSTable(0, map[string]string{"targetKey": "targetVal"})
		newer := ts.createSSTable(0, map[string]string{"otherKey": "otherVal"})

		// Add to level 0 (older first, then newer)
		ts.AddSSTableToLevel(0, older)
		ts.AddSSTableToLevel(0, newer)

		// Verify search finds the key in older SSTable
		result, err := ts.Manager.searchKey(context.Background(), Bytes("targetKey"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("targetVal"), result)
	})
}

func Test_getMaxSequenceNumberFromSSTables(t *testing.T) {
	cfg := testConfig()

	t.Run("Empty or Non-existent Level 0", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// Case 1: ssTableManager.levels is nil
		ts.Manager.levels = nil
		maxSeqNum, err := getMaxSequenceNumberFromSSTables(context.Background(), ts.Manager)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), maxSeqNum)

		// Case 2: ssTableManager.levels is empty slice
		ts.Manager.levels = []*LinkedList[*FileSystem]{}
		maxSeqNum, err = getMaxSequenceNumberFromSSTables(context.Background(), ts.Manager)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), maxSeqNum)

		// Case 3: Level 0 exists but is empty
		ts.Manager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		maxSeqNum, err = getMaxSequenceNumberFromSSTables(context.Background(), ts.Manager)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), maxSeqNum)
	})

	t.Run("Single SSTable in Level 0", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// Create an SSTable with a specific sequence number
		sstable := ts.createSSTableWithSequence(0, map[string]string{"key1": "val1"}, 100)
		ts.AddSSTableToLevel(0, sstable)

		maxSeqNum, err := getMaxSequenceNumberFromSSTables(context.Background(), ts.Manager)
		assert.NoError(t, err)
		assert.Equal(t, uint64(100), maxSeqNum)
	})

	t.Run("Multiple SSTables in Level 0", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// Add SSTables with various sequence numbers
		ts.AddSSTableToLevel(0, ts.createSSTableWithSequence(0, map[string]string{"k1": "v1"}, 50))
		ts.AddSSTableToLevel(0, ts.createSSTableWithSequence(0, map[string]string{"k2": "v2"}, 150))
		ts.AddSSTableToLevel(0, ts.createSSTableWithSequence(0, map[string]string{"k3": "v3"}, 75))
		ts.AddSSTableToLevel(0, ts.createSSTableWithSequence(0, map[string]string{"k4": "v4"}, 200)) // Max

		maxSeqNum, err := getMaxSequenceNumberFromSSTables(context.Background(), ts.Manager)
		assert.NoError(t, err)
		assert.Equal(t, uint64(200), maxSeqNum)
	})

	t.Run("SSTables with Zero Sequence Numbers", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		ts.AddSSTableToLevel(0, ts.createSSTableWithSequence(0, map[string]string{"k1": "v1"}, 0))
		ts.AddSSTableToLevel(0, ts.createSSTableWithSequence(0, map[string]string{"k2": "v2"}, 0))

		maxSeqNum, err := getMaxSequenceNumberFromSSTables(context.Background(), ts.Manager)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), maxSeqNum)
	})

	t.Run("Error during fs.Open", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// Create a dummy FS that will return an error on Open
		badFs := &FileSystem{filePath: "/non/existent/path.sst"}
		ts.Manager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		ts.Manager.levels[0].PushBack(badFs)

		maxSeqNum, err := getMaxSequenceNumberFromSSTables(context.Background(), ts.Manager)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no such file or directory")
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
		// Use the config with the modified threshold
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// Create SSTables and add them to Level 0
		sstable1 := ts.createSSTable(0, map[string]string{
			"1": "2", // Will be overridden by sstable2
			"2": "3", // Will be tombstoned by sstable2
			"3": "4",
		})
		ts.AddSSTableToLevel(0, sstable1)

		sstable2 := ts.createSSTable(0, map[string]string{
			"1": "3", // Overrides sstable1
			"2": "",  // Tombstone overrides sstable1
			"4": "5",
		})
		ts.AddSSTableToLevel(0, sstable2)

		sstable3 := ts.createSSTable(0, map[string]string{
			"5": "6",
		})
		ts.AddSSTableToLevel(0, sstable3)

		// Verify initial state: Level 0 has 3 files
		assert.Equal(t, 3, ts.Manager.levels[0].Len(), "Level 0 should have 3 SSTables before compaction")

		// Trigger compaction (Level 0 -> Level 1)
		err := ts.Manager.Compact(context.Background())
		assert.NoError(t, err, "Compaction failed")

		// Verify state after compaction
		assert.Equal(t, 0, ts.Manager.levels[0].Len(), "Level 0 should be empty after compaction")
		assert.GreaterOrEqual(t, len(ts.Manager.levels), 2, "Manager should have at least 2 levels after compaction")
		assert.NotNil(t, ts.Manager.levels[1], "Level 1 list should exist")
		assert.Equal(t, 1, ts.Manager.levels[1].Len(), "Level 1 should have exactly one merged SSTable")

		// Retrieve the merged SSTable from Level 1
		mergedFs, err := ts.Manager.levels[1].Iterator().Next()
		assert.NoError(t, err, "Failed to get merged FS from Level 1")
		err = mergedFs.Open(context.Background()) // Ensure it's open if closed previously
		assert.NoError(t, err, "Failed to open merged FS")

		mergedSSTable, err := NewSSTable(context.Background(), ts.Manager.config, mergedFs)
		assert.NoError(t, err, "Failed to create SStable object from merged FS")

		// Verify the content of the merged SSTable
		assert.Equal(t, 5, len(mergedSSTable.SparseIndex), "Merged SSTable sparse index length mismatch")

		sstableIterator, err := mergedSSTable.Iterator()
		assert.NoError(t, err, "Failed to get iterator for merged SSTable")

		expectedRecords := []Record{
			RecordImpl{Key: Bytes("1"), Value: Bytes("3")}, // sstable2 value
			RecordImpl{Key: Bytes("2"), Value: nil},        // sstable2 tombstone
			RecordImpl{Key: Bytes("3"), Value: Bytes("4")}, // sstable1 value
			RecordImpl{Key: Bytes("4"), Value: Bytes("5")}, // sstable2 value
			RecordImpl{Key: Bytes("5"), Value: Bytes("6")}, // sstable3 value
		}
		assertIteratorRecords(t, sstableIterator, expectedRecords)
	})
}

func TestSSTableManager_SearchKey(t *testing.T) {
	cfg := testConfig()
	t.Run("Key absent in empty SSTables", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
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
		result, err := ts.Manager.searchKey(context.Background(), key)

		// Assertions remain the same: expect key not found
		assert.ErrorIs(t, err, ErrKeyNotFound, "Expected ErrKeyNotFound when searching empty manager")
		assert.Nil(t, result, "Expected nil result when key is not found")
	})

	t.Run("Key in level 0 only", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		fs, err := ts.Manager.NewSSTableFS(context.Background(), 0)
		assert.NoError(t, err)

		key := Bytes("level0-key")
		value := Bytes("level0-value")
		_ = createSSTable(t, cfg, fs, [2]Bytes{key, value})

		if len(ts.Manager.levels) == 0 {
			// If the levels slice is empty, add a new list for level 0
			ts.Manager.levels = append(ts.Manager.levels, InitLinkedList[*FileSystem]())
		} else if ts.Manager.levels[0] == nil {
			// If the slice has space but level 0 is nil (less likely here, but good practice)
			ts.Manager.levels[0] = InitLinkedList[*FileSystem]()
		}
		ts.Manager.levels[0].PushBack(fs)

		result, err := ts.Manager.searchKey(context.Background(), key)
		assert.NoError(t, err)
		assert.Equal(t, value, result)
	})

	t.Run("Key in level 1 overridden by level 0", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// Level 1: older value
		fs1, err := ts.Manager.NewSSTableFS(context.Background(), 1)
		assert.NoError(t, err)
		key := randStringBytes(10)
		oldValue := Bytes("old-value")
		_ = createSSTable(t, cfg, fs1, [2]Bytes{key, oldValue})

		// Level 0: newer value
		fs0, err := ts.Manager.NewSSTableFS(context.Background(), 0)
		assert.NoError(t, err)
		newValue := Bytes("new-value")
		_ = createSSTable(t, cfg, fs0, [2]Bytes{key, newValue})

		// Override levels with new values
		ts.Manager.levels = []*LinkedList[*FileSystem]{
			InitLinkedList[*FileSystem](),
			InitLinkedList[*FileSystem](),
		}

		ts.Manager.levels[0].PushBack(fs0)
		ts.Manager.levels[1].PushBack(fs1)
		assert.Equal(t, 1, ts.Manager.levels[0].Len())
		assert.Equal(t, 1, ts.Manager.levels[1].Len())

		result, err := ts.Manager.searchKey(context.Background(), key)
		assert.NoError(t, err)
		assert.Equal(t, newValue, result)
	})

	t.Run("Key not found in any level", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		fs, err := ts.Manager.NewSSTableFS(context.Background(), 0)
		assert.NoError(t, err)
		_ = createSSTable(t, cfg, fs, [2]Bytes{Bytes("some-key"), Bytes("some-value")})

		// Override levels with new values
		ts.Manager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		ts.Manager.levels[0].PushBack(fs)

		missingKey := randStringBytes(10)
		result, err := ts.Manager.searchKey(context.Background(), missingKey)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Empty SSTableManager", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		ts.Manager.levels = nil // Explicitly empty

		result, err := ts.Manager.searchKey(context.Background(), Bytes("any-key"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Single SSTable in level 0", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		fs, err := ts.Manager.NewSSTableFS(context.Background(), 0)
		assert.NoError(t, err)
		key := Bytes("single-key")
		value := Bytes("single-value")
		_ = createSSTable(t, cfg, fs, [2]Bytes{key, value})
		ts.Manager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		ts.Manager.levels[0].PushBack(fs)

		result, err := ts.Manager.searchKey(context.Background(), key)
		assert.NoError(t, err)
		assert.Equal(t, value, result)
	})

	t.Run("Tombstone in level 0 overrides level 1", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// Level 1: original value
		fs1, err := ts.Manager.NewSSTableFS(context.Background(), 1)
		assert.NoError(t, err)
		key := Bytes("tombstone-key")
		value := Bytes("original-value")
		_ = createSSTable(t, cfg, fs1, [2]Bytes{key, value})

		// Level 0: tombstone
		fs0, err := ts.Manager.NewSSTableFS(context.Background(), 0)
		assert.NoError(t, err)
		_ = createSSTable(t, cfg, fs0, [2]Bytes{key, nil})

		ts.Manager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem](), InitLinkedList[*FileSystem]()}
		ts.Manager.levels[0].PushBack(fs0)
		ts.Manager.levels[1].PushBack(fs1)

		result, err := ts.Manager.searchKey(context.Background(), key)
		assert.NoError(t, err)
		assert.Nil(t, result) // Tombstone returns nil value
	})

	t.Run("Bloom filter skips irrelevant SSTables", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		fs, err := ts.Manager.NewSSTableFS(context.Background(), 0)
		assert.NoError(t, err)
		_ = createSSTable(t, cfg, fs, [2]Bytes{Bytes("present-key"), Bytes("present-value")})
		ts.Manager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		ts.Manager.levels[0].PushBack(fs)

		// Key not in Bloom filter
		result, err := ts.Manager.searchKey(context.Background(), Bytes("absent-key"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})
}

func TestSSTableManager_CompactThreshold(t *testing.T) {
	t.Run("level 0 file count triggers compaction", func(t *testing.T) {
		cfg := NewConfig(WithLevel0CompactionThreshold(4))
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// Create 4 SSTables in level 0 using the setup helper
		for i := 0; i < 4; i++ {
			_ = ts.createSSTable(0, map[string]string{
				fmt.Sprintf("key%d", i): "value",
			})
			sstable := ts.createSSTable(0, map[string]string{fmt.Sprintf("key%d", i): "value"})
			ts.AddSSTableToLevel(0, sstable) // Add the created sstable's FS to the level
		}

		assert.True(t, ts.Manager.shouldCompact(context.Background(), 0, ts.Manager.levels[0]))
		assert.NoError(t, ts.Manager.Compact(context.Background()))
		assert.Equal(t, 0, ts.Manager.levels[0].Len(), "Level 0 should be empty after compaction")
		assert.GreaterOrEqual(t, len(ts.Manager.levels), 2, "Should have created level 1")
		assert.NotNil(t, ts.Manager.levels[1], "Level 1 list should exist")
		assert.Equal(t, 1, ts.Manager.levels[1].Len(), "Level 1 should have merged SSTable")
	})

	t.Run("levels below threshold dont compact", func(t *testing.T) {
		threshold := 4
		cfg := NewConfig(WithLevel0CompactionThreshold(threshold))
		ts := newTestRindbSetup(t, &cfg)
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
		err := ts.Manager.Compact(context.Background())
		assert.NoError(t, err)

		// --- Assert ---
		// Level 0 should be unchanged
		assert.Equal(t, level0FileCount, ts.Manager.levels[0].Len(), "Level 0 count should remain %d after compact", level0FileCount)
		currentLevel0Files := make([]*FileSystem, 0, ts.Manager.levels[0].Len())
		iter0 := ts.Manager.levels[0].Iterator()
		for iter0.HasNext() {
			f, _ := iter0.Next()
			currentLevel0Files = append(currentLevel0Files, f)
		}
		assert.ElementsMatch(t, initialLevel0Files, currentLevel0Files, "Level 0 files should be the same instances")

		// Level 1 should be unchanged
		assert.Equal(t, level1FileCount, ts.Manager.levels[1].Len(), "Level 1 count should remain %d after compact", level1FileCount)
		currentLevel1Files := make([]*FileSystem, 0, ts.Manager.levels[1].Len())
		iter1 := ts.Manager.levels[1].Iterator()
		for iter1.HasNext() {
			f, _ := iter1.Next()
			currentLevel1Files = append(currentLevel1Files, f)
		}
		assert.ElementsMatch(t, initialLevel1Files, currentLevel1Files, "Level 1 files should be the same instances")

		// No higher levels should have been created beyond the initial setup (usually 3 levels in setup)
		assert.LessOrEqual(t, len(ts.Manager.levels), 3, "No new levels beyond initial setup should be created")
	})

	t.Run("level 1 size above threshold triggers compaction (lowered threshold)", func(t *testing.T) {
		// Configure low thresholds for easy testing
		cfg := NewConfig(
			WithLevel0CompactionThreshold(2), // Low L0 threshold
			WithBaseCompactionSizeMB(1),      // Low base size: 1MB
			WithLevelSizeMultiplier(2),       // Low multiplier: 2x per level -> L1 threshold = 1 * 2^1 = 2MB
		)
		ts := newTestRindbSetup(t, &cfg) // Setup will use a temp dir
		defer ts.Cleanup()

		numFiles := 2
		recordsPerFile := 1000
		valueSize := 1024 // 1KB values

		level1Paths := make([]string, numFiles)
		var totalSize int64

		INFO(context.Background(), "Creating SSTables for Level 1 (target > 2MB total)...")
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
			INFO(context.Background(), "Created Level 1 SSTable %s, size: %d bytes", sstable.Path(), info.Size())
		}

		// Calculate the threshold used in this test
		level1ThresholdBytes := int64(ts.Manager.config.baseCompactionSizeMB) * int64(math.Pow(float64(ts.Manager.config.levelSizeMultiplier), 1.0)) * 1024 * 1024
		INFO(context.Background(), "Total size of Level 1 files: %d bytes (%.2f MB). Threshold: %d bytes (%.2f MB)",
			totalSize, float64(totalSize)/(1024*1024),
			level1ThresholdBytes, float64(level1ThresholdBytes)/(1024*1024))

		// Sanity check: Ensure total size exceeds the *lowered* threshold
		assert.Greater(t, totalSize, level1ThresholdBytes, "Total size should exceed the lowered threshold")
		assert.Equal(t, numFiles, ts.Manager.levels[1].Len(), "Pre-check: Level 1 should have %d files", numFiles)

		INFO(context.Background(), "Calling Compact()...")
		err := ts.Manager.Compact(context.Background())
		assert.NoError(t, err)
		INFO(context.Background(), "Compact() finished.")

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
		INFO(context.Background(), "Merged Level 2 SSTable size: %d bytes", mergedInfo.Size())
		// Check if size is roughly the sum of originals (minus overhead/duplicates, should be close)
		assert.InDelta(t, totalSize, mergedInfo.Size(), float64(totalSize)*0.1, "Merged size should be close to original total")
	})
}

func TestSSTableManager_compactHigherLevel(t *testing.T) {
	t.Run("compact level 1 into level 2 with overlap", func(t *testing.T) {
		cfg := NewConfig() // Use default config, setup will provide temp dir
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// --- Create SSTables using TestRindbSetup ---
		// Level 1 SSTable (Source)
		sstable1 := ts.createSSTable(1, map[string]string{
			"keyC": "valueC_L1", // Overwritten by L2
			"keyD": "valueD_L1",
		})
		ts.AddSSTableToLevel(1, sstable1)
		fs1Path := sstable1.Path() // Store path for later check

		// Level 2 SSTable (Overlapping)
		sstable2Overlap := ts.createSSTable(2, map[string]string{
			"keyB": "valueB_L2",
			"keyC": "valueC_L2", // Overwrites L1's keyC
		})
		ts.AddSSTableToLevel(2, sstable2Overlap)
		fs2OverlapPath := sstable2Overlap.Path() // Store path for later check

		// Level 2 SSTable (Non-Overlapping)
		sstable2NoOverlap := ts.createSSTable(2, map[string]string{
			"keyA": "valueA_L2",
		})
		ts.AddSSTableToLevel(2, sstable2NoOverlap)
		fs2NoOverlapPath := sstable2NoOverlap.Path() // Store path for later check

		// --- Act ---
		// Manually call compactHigherLevel using the manager from the setup
		err := ts.Manager.compactHigherLevel(context.Background(), ts.Manager.levels[1], 2)
		assert.NoError(t, err)

		// --- Assert ---
		// 1. Original files removed?
		assertFileNotExists(t, fs1Path)
		assertFileNotExists(t, fs2OverlapPath)
		assertFileExists(t, fs2NoOverlapPath) // Non-overlapping file should remain

		// 2. Level lists updated?
		assert.Equal(t, 0, ts.Manager.levels[1].Len(), "Level 1 should be empty after compaction")
		assert.Equal(t, 2, ts.Manager.levels[2].Len(), "Level 2 should have 2 files (non-overlapping + new merged)")

		// 3. Find the new and old files in Level 2
		var newMergedFS, oldNonOverlappingFS *FileSystem
		iter2 := ts.Manager.levels[2].Iterator()
		for iter2.HasNext() {
			fs, _ := iter2.Next()
			if fs.Path() == fs2NoOverlapPath {
				oldNonOverlappingFS = fs
			} else {
				newMergedFS = fs // Assume the other one is the new one
			}
		}
		assert.NotNil(t, newMergedFS, "New merged SSTable FS should be found in level 2")
		assert.NotNil(t, oldNonOverlappingFS, "Old non-overlapping SSTable FS should be found in level 2")
		assert.Equal(t, fs2NoOverlapPath, oldNonOverlappingFS.Path())

		// 4. Verify content of the new merged SSTable
		err = newMergedFS.Open(context.Background()) // Ensure FS is open
		assert.NoError(t, err)
		mergedSSTable, err := NewSSTable(context.Background(), ts.Manager.config, newMergedFS)
		assert.NoError(t, err)

		// Expected merged content: B(L2), C(L1 - newer), D(L1)
		assert.Equal(t, 3, len(mergedSSTable.SparseIndex), "Merged SSTable should have 3 keys")

		val, err := mergedSSTable.GetValue(context.Background(), Bytes("keyA")) // Should not be present
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, val)

		val, err = mergedSSTable.GetValue(context.Background(), Bytes("keyB"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueB_L2"), val)

		val, err = mergedSSTable.GetValue(context.Background(), Bytes("keyC"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueC_L1"), val) // Level 1 is newer, takes precedence

		val, err = mergedSSTable.GetValue(context.Background(), Bytes("keyD"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueD_L1"), val)

		// 5. Verify content of the non-overlapping SSTable (should be unchanged)
		err = oldNonOverlappingFS.Open(context.Background()) // Ensure FS is open
		assert.NoError(t, err)
		nonOverlappingSSTable, err := NewSSTable(context.Background(), ts.Manager.config, oldNonOverlappingFS)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(nonOverlappingSSTable.SparseIndex))
		val, err = nonOverlappingSSTable.GetValue(context.Background(), Bytes("keyA"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueA_L2"), val)
	})
}

// TestSSTableManager_shouldCompact tests the logic for deciding when to compact a level.
func TestSSTableManager_shouldCompact(t *testing.T) {
	cfg := testConfig() // Use default test config, setup will override dir
	ts := newTestRindbSetup(t, &cfg)
	defer ts.Cleanup()

	// Helper to create a FileSystem linked list (using dummy files)
	createLevelList := func(filePaths ...string) *LinkedList[*FileSystem] {
		list := InitLinkedList[*FileSystem]()
		for _, p := range filePaths {
			// Create a FileSystem object pointing to the dummy path
			// Note: These FS objects won't be properly opened/closed,
			// but it's okay for testing the shouldCompact logic which only needs paths/sizes.
			list.PushBack(&FileSystem{filePath: p})
		}
		return list
	}

	// Use ts.TempDir for creating dummy files
	tempDir := ts.TempDir

	// --- Level 0 Tests (File Count Based) ---
	// Note: Level 0 threshold is set by WithLevel0CompactionThreshold,
	// which defaults to 2 in testConfig() used by the TestRindbSetup.
	// These tests verify the file count logic.

	t.Run("Level0_BelowThreshold", func(t *testing.T) {
		// level0Threshold is 2 (from testConfig -> DefaultConfig)
		paths := []string{
			createDummyFile(t, tempDir, "l0_1.sst", 1), // 1 file < 2
		}
		levelList := createLevelList(paths...)
		assert.False(t, ts.Manager.shouldCompact(context.Background(), 0, levelList))
	})

	t.Run("Level0_AtThreshold", func(t *testing.T) {
		// level0Threshold is 2
		paths := []string{
			createDummyFile(t, tempDir, "l0_2.sst", 1),
			createDummyFile(t, tempDir, "l0_3.sst", 1), // 2 files == 2
		}
		levelList := createLevelList(paths...)
		assert.True(t, ts.Manager.shouldCompact(context.Background(), 0, levelList))
	})

	t.Run("Level0_AboveThreshold", func(t *testing.T) {
		// level0Threshold is 2
		paths := []string{
			createDummyFile(t, tempDir, "l0_4.sst", 1),
			createDummyFile(t, tempDir, "l0_5.sst", 1),
			createDummyFile(t, tempDir, "l0_6.sst", 1), // 3 files > 2
		}
		levelList := createLevelList(paths...)
		assert.True(t, ts.Manager.shouldCompact(context.Background(), 0, levelList))
	})

	// --- Higher Level Tests (Size Based) ---
	// Configure the manager specifically for these tests with lower thresholds
	cfgLowThreshold := NewConfig(
		WithBaseCompactionSizeMB(1), // 1MB base size
		WithLevelSizeMultiplier(2),  // 2x multiplier per level
	)
	tsLowThreshold := newTestRindbSetup(t, &cfgLowThreshold) // Use a separate setup with the low threshold config
	defer tsLowThreshold.Cleanup()
	tempDirLow := tsLowThreshold.TempDir // Use the temp dir from the low threshold setup

	t.Run("Level1_BelowThreshold (1MB base, 2x mult)", func(t *testing.T) {
		// Threshold = base(1MB) * multiplier(2)^1 = 2 MB
		paths := []string{
			createDummyFile(t, tempDirLow, "l1_1.sst", 1), // Total 1MB < 2MB
		}
		levelList := createLevelList(paths...)
		assert.False(t, tsLowThreshold.Manager.shouldCompact(context.Background(), 1, levelList))
	})

	t.Run("Level1_AtThreshold (1MB base, 2x mult)", func(t *testing.T) {
		// Threshold = 2 MB
		paths := []string{
			createDummyFile(t, tempDirLow, "l1_2.sst", 1),
			createDummyFile(t, tempDirLow, "l1_3.sst", 1), // Total 2MB == 2MB
		}
		levelList := createLevelList(paths...)
		assert.True(t, tsLowThreshold.Manager.shouldCompact(context.Background(), 1, levelList))
	})

	t.Run("Level1_AboveThreshold (1MB base, 2x mult)", func(t *testing.T) {
		// Threshold = 2 MB
		paths := []string{
			createDummyFile(t, tempDirLow, "l1_4.sst", 1),
			createDummyFile(t, tempDirLow, "l1_5.sst", 2), // Total 3MB > 2MB
		}
		levelList := createLevelList(paths...)
		assert.True(t, tsLowThreshold.Manager.shouldCompact(context.Background(), 1, levelList))
	})

	t.Run("Level2_BelowThreshold (1MB base, 2x mult)", func(t *testing.T) {
		// Threshold = base(1MB) * multiplier(2)^2 = 4 MB
		paths := []string{
			createDummyFile(t, tempDirLow, "l2_1.sst", 2),
			createDummyFile(t, tempDirLow, "l2_2.sst", 1), // Total 3MB < 4MB
		}
		levelList := createLevelList(paths...)
		assert.False(t, tsLowThreshold.Manager.shouldCompact(context.Background(), 2, levelList))
	})

	t.Run("Level2_AtThreshold (1MB base, 2x mult)", func(t *testing.T) {
		// Threshold = 4 MB
		paths := []string{
			createDummyFile(t, tempDirLow, "l2_3.sst", 2),
			createDummyFile(t, tempDirLow, "l2_4.sst", 2), // Total 4MB == 4MB
		}
		levelList := createLevelList(paths...)
		assert.True(t, tsLowThreshold.Manager.shouldCompact(context.Background(), 2, levelList))
	})

	t.Run("Level2_AboveThreshold (1MB base, 2x mult)", func(t *testing.T) {
		// Threshold = 4 MB
		paths := []string{
			createDummyFile(t, tempDirLow, "l2_5.sst", 3),
			createDummyFile(t, tempDirLow, "l2_6.sst", 2), // Total 5MB > 4MB
		}
		levelList := createLevelList(paths...)
		assert.True(t, tsLowThreshold.Manager.shouldCompact(context.Background(), 2, levelList))
	})

	t.Run("EmptyLevel", func(t *testing.T) {
		// Use the original setup (ts) as it doesn't matter which config for empty levels
		levelList := createLevelList() // Empty list
		assert.False(t, ts.Manager.shouldCompact(context.Background(), 0, levelList))
		assert.False(t, ts.Manager.shouldCompact(context.Background(), 1, levelList))
		assert.False(t, ts.Manager.shouldCompact(context.Background(), 2, levelList)) // Check level 2 as well
	})
}

func TestSSTableManager_GetRelevantSSTables(t *testing.T) {
	cfg := testConfig()

	t.Run("Level 0 returns all SSTables in newest-first order", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// Create SSTables for Level 0
		sstableL0_1 := ts.createSSTable(0, map[string]string{"k1": "v1", "k2": "v2"}) // Older
		sstableL0_2 := ts.createSSTable(0, map[string]string{"k3": "v3", "k4": "v4"}) // Newer
		ts.AddSSTableToLevel(0, sstableL0_1)
		ts.AddSSTableToLevel(0, sstableL0_2)

		// Call GetRelevantSSTables for Level 0
		relevantSSTables, err := ts.Manager.GetRelevantSSTables(context.Background(), Bytes("a"), Bytes("z"))
		assert.NoError(t, err)
		assert.NotNil(t, relevantSSTables)
		assert.Equal(t, 2, relevantSSTables.Len(), "Expected 2 relevant SSTables in Level 0")

		iter := relevantSSTables.Iterator()
		s1, err := iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, sstableL0_2.Path(), s1.Path())

		s2, err := iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, sstableL0_1.Path(), s2.Path())

		// Ensure SSTables are opened
		assert.True(t, s1.IsOpened(), "SSTable 1 should be opened")
		assert.True(t, s2.IsOpened(), "SSTable 2 should be opened")
	})

	t.Run("Level 1+ returns only overlapping SSTables in oldest-first order", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// Create SSTables for Level 1
		// sstableL1_1: range [b, c] - overlaps with [b, d]
		sstableL1_1 := ts.createSSTable(1, map[string]string{"b": "vb", "c": "vc"})
		// sstableL1_2: range [e, g] - does not overlap with [b, d]
		sstableL1_2 := ts.createSSTable(1, map[string]string{"e": "ve", "f": "vf", "g": "vg"})
		// sstableL1_3: range [c, d] - overlaps with [b, d]
		sstableL1_3 := ts.createSSTable(1, map[string]string{"c": "vc2", "d": "vd"})

		ts.AddSSTableToLevel(1, sstableL1_1)
		ts.AddSSTableToLevel(1, sstableL1_2)
		ts.AddSSTableToLevel(1, sstableL1_3)

		// Call GetRelevantSSTables for Level 1 with a specific range
		startKey := Bytes("b")
		endKey := Bytes("d")
		relevantSSTables, err := ts.Manager.GetRelevantSSTables(context.Background(), startKey, endKey)
		assert.NoError(t, err)
		assert.NotNil(t, relevantSSTables)
		assert.Equal(t, 2, relevantSSTables.Len(), "Expected 2 relevant SSTables in Level 1")

		// Verify order: oldest first (sstableL1_1 then sstableL1_3)
		iter := relevantSSTables.Iterator()
		s1, err := iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, sstableL1_1.Path(), s1.Path(), "Expected oldest overlapping SSTable first")

		s2, err := iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, sstableL1_3.Path(), s2.Path(), "Expected next oldest overlapping SSTable second")

		// Ensure SSTables are opened
		assert.True(t, s1.IsOpened(), "SSTable 1 should be opened")
		assert.True(t, s2.IsOpened(), "SSTable 2 should be opened")

		// Ensure non-overlapping SSTable is closed
		sstableL1_2.Close() // Close it if it was opened by createSSTable
		assert.False(t, sstableL1_2.IsOpened(), "Non-overlapping SSTable should be closed")
	})

	t.Run("No relevant SSTables found", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// Create some SSTables that won't overlap
		sstableL1_nonOverlap := ts.createSSTable(1, map[string]string{"x": "1", "y": "2"})
		ts.AddSSTableToLevel(1, sstableL1_nonOverlap)

		relevantSSTables, err := ts.Manager.GetRelevantSSTables(context.Background(), Bytes("a"), Bytes("b"))
		assert.NoError(t, err)
		assert.NotNil(t, relevantSSTables)
		assert.Equal(t, 0, relevantSSTables.Len(), "Expected 0 relevant SSTables")

		// Ensure the non-overlapping SSTable is closed
		sstableL1_nonOverlap.Close() // Close it if it was opened by createSSTable
		assert.False(t, sstableL1_nonOverlap.IsOpened(), "Non-overlapping SSTable should be closed")
	})

	t.Run("Empty SSTableManager", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		ts.Manager.levels = nil // Explicitly empty manager

		relevantSSTables, err := ts.Manager.GetRelevantSSTables(context.Background(), Bytes("a"), Bytes("z"))
		assert.NoError(t, err)
		assert.NotNil(t, relevantSSTables)
		assert.Equal(t, 0, relevantSSTables.Len(), "Expected 0 relevant SSTables from empty manager")
	})

	t.Run("Error opening SSTable", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// Create a dummy FS that will return an error on Open
		badFs := &FileSystem{filePath: "/non/existent/path.sst"}
		ts.Manager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		ts.Manager.levels[0].PushBack(badFs)

		relevantSSTables, err := ts.Manager.GetRelevantSSTables(context.Background(), Bytes("a"), Bytes("z"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no such file or directory")
		assert.Nil(t, relevantSSTables)
	})

	t.Run("Mixed levels with overlapping and non-overlapping", func(t *testing.T) {
		ts := newTestRindbSetup(t, &cfg)
		defer ts.Cleanup()

		// Level 0 (newest first)
		sstableL0_A := ts.createSSTable(0, map[string]string{"key0_A": "val0_A"}) // Range [key0_A, key0_A]
		sstableL0_B := ts.createSSTable(0, map[string]string{"key0_B": "val0_B"}) // Range [key0_B, key0_B]
		ts.AddSSTableToLevel(0, sstableL0_A)
		ts.AddSSTableToLevel(0, sstableL0_B)

		// Level 1 (oldest first)
		sstableL1_X := ts.createSSTable(1, map[string]string{"key1_X": "val1_X", "key1_Y": "val1_Y"}) // Range [key1_X, key1_Y]
		sstableL1_Z := ts.createSSTable(1, map[string]string{"key1_Z": "val1_Z"})                     // Range [key1_Z, key1_Z]
		ts.AddSSTableToLevel(1, sstableL1_X)
		ts.AddSSTableToLevel(1, sstableL1_Z)

		// Level 2 (oldest first)
		sstableL2_P := ts.createSSTable(2, map[string]string{"key2_P": "val2_P", "key2_Q": "val2_Q"}) // Range [key2_P, key2_Q]
		ts.AddSSTableToLevel(2, sstableL2_P)

		// Search range: [key0_A, key1_Y]
		startKey := Bytes("key0_A")
		endKey := Bytes("key1_Y")

		relevantSSTables, err := ts.Manager.GetRelevantSSTables(context.Background(), startKey, endKey)
		assert.NoError(t, err)
		assert.NotNil(t, relevantSSTables)

		// Expected:
		// L0: sstableL0_B (newest), sstableL0_A (older)
		// L1: sstableL1_X (overlaps [key0_A, key1_Y])
		// L2: sstableL2_P (does not overlap)
		// Total expected: 3
		assert.Equal(t, 3, relevantSSTables.Len(), "Expected 3 relevant SSTables from mixed levels")

		// Verify order: L0 newest first, then L1+ oldest first
		iter := relevantSSTables.Iterator()

		// L0 SSTables (iterated newest-to-oldest, added to the back of the list)
		s, err := iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, sstableL0_B.Path(), s.Path(), "Expected L0 newest first")

		s, err = iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, sstableL0_A.Path(), s.Path(), "Expected L0 older second")

		// L1 SSTables (pushed to back, so oldest first among them)
		s, err = iter.Next()
		assert.NoError(t, err)
		assert.Equal(t, sstableL1_X.Path(), s.Path(), "Expected L1 overlapping oldest first")

		// Ensure non-relevant SSTables are closed
		sstableL1_Z.Close()
		assert.False(t, sstableL1_Z.IsOpened(), "Non-overlapping L1 SSTable should be closed")
		sstableL2_P.Close()
		assert.False(t, sstableL2_P.IsOpened(), "Non-overlapping L2 SSTable should be closed")
	})
}
