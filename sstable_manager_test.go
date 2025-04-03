package rindb

import (
	"container/list"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSSTableManager_LoadLevels(t *testing.T) {
	cfg := testConfig()
	t.Run("SSTableManager::LoadLevels", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager(cfg)
		assert.NoError(t, err)
		assert.NoError(t, ssTableManager.Compact())
		defer ssTableManager.Close()
		for levelNumb, level := range ssTableManager.levels {
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
}

func TestSSTableManager_MergeSSTables(t *testing.T) {
	cfg := testConfig()
	t.Run("Merging sstables", func(t *testing.T) {
		ssTableManager := SSTableManager{openedFs: list.New(), config: cfg}
		defer ssTableManager.Close()

		// No initial content needed for these files
		fss, closer := initTempFileSystems(t, 4, nil)
		defer closer()

		sstables := make([]SStable, 0, 3)

		sstable1 := createSSTable(t, cfg, fss[0],
			[2]Bytes{Bytes("1"), Bytes("2")},
			[2]Bytes{Bytes("2"), Bytes("3")},
			[2]Bytes{Bytes("3"), Bytes("4")},
		)
		sstables = append(sstables, sstable1)

		sstable2 := createSSTable(t, cfg, fss[1],
			[2]Bytes{Bytes("1"), Bytes("3")},
			[2]Bytes{Bytes("2"), Bytes(nil)}, // Tombstone
			[2]Bytes{Bytes("4"), Bytes("5")},
		)
		sstables = append(sstables, sstable2)

		sstable3 := createSSTable(t, cfg, fss[2],
			[2]Bytes{Bytes("5"), Bytes("6")},
		)
		sstables = append(sstables, sstable3)

		fs := fss[3]
		newSSTable, err := mergeSSTables(cfg, fs, sstables)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(newSSTable.SparseIndex))

		sstableIterator, err := newSSTable.Iterator()
		assert.NoError(t, err)

		expectedRecords := []Record{
			RecordImpl{Key: Bytes("1"), Value: Bytes("3")},
			RecordImpl{Key: Bytes("2"), Value: nil}, // Tombstone
			RecordImpl{Key: Bytes("3"), Value: Bytes("4")},
			RecordImpl{Key: Bytes("4"), Value: Bytes("5")},
			RecordImpl{Key: Bytes("5"), Value: Bytes("6")},
		}
		assertIteratorRecords(t, sstableIterator, expectedRecords)
	})
}

func TestSSTableManager_SearchKey(t *testing.T) {
	cfg := testConfig()
	t.Run("Key in memtable, absent in SSTables", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager(cfg)
		assert.NoError(t, err)
		defer ssTableManager.Close()

		key := randStringBytes(10)

		result, err := ssTableManager.searchKey(key)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Key in level 0 only", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager(cfg)
		assert.NoError(t, err)
		defer ssTableManager.Close()

		fs, err := ssTableManager.NewSSTableFS(0)
		assert.NoError(t, err)
		defer fs.Close()

		key := Bytes("level0-key")
		value := Bytes("level0-value")
		_ = createSSTable(t, cfg, fs, [2]Bytes{key, value})

		if len(ssTableManager.levels) == 0 {
			// If the levels slice is empty, add a new list for level 0
			ssTableManager.levels = append(ssTableManager.levels, InitLinkedList[*FileSystem]())
		} else if ssTableManager.levels[0] == nil {
			// If the slice has space but level 0 is nil (less likely here, but good practice)
			ssTableManager.levels[0] = InitLinkedList[*FileSystem]()
		}
		ssTableManager.levels[0].PushBack(fs)

		result, err := ssTableManager.searchKey(key)
		assert.NoError(t, err)
		assert.Equal(t, value, result)
	})

	t.Run("Key in level 1 overridden by level 0", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager(cfg)
		assert.NoError(t, err)
		backupLevels := ssTableManager.levels
		defer func() {
			ssTableManager.levels = backupLevels
			ssTableManager.Close()
		}()

		// Level 1: older value
		fs1, err := ssTableManager.NewSSTableFS(1)
		assert.NoError(t, err)
		defer fs1.Close()
		key := randStringBytes(10)
		oldValue := Bytes("old-value")
		_ = createSSTable(t, cfg, fs1, [2]Bytes{key, oldValue})

		// Level 0: newer value
		fs0, err := ssTableManager.NewSSTableFS(0)
		assert.NoError(t, err)
		defer fs0.Close()
		newValue := Bytes("new-value")
		_ = createSSTable(t, cfg, fs0, [2]Bytes{key, newValue})

		// Backup old levels
		oldLevels := ssTableManager.levels

		// Override levels with new values
		ssTableManager.levels = []*LinkedList[*FileSystem]{
			InitLinkedList[*FileSystem](),
			InitLinkedList[*FileSystem](),
		}

		ssTableManager.levels[0].PushBack(fs0)
		ssTableManager.levels[1].PushBack(fs1)
		assert.Equal(t, 1, ssTableManager.levels[0].Len())
		assert.Equal(t, 1, ssTableManager.levels[1].Len())

		result, err := ssTableManager.searchKey(key)
		assert.NoError(t, err)
		assert.Equal(t, newValue, result)

		// Restore old levels
		ssTableManager.levels = oldLevels
	})

	t.Run("Key not found in any level", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager(cfg)
		assert.NoError(t, err)
		backupLevels := ssTableManager.levels
		defer func() {
			ssTableManager.levels = backupLevels
			ssTableManager.Close()
		}()

		fs, err := ssTableManager.NewSSTableFS(0)
		assert.NoError(t, err)
		defer fs.Close()
		_ = createSSTable(t, cfg, fs, [2]Bytes{Bytes("some-key"), Bytes("some-value")})

		// Override levels with new values
		ssTableManager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		ssTableManager.levels[0].PushBack(fs)

		missingKey := randStringBytes(10)
		result, err := ssTableManager.searchKey(missingKey)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Empty SSTableManager", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager(cfg)
		assert.NoError(t, err)
		defer ssTableManager.Close()

		ssTableManager.levels = nil // Explicitly empty

		result, err := ssTableManager.searchKey(Bytes("any-key"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Single SSTable in level 0", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager(cfg)
		assert.NoError(t, err)
		defer ssTableManager.Close()

		fs, err := ssTableManager.NewSSTableFS(0)
		assert.NoError(t, err)
		defer fs.Close()
		key := Bytes("single-key")
		value := Bytes("single-value")
		_ = createSSTable(t, cfg, fs, [2]Bytes{key, value})
		ssTableManager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		ssTableManager.levels[0].PushBack(fs)

		result, err := ssTableManager.searchKey(key)
		assert.NoError(t, err)
		assert.Equal(t, value, result)
	})

	t.Run("Tombstone in level 0 overrides level 1", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager(cfg)
		assert.NoError(t, err)
		defer ssTableManager.Close()

		// Level 1: original value
		fs1, err := ssTableManager.NewSSTableFS(1)
		assert.NoError(t, err)
		defer fs1.Close()
		key := Bytes("tombstone-key")
		value := Bytes("original-value")
		_ = createSSTable(t, cfg, fs1, [2]Bytes{key, value})

		// Level 0: tombstone
		fs0, err := ssTableManager.NewSSTableFS(0)
		assert.NoError(t, err)
		defer fs0.Close()
		_ = createSSTable(t, cfg, fs0, [2]Bytes{key, nil})

		ssTableManager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem](), InitLinkedList[*FileSystem]()}
		ssTableManager.levels[0].PushBack(fs0)
		ssTableManager.levels[1].PushBack(fs1)

		result, err := ssTableManager.searchKey(key)
		assert.NoError(t, err)
		assert.Nil(t, result) // Tombstone returns nil value
	})

	t.Run("Bloom filter skips irrelevant SSTables", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager(cfg)
		assert.NoError(t, err)
		defer ssTableManager.Close()

		fs, err := ssTableManager.NewSSTableFS(0)
		assert.NoError(t, err)
		defer fs.Close()
		_ = createSSTable(t, cfg, fs, [2]Bytes{Bytes("present-key"), Bytes("present-value")})
		ssTableManager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		ssTableManager.levels[0].PushBack(fs)

		// Key not in Bloom filter
		result, err := ssTableManager.searchKey(Bytes("absent-key"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})
}

func TestSSTableManager_CompactThreshold(t *testing.T) {
	t.Run("level 0 file count triggers compaction", func(t *testing.T) {
		tempDir := t.TempDir()
		cfg := NewConfig(
			WithDatabaseDir(tempDir),
			WithLevel0CompactionThreshold(4),
		)

		ssm, err := InitSSTableManager(cfg)
		assert.NoError(t, err)
		defer ssm.Close()

		// Initialize level 0 if needed
		if len(ssm.levels) == 0 {
			ssm.levels = append(ssm.levels, InitLinkedList[*FileSystem]())
		}

		// Create 4 SSTables in level 0
		for i := 0; i < 4; i++ {
			fs, err := ssm.NewSSTableFS(0)
			assert.NoError(t, err)
			_ = createSSTable(t, cfg, fs, [2]Bytes{Bytes(fmt.Sprintf("key%d", i)), Bytes("value")})
			ssm.levels[0].PushBack(fs)
		}

		assert.True(t, ssm.shouldCompact(0, ssm.levels[0]))
		assert.NoError(t, ssm.Compact())
		assert.Equal(t, 0, ssm.levels[0].Len(), "Level 0 should be empty after compaction")
		assert.GreaterOrEqual(t, len(ssm.levels), 2, "Should have created level 1")
		assert.Equal(t, 1, ssm.levels[1].Len(), "Level 1 should have merged SSTable")
	})

	t.Run("levels below threshold dont compact", func(t *testing.T) {
		tempDir := t.TempDir()
		threshold := 4
		cfg := NewConfig(
			WithDatabaseDir(tempDir),
			WithLevel0CompactionThreshold(threshold),
		)

		ssm, err := InitSSTableManager(cfg)
		assert.NoError(t, err)
		defer ssm.Close()

		// Ensure levels 0 and 1 exist for the test setup
		if len(ssm.levels) < 2 {
			ssm.levels = append(ssm.levels, make([]*LinkedList[*FileSystem], 2-len(ssm.levels))...)
		}
		if ssm.levels[0] == nil {
			ssm.levels[0] = InitLinkedList[*FileSystem]()
		}
		if ssm.levels[1] == nil {
			ssm.levels[1] = InitLinkedList[*FileSystem]()
		}

		// --- Test Level 0 ---
		// Add files less than threshold
		level0FileCount := threshold - 1
		initialLevel0Files := make([]*FileSystem, level0FileCount)
		for i := 0; i < level0FileCount; i++ {
			fs, err := ssm.NewSSTableFS(0)
			assert.NoError(t, err)
			_ = createSSTable(t, cfg, fs, [2]Bytes{Bytes(fmt.Sprintf("l0-key%d", i)), Bytes("value")})
			ssm.levels[0].PushBack(fs)
			initialLevel0Files[i] = fs // Keep track for assertion
		}
		assert.Equal(t, level0FileCount, ssm.levels[0].Len(), "Pre-check: Level 0 should have %d files", level0FileCount)

		// --- Test Level 1 ---
		// Add a small file (guaranteed below size threshold)
		level1FileCount := 1
		initialLevel1Files := make([]*FileSystem, level1FileCount)
		fs1, err := ssm.NewSSTableFS(1)
		assert.NoError(t, err)
		_ = createSSTable(t, cfg, fs1, [2]Bytes{Bytes("l1-key"), Bytes("small-value")})
		ssm.levels[1].PushBack(fs1)
		initialLevel1Files[0] = fs1
		assert.Equal(t, level1FileCount, ssm.levels[1].Len(), "Pre-check: Level 1 should have %d file", level1FileCount)

		// --- Act ---
		err = ssm.Compact()
		assert.NoError(t, err)

		// --- Assert ---
		// Level 0 should be unchanged
		assert.Equal(t, level0FileCount, ssm.levels[0].Len(), "Level 0 count should remain %d after compact", level0FileCount)
		// Verify the actual files are the same (optional, but good sanity check)
		currentLevel0Files := make([]*FileSystem, 0, ssm.levels[0].Len())
		iter0 := ssm.levels[0].Iterator()
		for iter0.HasNext() {
			f, _ := iter0.Next()
			currentLevel0Files = append(currentLevel0Files, f)
		}
		assert.ElementsMatch(t, initialLevel0Files, currentLevel0Files, "Level 0 files should be the same instances")

		// Level 1 should be unchanged
		assert.Equal(t, level1FileCount, ssm.levels[1].Len(), "Level 1 count should remain %d after compact", level1FileCount)
		// Verify the actual files are the same
		currentLevel1Files := make([]*FileSystem, 0, ssm.levels[1].Len())
		iter1 := ssm.levels[1].Iterator()
		for iter1.HasNext() {
			f, _ := iter1.Next()
			currentLevel1Files = append(currentLevel1Files, f)
		}
		assert.ElementsMatch(t, initialLevel1Files, currentLevel1Files, "Level 1 files should be the same instances")

		// No higher levels should have been created
		assert.LessOrEqual(t, len(ssm.levels), 2, "No new levels should be created")
	})

	// New test case using configurable thresholds
	t.Run("level 1 size above threshold triggers compaction (lowered threshold)", func(t *testing.T) {
		tempDir := t.TempDir()
		// Configure low thresholds for easy testing
		// Level 1 threshold = 1MB * (2^1) = 2MB
		cfg := NewConfig(
			WithDatabaseDir(tempDir),
			WithLevel0CompactionThreshold(2), // Low L0 threshold (doesn't affect this test directly)
			WithBaseCompactionSizeMB(1),      // Low base size: 1MB
			WithLevelSizeMultiplier(2),       // Low multiplier: 2x per level
		)

		ssm, err := InitSSTableManager(cfg)
		assert.NoError(t, err)
		defer ssm.Close()

		// Ensure levels 0 and 1 exist
		if len(ssm.levels) < 2 {
			ssm.levels = append(ssm.levels, make([]*LinkedList[*FileSystem], 2-len(ssm.levels))...)
		}
		if ssm.levels[0] == nil {
			ssm.levels[0] = InitLinkedList[*FileSystem]()
		}
		if ssm.levels[1] == nil {
			ssm.levels[1] = InitLinkedList[*FileSystem]()
		}

		numFiles := 2
		// Need > 2MB total. Estimate ~1050 bytes/record (10b key, 1k val, 16b meta).
		// 2MB / 1050 bytes/rec ≈ 1950 records. Let's use 1000 per file (total ~2.1MB).
		recordsPerFile := 1000
		valueSize := 1024 // 1KB values

		level1Paths := make([]string, numFiles)
		var totalSize int64

		INFO("Creating SSTables for Level 1 (target > 2MB total)...")
		for i := 0; i < numFiles; i++ {
			fs, err := ssm.NewSSTableFS(1) // Create in level 1
			assert.NoError(t, err)
			level1Paths[i] = fs.Path()

			pairs := generateKeyValuePairs(recordsPerFile, 10, valueSize)
			_ = createSSTable(t, cfg, fs, pairs...)

			info, statErr := os.Stat(fs.Path())
			assert.NoError(t, statErr)
			totalSize += info.Size()
			INFO("Created Level 1 SSTable %s, size: %d bytes", fs.Path(), info.Size())

			ssm.levels[1].PushBack(fs)
			err = fs.Close() // Close after flush, Compact will reopen if needed
			assert.NoError(t, err)
		}

		// Calculate the threshold used in this test
		level1ThresholdBytes := int64(cfg.baseCompactionSizeMB) * int64(math.Pow(float64(cfg.levelSizeMultiplier), 1.0)) * 1024 * 1024
		INFO("Total size of Level 1 files: %d bytes (%.2f MB). Threshold: %d bytes (%.2f MB)",
			totalSize, float64(totalSize)/(1024*1024),
			level1ThresholdBytes, float64(level1ThresholdBytes)/(1024*1024))

		// Sanity check: Ensure total size exceeds the *lowered* threshold
		assert.Greater(t, totalSize, level1ThresholdBytes, "Total size should exceed the lowered threshold")
		assert.Equal(t, numFiles, ssm.levels[1].Len(), "Pre-check: Level 1 should have %d files", numFiles)

		// Act: Call Compact(). This should trigger compaction for Level 1 based on size.
		INFO("Calling Compact()...")
		err = ssm.Compact()
		assert.NoError(t, err)
		INFO("Compact() finished.")

		// Assert:
		// 1. Level 1 should now be empty.
		assert.Equal(t, 0, ssm.levels[1].Len(), "Level 1 should be empty after compaction")

		// 2. Level 2 should exist and contain exactly one merged SSTable.
		assert.GreaterOrEqual(t, len(ssm.levels), 3, "Should have created level 2")
		assert.NotNil(t, ssm.levels[2], "Level 2 list should exist")
		assert.Equal(t, 1, ssm.levels[2].Len(), "Level 2 should have 1 merged SSTable")

		// 3. Check if the original Level 1 files were removed.
		for _, p := range level1Paths {
			_, err = os.Stat(p)
			assert.True(t, os.IsNotExist(err), "Original file %s should have been removed", p)
		}

		// 4. (Optional) Verify the content/size of the merged Level 2 SSTable
		iter2 := ssm.levels[2].Iterator()
		mergedFs, err := iter2.Next()
		assert.NoError(t, err)
		mergedInfo, err := os.Stat(mergedFs.Path())
		assert.NoError(t, err)
		INFO("Merged Level 2 SSTable size: %d bytes", mergedInfo.Size())
		// Check if size is roughly the sum of originals (minus overhead/duplicates, should be close)
		assert.InDelta(t, totalSize, mergedInfo.Size(), float64(totalSize)*0.1, "Merged size should be close to original total")
	})
}

func TestSSTableManager_compactHigherLevel(t *testing.T) {
	t.Run("compact level 1 into level 2 with overlap", func(t *testing.T) {
		tempDir := t.TempDir()
		cfg := NewConfig(WithDatabaseDir(tempDir))

		ssm, err := InitSSTableManager(cfg)
		assert.NoError(t, err)
		defer ssm.Close()

		// Ensure levels 1 and 2 exist
		if len(ssm.levels) < 3 {
			ssm.levels = append(ssm.levels, make([]*LinkedList[*FileSystem], 3-len(ssm.levels))...)
		}
		if ssm.levels[1] == nil {
			ssm.levels[1] = InitLinkedList[*FileSystem]()
		}
		if ssm.levels[2] == nil {
			ssm.levels[2] = InitLinkedList[*FileSystem]()
		}

		// --- Create SSTables ---
		// Level 1 SSTable (Source)
		fs1, err := ssm.NewSSTableFS(1)
		assert.NoError(t, err)
		_ = createSSTable(t, cfg, fs1,
			[2]Bytes{Bytes("keyC"), Bytes("valueC_L1")}, // Overwritten by L2
			[2]Bytes{Bytes("keyD"), Bytes("valueD_L1")},
		)
		ssm.levels[1].PushBack(fs1)
		fs1Path := fs1.Path() // Store path for later check

		// Level 2 SSTable (Overlapping)
		fs2Overlap, err := ssm.NewSSTableFS(2)
		assert.NoError(t, err)
		_ = createSSTable(t, cfg, fs2Overlap,
			[2]Bytes{Bytes("keyB"), Bytes("valueB_L2")},
			[2]Bytes{Bytes("keyC"), Bytes("valueC_L2")}, // Overwrites L1's keyC
		)
		ssm.levels[2].PushBack(fs2Overlap)
		fs2OverlapPath := fs2Overlap.Path() // Store path for later check

		// Level 2 SSTable (Non-Overlapping)
		fs2NoOverlap, err := ssm.NewSSTableFS(2)
		assert.NoError(t, err)
		_ = createSSTable(t, cfg, fs2NoOverlap,
			[2]Bytes{Bytes("keyA"), Bytes("valueA_L2")},
		)
		ssm.levels[2].PushBack(fs2NoOverlap)
		fs2NoOverlapPath := fs2NoOverlap.Path() // Store path for later check

		// --- Act ---
		// Manually call compactHigherLevel (Compact() would normally pick the SSTable)
		err = ssm.compactHigherLevel(ssm.levels[1], 2)
		assert.NoError(t, err)

		// --- Assert ---
		// 1. Original files removed?
		_, err = os.Stat(fs1Path)
		assert.True(t, os.IsNotExist(err), "Level 1 source SSTable file should be removed")
		_, err = os.Stat(fs2OverlapPath)
		assert.True(t, os.IsNotExist(err), "Level 2 overlapping SSTable file should be removed")
		_, err = os.Stat(fs2NoOverlapPath)
		assert.NoError(t, err, "Level 2 non-overlapping SSTable file should still exist")

		// 2. Level lists updated?
		assert.Equal(t, 0, ssm.levels[1].Len(), "Level 1 should be empty after compaction")
		assert.Equal(t, 2, ssm.levels[2].Len(), "Level 2 should have 2 files (non-overlapping + new merged)")

		// 3. Find the new and old files in Level 2
		var newMergedFS, oldNonOverlappingFS *FileSystem
		iter2 := ssm.levels[2].Iterator()
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
		err = newMergedFS.Open()
		assert.NoError(t, err)
		defer newMergedFS.Close()
		mergedSSTable, err := NewSSTable(cfg, newMergedFS)
		assert.NoError(t, err)

		// Expected merged content: B(L2), C(L2), D(L1)
		assert.Equal(t, 3, len(mergedSSTable.SparseIndex), "Merged SSTable should have 3 keys")

		val, err := mergedSSTable.GetValue(Bytes("keyA")) // Should not be present
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, val)

		val, err = mergedSSTable.GetValue(Bytes("keyB"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueB_L2"), val)

		val, err = mergedSSTable.GetValue(Bytes("keyC"))
		assert.NoError(t, err)
		// assert.Equal(t, Bytes("valueC_L2"), val) // L2 value takes precedence
		assert.Equal(t, Bytes("valueC_L1"), val) // Level 1 is newer

		val, err = mergedSSTable.GetValue(Bytes("keyD"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueD_L1"), val)

		// 5. Verify content of the non-overlapping SSTable (should be unchanged)
		err = oldNonOverlappingFS.Open()
		assert.NoError(t, err)
		defer oldNonOverlappingFS.Close()
		nonOverlappingSSTable, err := NewSSTable(cfg, oldNonOverlappingFS)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(nonOverlappingSSTable.SparseIndex))
		val, err = nonOverlappingSSTable.GetValue(Bytes("keyA"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueA_L2"), val)
	})
}

// TestSSTableManager_shouldCompact tests the logic for deciding when to compact a level.
func TestSSTableManager_shouldCompact(t *testing.T) {
	cfg := testConfig()
	// Use t.TempDir() for automatic cleanup
	tempDir := t.TempDir()
	cfg.databaseDir = tempDir // Override databaseDir for this test

	manager, err := InitSSTableManager(cfg)
	assert.NoError(t, err)

	// Helper to create a FileSystem linked list
	createLevelList := func(filePaths ...string) *LinkedList[*FileSystem] {
		list := InitLinkedList[*FileSystem]()
		for _, p := range filePaths {
			list.PushBack(&FileSystem{filePath: p})
		}
		return list
	}

	t.Run("Level0_BelowThreshold", func(t *testing.T) {
		// level0Threshold is 4
		paths := []string{
			createDummyFile(t, tempDir, "l0_1.sst", 1),
		}
		levelList := createLevelList(paths...)
		assert.False(t, manager.shouldCompact(0, levelList))
	})

	t.Run("Level0_AtThreshold", func(t *testing.T) {
		// level0CompactionThreshold is 2 (from testConfig)
		paths := []string{
			createDummyFile(t, tempDir, "l0_2.sst", 1), // Renamed for clarity
			createDummyFile(t, tempDir, "l0_3.sst", 1), // Renamed for clarity
			createDummyFile(t, tempDir, "l0_6.sst", 1),
			createDummyFile(t, tempDir, "l0_7.sst", 1),
		}
		levelList := createLevelList(paths...)
		assert.True(t, manager.shouldCompact(0, levelList))
	})

	t.Run("Level0_AboveThreshold", func(t *testing.T) {
		// level0CompactionThreshold is 2 (from testConfig)
		paths := []string{
			createDummyFile(t, tempDir, "l0_4.sst", 1), // Renamed for clarity
			createDummyFile(t, tempDir, "l0_5.sst", 1), // Renamed for clarity
			createDummyFile(t, tempDir, "l0_6.sst", 1), // Renamed for clarity
			createDummyFile(t, tempDir, "l0_11.sst", 1),
			createDummyFile(t, tempDir, "l0_12.sst", 1),
		}
		levelList := createLevelList(paths...)
		assert.True(t, manager.shouldCompact(0, levelList))
	})

	t.Run("Level1_BelowThreshold", func(t *testing.T) {
		// Threshold = 10 * 10^1 = 100 MB
		paths := []string{
			createDummyFile(t, tempDir, "l1_1.sst", 50),
			createDummyFile(t, tempDir, "l1_2.sst", 49), // Total 99MB
		}
		levelList := createLevelList(paths...)
		assert.False(t, manager.shouldCompact(1, levelList))
	})

	t.Run("Level1_AtThreshold", func(t *testing.T) {
		// Threshold = 10 * 10^1 = 100 MB
		paths := []string{
			createDummyFile(t, tempDir, "l1_3.sst", 50),
			createDummyFile(t, tempDir, "l1_4.sst", 50), // Total 100MB
		}
		levelList := createLevelList(paths...)
		assert.True(t, manager.shouldCompact(1, levelList))
	})

	t.Run("Level1_AboveThreshold", func(t *testing.T) {
		// Threshold = 10 * 10^1 = 100 MB
		paths := []string{
			createDummyFile(t, tempDir, "l1_5.sst", 50),
			createDummyFile(t, tempDir, "l1_6.sst", 51), // Total 101MB
		}
		levelList := createLevelList(paths...)
		assert.True(t, manager.shouldCompact(1, levelList))
	})

	t.Run("Level2_BelowThreshold", func(t *testing.T) {
		// Threshold = 10 * 10^2 = 1000 MB
		paths := []string{
			createDummyFile(t, tempDir, "l2_1.sst", 500),
			createDummyFile(t, tempDir, "l2_2.sst", 499), // Total 999MB
		}
		levelList := createLevelList(paths...)
		assert.False(t, manager.shouldCompact(2, levelList))
	})

	t.Run("Level2_AtThreshold", func(t *testing.T) {
		// Threshold = 10 * 10^2 = 1000 MB
		paths := []string{
			createDummyFile(t, tempDir, "l2_3.sst", 500),
			createDummyFile(t, tempDir, "l2_4.sst", 500), // Total 1000MB
		}
		levelList := createLevelList(paths...)
		assert.True(t, manager.shouldCompact(2, levelList))
	})

	t.Run("EmptyLevel", func(t *testing.T) {
		levelList := createLevelList() // Empty list
		assert.False(t, manager.shouldCompact(0, levelList))
		assert.False(t, manager.shouldCompact(1, levelList))
	})
}
