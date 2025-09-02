package rindb

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type failingManifestWriter struct{}

func (f failingManifestWriter) Append(VersionEdit) error { return fmt.Errorf("append fail") }
func (f failingManifestWriter) Sync() error              { return nil }
func (f failingManifestWriter) Close() error             { return nil }

func TestSSTableManager_LoadLevels(t *testing.T) {
	cfg := testConfig()
	t.Run("LoadLevels validates file names", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()
		assert.NoError(t, ts.Manager.Compact(ctx))
		for _, level := range ts.Manager.levels {
			iterator := level.Iterator()
			for iterator.HasNext() {
				fs, err := iterator.Next()
				assert.NoError(t, err)
				fileName := path.Base(fs.Path())
				assert.True(t, strings.HasSuffix(fileName, ".sst"),
					"expected filename '%s' to end with '.sst'", fileName)
				base := strings.TrimSuffix(fileName, ".sst")
				_, err = strconv.ParseUint(base, 10, 64)
				assert.NoError(t, err)
			}
		}
	})

	t.Run("Key in older SSTable requires Prev() iteration", func(t *testing.T) {
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
	})
}

func TestInitSSTableManagerRepairMode(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Create two SSTable files on disk.
	createDummyFile(t, dir, sstPath(1), 1)
	createDummyFile(t, dir, sstPath(2), 1)

	vs := &VersionSet{Levels: [][]FileMeta{{{Number: 1, Level: 0}}}}

	cfg := testConfig()
	cfg.databaseDir = dir

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
		sm, err := InitSSTableManager(ctx, cfg, &VersionSet{}, nil)
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
			ts.Manager.versionSet.Levels = [][]FileMeta{{}}
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

func TestSSTableManager_findOverlappingSSTables(t *testing.T) {
	cfg := testConfig()

	t.Run("Level does not exist", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		sources := []SStable{*ts.createSSTable(0, map[string]string{"g": "1", "k": "2"})}
		res, err := ts.Manager.findOverlappingSSTables(ctx, len(ts.Manager.levels), sources)
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("Level is nil", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		ts.Manager.levels[1] = nil
		sources := []SStable{*ts.createSSTable(0, map[string]string{"g": "1", "k": "2"})}
		res, err := ts.Manager.findOverlappingSSTables(ctx, 1, sources)
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("Level empty", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		res, err := ts.Manager.findOverlappingSSTables(ctx, 2, []SStable{*ts.createSSTable(0, map[string]string{"g": "1", "k": "2"})})
		assert.NoError(t, err)
		assert.Len(t, res, 0)
	})

	t.Run("Return only overlapping SSTables", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// Source SSTable with range [g, k]
		source := ts.createSSTable(0, map[string]string{"g": "1", "k": "2"})

		// Level 1 SSTables
		before := ts.createSSTable(1, map[string]string{"a": "1", "d": "2"})
		overlap := ts.createSSTable(1, map[string]string{"j": "1", "m": "2"})
		after := ts.createSSTable(1, map[string]string{"x": "1", "z": "2"})
		ts.AddSSTableToLevel(1, before)
		ts.AddSSTableToLevel(1, overlap)
		ts.AddSSTableToLevel(1, after)

		res, err := ts.Manager.findOverlappingSSTables(ctx, 1, []SStable{*source})
		assert.NoError(t, err)
		assert.Len(t, res, 1)
		assert.Equal(t, overlap.Path(), res[0].Path())
	})

	t.Run("No overlapping SSTables", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		source := ts.createSSTable(0, map[string]string{"g": "1", "k": "2"})
		before := ts.createSSTable(1, map[string]string{"a": "1", "d": "2"})
		after := ts.createSSTable(1, map[string]string{"x": "1", "z": "2"})
		ts.AddSSTableToLevel(1, before)
		ts.AddSSTableToLevel(1, after)

		res, err := ts.Manager.findOverlappingSSTables(ctx, 1, []SStable{*source})
		assert.NoError(t, err)
		assert.Len(t, res, 0)
	})

	t.Run("Error opening SSTable and resource cleanup", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		source := ts.createSSTable(0, map[string]string{"g": "1", "k": "2"})
		goodFs := ts.createSSTable(1, map[string]string{"j": "1", "m": "2"})
		ts.AddSSTableToLevel(1, goodFs)

		badFs := &FileSystem{filePath: "/non/existent/path.sst"}
		ts.Manager.levels[1].PushBack(badFs)

		initial := len(ts.Manager.openedFs)
		_, err := ts.Manager.findOverlappingSSTables(ctx, 1, []SStable{*source})
		assert.Error(t, err)
		assert.False(t, goodFs.IsOpened(), "file system for successfully opened sstable should be closed on subsequent error")
		assert.Equal(t, initial-1, len(ts.Manager.openedFs), "openedFs should exclude closed sstables after cleanup")
	})

	t.Run("Empty sources", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		overlap := ts.createSSTable(1, map[string]string{"a": "1", "d": "2"})
		ts.AddSSTableToLevel(1, overlap)

		res, err := ts.Manager.findOverlappingSSTables(ctx, 1, nil)
		assert.NoError(t, err)
		assert.Len(t, res, 0)
	})

	t.Run("With empty and non-empty source sstables", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		emptyFs := ts.newSSTableFS(0)
		emptySource := SStable{FileSystem: emptyFs}
		nonEmptySource := ts.createSSTable(0, map[string]string{"g": "1", "k": "2"})

		overlap := ts.createSSTable(1, map[string]string{"j": "1", "m": "2"})
		ts.AddSSTableToLevel(1, overlap)

		sources := []SStable{emptySource, *nonEmptySource}
		res, err := ts.Manager.findOverlappingSSTables(ctx, 1, sources)
		assert.NoError(t, err)
		assert.Len(t, res, 1)
		assert.Equal(t, overlap.Path(), res[0].Path())
	})
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

		assert.True(t, ts.Manager.shouldCompact(ctx, 0, ts.Manager.levels[0]))
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

func TestSSTableManager_compactLevel0(t *testing.T) {
	t.Run("compacts level 0 into new level when next level missing", func(t *testing.T) {
		ctx := context.Background()
		cfg := NewConfig()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// Drop levels beyond level 0 to simulate missing next level
		ts.Manager.levels = ts.Manager.levels[:1]
		ts.Levels = ts.Manager.levels

		// Create two level 0 SSTables with overlapping keys and increasing sequence numbers
		sst1 := ts.createSSTableWithSequence(0, map[string]string{"keyA": "valueA1"}, 1)
		ts.AddSSTableToLevel(0, sst1)
		sst2 := ts.createSSTableWithSequence(0, map[string]string{"keyA": "valueA2", "keyB": "valueB2"}, 10)
		ts.AddSSTableToLevel(0, sst2)
		path1, path2 := sst1.Path(), sst2.Path()

		err := ts.Manager.compactLevel0(ctx, ts.Manager.levels[0], 1)
		assert.NoError(t, err)

		assert.Equal(t, 0, ts.Manager.levels[0].Len(), "Level 0 should be empty after compaction")
		assert.GreaterOrEqual(t, len(ts.Manager.levels), 2)
		assert.Equal(t, 1, ts.Manager.levels[1].Len(), "Level 1 should contain merged SSTable")

		assertFileNotExists(t, path1)
		assertFileNotExists(t, path2)

		iter := ts.Manager.levels[1].Iterator()
		mergedFS, err := iter.Next()
		assert.NoError(t, err)
		assert.NoError(t, mergedFS.Open(ctx))
		merged, err := NewSSTable(ctx, cfg, mergedFS)
		assert.NoError(t, err)

		val, err := merged.GetValue(ctx, Bytes("keyA"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueA2"), val)
		val, err = merged.GetValue(ctx, Bytes("keyB"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueB2"), val)
	})

	t.Run("compacts level 0 and merges overlapping SSTables from level 1", func(t *testing.T) {
		ctx := context.Background()
		cfg := NewConfig()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// Level 0 SSTables with increasing sequence numbers
		l0a := ts.createSSTableWithSequence(0, map[string]string{"keyA": "valueA1"}, 1)
		ts.AddSSTableToLevel(0, l0a)
		l0b := ts.createSSTableWithSequence(0, map[string]string{"keyA": "valueA2", "keyB": "valueB2"}, 10)
		ts.AddSSTableToLevel(0, l0b)
		l0aPath, l0bPath := l0a.Path(), l0b.Path()

		// Level 1 SSTables (older)
		l1Overlap := ts.createSSTableWithSequence(1, map[string]string{"keyA": "valueA_L1", "keyB": "valueB_L1", "keyC": "valueC_L1"}, 0)
		ts.AddSSTableToLevel(1, l1Overlap)
		l1Non := ts.createSSTableWithSequence(1, map[string]string{"keyD": "valueD_L1"}, 0)
		ts.AddSSTableToLevel(1, l1Non)
		l1OverlapPath, l1NonPath := l1Overlap.Path(), l1Non.Path()

		err := ts.Manager.compactLevel0(ctx, ts.Manager.levels[0], 1)
		assert.NoError(t, err)

		assert.Equal(t, 0, ts.Manager.levels[0].Len())
		assert.Equal(t, 2, ts.Manager.levels[1].Len())

		assertFileNotExists(t, l0aPath)
		assertFileNotExists(t, l0bPath)
		assertFileNotExists(t, l1OverlapPath)
		assertFileExists(t, l1NonPath)

		// Identify files in level 1
		var (
			mergedFS        *FileSystem
			oldNonOverlapFS *FileSystem
		)
		it := ts.Manager.levels[1].Iterator()
		for it.HasNext() {
			fs, err := it.Next()
			assert.NoError(t, err)
			switch fs.Path() {
			case l1NonPath:
				oldNonOverlapFS = fs
			default:
				// The other file must be the newly merged one.
				mergedFS = fs
			}
		}
		assert.NotNil(t, mergedFS)
		assert.NotNil(t, oldNonOverlapFS)

		assert.NoError(t, mergedFS.Open(ctx))
		merged, err := NewSSTable(ctx, cfg, mergedFS)
		assert.NoError(t, err)

		val, err := merged.GetValue(ctx, Bytes("keyA"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueA2"), val)
		val, err = merged.GetValue(ctx, Bytes("keyB"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueB2"), val)
		val, err = merged.GetValue(ctx, Bytes("keyC"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueC_L1"), val)
		_, err = merged.GetValue(ctx, Bytes("keyD"))
		assert.ErrorIs(t, err, ErrKeyNotFound)

		// Ensure non-overlapping SSTable remains intact
		assert.NoError(t, oldNonOverlapFS.Open(ctx))
		remain, err := NewSSTable(ctx, cfg, oldNonOverlapFS)
		assert.NoError(t, err)
		val, err = remain.GetValue(ctx, Bytes("keyD"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueD_L1"), val)

		// Overlapping SSTable should be removed from disk
		assertFileNotExists(t, l1OverlapPath)
	})

	t.Run("returns error when a level 0 SSTable cannot be opened", func(t *testing.T) {
		ctx := context.Background()
		cfg := NewConfig()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		badFS := &FileSystem{filePath: ts.TempDir}
		ts.Manager.levels[0].PushBack(badFS)

		err := ts.Manager.compactLevel0(ctx, ts.Manager.levels[0], 1)
		assert.Error(t, err)
	})

	t.Run("returns error when overlapping SSTable cannot be opened", func(t *testing.T) {
		ctx := context.Background()
		cfg := NewConfig()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		l0 := ts.createSSTable(0, map[string]string{"key": "val"})
		ts.AddSSTableToLevel(0, l0)

		badFS := &FileSystem{filePath: ts.TempDir}
		ts.Manager.levels[1].PushBack(badFS)

		err := ts.Manager.compactLevel0(ctx, ts.Manager.levels[0], 1)
		assert.Error(t, err)
	})

	t.Run("returns error when new SSTable cannot be created", func(t *testing.T) {
		ctx := context.Background()
		cfg := NewConfig()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		l0 := ts.createSSTable(0, map[string]string{"key": "val"})
		ts.AddSSTableToLevel(0, l0)

		ts.Manager.config.databaseDir = path.Join(ts.TempDir, "missing", "dir")

		err := ts.Manager.compactLevel0(ctx, ts.Manager.levels[0], 1)
		assert.Error(t, err)
	})
}

func TestSSTableManager_compactHigherLevel(t *testing.T) {
	t.Run("compact level 1 into level 2 with overlap", func(t *testing.T) {
		ctx := context.Background()
		cfg := NewConfig() // Use default config, setup will provide temp dir
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// --- Create SSTables using TestRindbSetup ---
		// Level 1 SSTable (Source, newer)
		sstable1 := ts.createSSTableWithSequence(1, map[string]string{
			"keyC": "valueC_L1", // Overwritten by L2
			"keyD": "valueD_L1",
		}, 10)
		ts.AddSSTableToLevel(1, sstable1)
		fs1Path := sstable1.Path() // Store path for later check

		// Level 2 SSTable (Overlapping, older)
		sstable2Overlap := ts.createSSTableWithSequence(2, map[string]string{
			"keyB": "valueB_L2",
			"keyC": "valueC_L2", // Overwrites L1's keyC
		}, 1)
		ts.AddSSTableToLevel(2, sstable2Overlap)
		fs2OverlapPath := sstable2Overlap.Path() // Store path for later check

		// Level 2 SSTable (Non-Overlapping, older)
		sstable2NoOverlap := ts.createSSTableWithSequence(2, map[string]string{
			"keyA": "valueA_L2",
		}, 1)
		ts.AddSSTableToLevel(2, sstable2NoOverlap)
		fs2NoOverlapPath := sstable2NoOverlap.Path() // Store path for later check

		// --- Act ---
		// Manually call compactHigherLevel using the manager from the setup
		err := ts.Manager.compactHigherLevel(ctx, ts.Manager.levels[1], 2)
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
			fs, err := iter2.Next()
			assert.NoError(t, err)
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
		err = newMergedFS.Open(ctx) // Ensure FS is open
		assert.NoError(t, err)
		mergedSSTable, err := NewSSTable(ctx, cfg, newMergedFS)
		assert.NoError(t, err)

		// Expected merged content: B(L2), C(L1 - newer), D(L1)
		assert.Equal(t, 3, len(mergedSSTable.SparseIndex), "Merged SSTable should have 3 keys")

		val, err := mergedSSTable.GetValue(ctx, Bytes("keyA")) // Should not be present
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, val)

		val, err = mergedSSTable.GetValue(ctx, Bytes("keyB"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueB_L2"), val)

		val, err = mergedSSTable.GetValue(ctx, Bytes("keyC"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueC_L1"), val) // Level 1 is newer, takes precedence

		val, err = mergedSSTable.GetValue(ctx, Bytes("keyD"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueD_L1"), val)

		// 5. Verify content of the non-overlapping SSTable (should be unchanged)
		err = oldNonOverlappingFS.Open(ctx) // Ensure FS is open
		assert.NoError(t, err)
		nonOverlappingSSTable, err := NewSSTable(ctx, cfg, oldNonOverlappingFS)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(nonOverlappingSSTable.SparseIndex))
		val, err = nonOverlappingSSTable.GetValue(ctx, Bytes("keyA"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueA_L2"), val)
	})

	t.Run("compact level 1 into level 2 without overlap", func(t *testing.T) {
		ctx := context.Background()
		cfg := NewConfig()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// Source SSTable in level 1
		sstable1 := ts.createSSTable(1, map[string]string{
			"keyE": "valueE_L1",
			"keyF": "valueF_L1",
		})
		ts.AddSSTableToLevel(1, sstable1)
		fs1Path := sstable1.Path()

		// Non-overlapping SSTable in level 2
		sstable2 := ts.createSSTable(2, map[string]string{
			"keyA": "valueA_L2",
		})
		ts.AddSSTableToLevel(2, sstable2)
		fs2Path := sstable2.Path()

		err := ts.Manager.compactHigherLevel(ctx, ts.Manager.levels[1], 2)
		assert.NoError(t, err)

		// Original level1 file removed, level2 file remains
		assertFileNotExists(t, fs1Path)
		assertFileExists(t, fs2Path)

		assert.Equal(t, 0, ts.Manager.levels[1].Len())
		assert.Equal(t, 2, ts.Manager.levels[2].Len())

		// Identify new merged file
		var newMergedFS *FileSystem
		iter2 := ts.Manager.levels[2].Iterator()
		for iter2.HasNext() {
			fs, err := iter2.Next()
			assert.NoError(t, err)
			if fs.Path() != fs2Path {
				newMergedFS = fs
			}
		}
		assert.NotNil(t, newMergedFS)

		err = newMergedFS.Open(ctx)
		assert.NoError(t, err)
		mergedSSTable, err := NewSSTable(ctx, cfg, newMergedFS)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(mergedSSTable.SparseIndex))

		val, err := mergedSSTable.GetValue(ctx, Bytes("keyE"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueE_L1"), val)

		val, err = mergedSSTable.GetValue(ctx, Bytes("keyF"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("valueF_L1"), val)
	})

	t.Run("returns error when source SSTable cannot be opened", func(t *testing.T) {
		ctx := context.Background()
		cfg := NewConfig()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		badFS := &FileSystem{filePath: ts.TempDir}
		ts.Manager.levels[1].PushBack(badFS)

		err := ts.Manager.compactHigherLevel(ctx, ts.Manager.levels[1], 2)
		assert.Error(t, err)
	})

	t.Run("returns error when overlapping SSTable cannot be opened", func(t *testing.T) {
		ctx := context.Background()
		cfg := NewConfig()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		sstable1 := ts.createSSTable(1, map[string]string{"key": "val"})
		ts.AddSSTableToLevel(1, sstable1)

		badFS := &FileSystem{filePath: ts.TempDir}
		ts.Manager.levels[2].PushBack(badFS)

		err := ts.Manager.compactHigherLevel(ctx, ts.Manager.levels[1], 2)
		assert.Error(t, err)
	})

	t.Run("returns error when new SSTable cannot be created", func(t *testing.T) {
		ctx := context.Background()
		cfg := NewConfig()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		sstable1 := ts.createSSTable(1, map[string]string{"key": "val"})
		ts.AddSSTableToLevel(1, sstable1)

		ts.Manager.config.databaseDir = path.Join(ts.TempDir, "missing", "dir")

		err := ts.Manager.compactHigherLevel(ctx, ts.Manager.levels[1], 2)
		assert.Error(t, err)
	})
}

// TestSSTableManager_shouldCompact tests the logic for deciding when to compact a level.
func TestSSTableManager_shouldCompact(t *testing.T) {
	cfg := testConfig() // Use default test config, setup will override dir
	ctx := context.Background()
	ts := newTestRindbSetup(t, ctx, &cfg)
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
			createDummyFile(t, tempDir, sstPath(1), 1), // 1 file < 2
		}
		levelList := createLevelList(paths...)
		assert.False(t, ts.Manager.shouldCompact(context.Background(), 0, levelList))
	})

	t.Run("Level0_AtThreshold", func(t *testing.T) {
		// level0Threshold is 2
		paths := []string{
			createDummyFile(t, tempDir, sstPath(2), 1),
			createDummyFile(t, tempDir, sstPath(3), 1), // 2 files == 2
		}
		levelList := createLevelList(paths...)
		assert.True(t, ts.Manager.shouldCompact(context.Background(), 0, levelList))
	})

	t.Run("Level0_AboveThreshold", func(t *testing.T) {
		// level0Threshold is 2
		paths := []string{
			createDummyFile(t, tempDir, sstPath(4), 1),
			createDummyFile(t, tempDir, sstPath(5), 1),
			createDummyFile(t, tempDir, sstPath(6), 1), // 3 files > 2
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
	tsLowThreshold := newTestRindbSetup(t, ctx, &cfgLowThreshold) // Use a separate setup with the low threshold config
	defer tsLowThreshold.Cleanup()
	tempDirLow := tsLowThreshold.TempDir // Use the temp dir from the low threshold setup

	t.Run("Level1_BelowThreshold (1MB base, 2x mult)", func(t *testing.T) {
		// Threshold = base(1MB) * multiplier(2)^1 = 2 MB
		paths := []string{
			createDummyFile(t, tempDirLow, sstPath(1), 1), // Total 1MB < 2MB
		}
		levelList := createLevelList(paths...)
		assert.False(t, tsLowThreshold.Manager.shouldCompact(context.Background(), 1, levelList))
	})

	t.Run("Level1_AtThreshold (1MB base, 2x mult)", func(t *testing.T) {
		// Threshold = 2 MB
		paths := []string{
			createDummyFile(t, tempDirLow, sstPath(2), 1),
			createDummyFile(t, tempDirLow, sstPath(3), 1), // Total 2MB == 2MB
		}
		levelList := createLevelList(paths...)
		assert.True(t, tsLowThreshold.Manager.shouldCompact(context.Background(), 1, levelList))
	})

	t.Run("Level1_AboveThreshold (1MB base, 2x mult)", func(t *testing.T) {
		// Threshold = 2 MB
		paths := []string{
			createDummyFile(t, tempDirLow, sstPath(4), 1),
			createDummyFile(t, tempDirLow, sstPath(5), 2), // Total 3MB > 2MB
		}
		levelList := createLevelList(paths...)
		assert.True(t, tsLowThreshold.Manager.shouldCompact(context.Background(), 1, levelList))
	})

	t.Run("Level2_BelowThreshold (1MB base, 2x mult)", func(t *testing.T) {
		// Threshold = base(1MB) * multiplier(2)^2 = 4 MB
		paths := []string{
			createDummyFile(t, tempDirLow, sstPath(6), 2),
			createDummyFile(t, tempDirLow, sstPath(7), 1), // Total 3MB < 4MB
		}
		levelList := createLevelList(paths...)
		assert.False(t, tsLowThreshold.Manager.shouldCompact(context.Background(), 2, levelList))
	})

	t.Run("Level2_AtThreshold (1MB base, 2x mult)", func(t *testing.T) {
		// Threshold = 4 MB
		paths := []string{
			createDummyFile(t, tempDirLow, sstPath(8), 2),
			createDummyFile(t, tempDirLow, sstPath(9), 2), // Total 4MB == 4MB
		}
		levelList := createLevelList(paths...)
		assert.True(t, tsLowThreshold.Manager.shouldCompact(context.Background(), 2, levelList))
	})

	t.Run("Level2_AboveThreshold (1MB base, 2x mult)", func(t *testing.T) {
		// Threshold = 4 MB
		paths := []string{
			createDummyFile(t, tempDirLow, sstPath(10), 3),
			createDummyFile(t, tempDirLow, sstPath(11), 2), // Total 5MB > 4MB
		}
		levelList := createLevelList(paths...)
		assert.True(t, tsLowThreshold.Manager.shouldCompact(context.Background(), 2, levelList))
	})

	t.Run("EmptyLevel", func(t *testing.T) {
		ctx := context.Background()
		// Use the original setup (ts) as it doesn't matter which config for empty levels
		levelList := createLevelList() // Empty list
		assert.False(t, ts.Manager.shouldCompact(ctx, 0, levelList))
		assert.False(t, ts.Manager.shouldCompact(ctx, 1, levelList))
		assert.False(t, ts.Manager.shouldCompact(ctx, 2, levelList)) // Check level 2 as well
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

			assert.Equal(t, tc.expect, sm.shouldCompact(ctx, 0, sm.levels[0]))
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

func TestSSTableManager_mergeSSTables(t *testing.T) {
	cfg := testConfig()

	t.Run("creates new level and removes sources", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		ts.Manager.levels = ts.Manager.levels[:1]

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

		_, err = os.Stat(sst1.Path())
		assert.NoError(t, err)
		_, err = os.Stat(sst2.Path())
		assert.NoError(t, err)

		ts.Manager.mu.Lock()
		err = ts.Manager.mergeSSTables(ctx, 1, []SStable{sst1, sst2})
		ts.Manager.mu.Unlock()
		assert.NoError(t, err)

		assert.GreaterOrEqual(t, len(ts.Manager.levels), 2)
		assert.Equal(t, 1, ts.Manager.levels[1].Len())

		// version set should only contain the new file at level 1
		nums := make([]uint64, 0)
		for _, fm := range ts.Manager.versionSet.Levels[1] {
			nums = append(nums, fm.Number)
		}
		assert.Len(t, nums, 1)

		fs, err := ts.Manager.levels[1].Iterator().Next()
		assert.NoError(t, err)
		merged, err := ts.Manager.openAndLoadSSTable(ctx, fs)
		assert.NoError(t, err)

		v, err := merged.GetValue(ctx, Bytes("a"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("1"), v)

		v, err = merged.GetValue(ctx, Bytes("b"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, v)

		v, err = merged.GetValue(ctx, Bytes("c"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("3"), v)

		_, err = os.Stat(sst1.Path())
		assert.Error(t, err)
		_, err = os.Stat(sst2.Path())
		assert.Error(t, err)
	})

	t.Run("retains sources on manifest failure", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		mem1 := InitMemtable(*ts.Config)
		mem1.Put(newRecord(Bytes("a"), Bytes("1"), 1))
		sst1, _, err := flush(ctx, *ts.Config, mem1, ts.newSSTableFS(0))
		assert.NoError(t, err)

		mem2 := InitMemtable(*ts.Config)
		mem2.Put(newRecord(Bytes("b"), Bytes("2"), 2))
		sst2, _, err := flush(ctx, *ts.Config, mem2, ts.newSSTableFS(0))
		assert.NoError(t, err)

		ts.Manager.manifest = failingManifestWriter{}

		ts.Manager.mu.Lock()
		err = ts.Manager.mergeSSTables(ctx, 1, []SStable{sst1, sst2})
		ts.Manager.mu.Unlock()
		assert.Error(t, err)

		_, err = os.Stat(sst1.Path())
		assert.NoError(t, err)
		_, err = os.Stat(sst2.Path())
		assert.NoError(t, err)
	})

	t.Run("keeps tombstone with active snapshot", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		ts.Manager.minSnapshotSeq = 2

		mem1 := InitMemtable(*ts.Config)
		mem1.Put(newRecord(Bytes("b"), Bytes("1"), 1))
		sst1, _, err := flush(ctx, *ts.Config, mem1, ts.newSSTableFS(0))
		assert.NoError(t, err)

		mem2 := InitMemtable(*ts.Config)
		mem2.Put(newRecord(Bytes("b"), nil, 3))
		sst2, _, err := flush(ctx, *ts.Config, mem2, ts.newSSTableFS(0))
		assert.NoError(t, err)

		ts.AddSSTableToLevel(0, &sst1)
		ts.AddSSTableToLevel(0, &sst2)
		ts.Manager.levels[0] = InitLinkedList[*FileSystem]()

		ts.Manager.mu.Lock()
		err = ts.Manager.mergeSSTables(ctx, 1, []SStable{sst1, sst2})
		ts.Manager.mu.Unlock()
		assert.NoError(t, err)

		fs, err := ts.Manager.levels[1].Iterator().Next()
		assert.NoError(t, err)
		merged, err := ts.Manager.openAndLoadSSTable(ctx, fs)
		assert.NoError(t, err)

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
		assert.Equal(t, uint64(1), recs[1].GetSequenceNumber())
		assert.Equal(t, TypeValue, recs[1].GetType())
	})

	t.Run("no-op on empty sources", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		err := ts.Manager.mergeSSTables(ctx, 1, []SStable{})
		assert.NoError(t, err)
	})
}
