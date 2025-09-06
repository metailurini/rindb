package rindb

import (
	"context"
	"errors"
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

type failingManifest struct{}

func (failingManifest) Append(versionEdit) error { return errors.New("append fail") }
func (failingManifest) Sync() error              { return nil }
func (failingManifest) Close() error             { return nil }
func (failingManifest) Path() string             { return "" }

func TestSSTableManager_openAndLoadSSTable(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	mgr := &ssTableManager{config: cfg}

	t.Run("loads valid sstable", func(t *testing.T) {
		dir := t.TempDir()
		fs, err := OpenFS(ctx, filepath.Join(dir, sstPath(1)))
		require.NoError(t, err)
		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("k"), Bytes("v"), 1))
		sst, _, err := flush(ctx, cfg, mem, fs)
		require.NoError(t, err)
		require.NoError(t, sst.Close())

		fsToLoad := &FileSystem{filePath: sst.Path()}
		loaded, err := mgr.openAndLoadSSTable(ctx, fsToLoad)
		require.NoError(t, err)
		require.NotNil(t, loaded)
		assert.True(t, loaded.IsOpened())
		val, err := loaded.GetValue(ctx, Bytes("k"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("v"), val)
		require.NoError(t, loaded.Close())
	})

	t.Run("fs open failure", func(t *testing.T) {
		fs := &FileSystem{filePath: filepath.Join(t.TempDir(), "no", "dir", sstPath(2))}
		sst, err := mgr.openAndLoadSSTable(ctx, fs)
		assert.Nil(t, sst)
		assert.Error(t, err)
		assert.False(t, fs.IsOpened())
	})

	t.Run("sstable creation failure closes fs", func(t *testing.T) {
		dir := t.TempDir()
		badPath := filepath.Join(dir, sstPath(3))
		require.NoError(t, os.WriteFile(badPath, []byte("bad"), 0o644))
		fs := &FileSystem{filePath: badPath}
		sst, err := mgr.openAndLoadSSTable(ctx, fs)
		assert.Nil(t, sst)
		assert.Error(t, err)
		assert.False(t, fs.IsOpened())
	})
}

func TestSSTableManager_SearchKeyPrevIteration(t *testing.T) {
	cfg := testConfig()
	ctx := context.Background()
	ts := newTestRindbSetup(t, ctx, &cfg)
	defer ts.Cleanup()

	// Create SSTables
	older := ts.createSSTable(0, map[string]string{"targetKey": "targetVal"})
	newer := ts.createSSTable(0, map[string]string{"otherKey": "otherVal"})

	// Add to level 0 (older first, then newer)
	ts.AddSSTable(0, older)
	ts.AddSSTable(0, newer)

	// Verify search finds the key in older SSTable
	result, err := ts.Manager.SearchKey(ctx, Bytes("targetKey"))
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

	vs := &versionSet{Levels: [][]fileMeta{{{Number: 1, Level: 0}}}}

	t.Run("normal startup uses manifest", func(t *testing.T) {
		sm, err := InitSSTableManager(ctx, cfg, vs, nil)
		assert.NoError(t, err)
		defer sm.Close(ctx)

		if assert.Len(t, sm.versionSet.Levels, 1) {
			var nums []uint64
			for _, fm := range sm.versionSet.Levels[0] {
				nums = append(nums, fm.Number)
			}
			assert.Equal(t, []uint64{1}, nums)
		}
	})

	t.Run("repair mode scans directory", func(t *testing.T) {
		cfg.repairMode = true
		sm, err := InitSSTableManager(ctx, cfg, nil, nil)
		assert.NoError(t, err)
		defer sm.Close(ctx)

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
		ts.AddSSTable(0, sstable1)

		sstable2 := ts.createSSTableWithSequence(0, map[string]string{
			"1": "3", // Overrides sstable1
			"2": "",  // Tombstone overrides sstable1
			"4": "5",
		}, 10)
		ts.AddSSTable(0, sstable2)

		sstable3 := ts.createSSTableWithSequence(0, map[string]string{
			"5": "6",
		}, 20)
		ts.AddSSTable(0, sstable3)

		assert.Equal(t, 3, len(ts.Manager.versionSet.Levels[0]))

		err := ts.Manager.Compact(ctx)
		assert.NoError(t, err)

		assert.Equal(t, 0, len(ts.Manager.versionSet.Levels[0]))
		if assert.GreaterOrEqual(t, len(ts.Manager.versionSet.Levels), 2) {
			assert.Equal(t, 1, len(ts.Manager.versionSet.Levels[1]))
		}

		meta := ts.Manager.versionSet.Levels[1][0]
		fs, err := OpenExistingFS(ctx, path.Join(ts.Manager.config.databaseDir, sstPath(meta.Number)))
		assert.NoError(t, err)
		defer func() { _ = fs.Close() }()
		mergedSSTable, err := NewSSTable(ctx, cfg, fs)
		assert.NoError(t, err)

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

		assert.NotNil(t, ts.Manager, "Manager should not be nil")
		if assert.NotNil(t, ts.Manager.versionSet, "Version set should not be nil") {
			if assert.GreaterOrEqual(t, len(ts.Manager.versionSet.Levels), 1) {
				assert.Len(t, ts.Manager.versionSet.Levels[0], 0)
			}
		}

		// Search for a random key in an empty manager
		key := randStringBytes(10)
		result, err := ts.Manager.SearchKey(ctx, key)

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
		ts.AddSSTable(0, sst)

		result, err := ts.Manager.SearchKey(ctx, key)
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
		ts.AddSSTable(1, sst1)

		newValue := Bytes("new-value")
		sst0 := ts.createSSTable(0, map[string]string{string(key): string(newValue)})
		ts.AddSSTable(0, sst0)

		result, err := ts.Manager.SearchKey(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, newValue, result)
	})

	t.Run("Key not found in any level", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		sst := ts.createSSTable(0, map[string]string{"some-key": "some-value"})
		ts.AddSSTable(0, sst)

		missingKey := randStringBytes(10)
		result, err := ts.Manager.SearchKey(ctx, missingKey)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Empty SSTableManager", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		ts.Manager.versionSet.Levels = nil

		result, err := ts.Manager.SearchKey(ctx, Bytes("any-key"))
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
		ts.AddSSTable(0, sst)

		result, err := ts.Manager.SearchKey(ctx, key)
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
		ts.AddSSTable(1, sst1)

		fs0 := ts.newSSTableFS(0)
		sst0 := createSSTable(t, cfg, fs0, [2]Bytes{key, nil})
		ts.AddSSTable(0, &sst0)

		result, err := ts.Manager.SearchKey(ctx, key)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Bloom filter skips irrelevant SSTables", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		sst := ts.createSSTable(0, map[string]string{"present-key": "present-value"})
		ts.AddSSTable(0, sst)

		result, err := ts.Manager.SearchKey(ctx, Bytes("absent-key"))
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
		ts.AddSSTable(0, sst)

		path := sst.Path()
		err := os.Remove(path)
		assert.NoError(t, err)

		result, err := ts.Manager.SearchKey(ctx, key)
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
			ts.AddSSTable(0, sstable) // Register the created SSTable
		}

		assert.True(t, ts.Manager.shouldCompact(ctx, 0, ts.Manager.versionSet.Levels[0]))
		assert.NoError(t, ts.Manager.Compact(ctx))
		assert.Equal(t, 0, len(ts.Manager.versionSet.Levels[0]))
		if assert.GreaterOrEqual(t, len(ts.Manager.versionSet.Levels), 2) {
			assert.Equal(t, 1, len(ts.Manager.versionSet.Levels[1]))
		}
	})

	t.Run("levels below threshold dont compact", func(t *testing.T) {
		threshold := 4
		cfg := NewConfig(WithLevel0CompactionThreshold(threshold))
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// --- Test Level 0 ---
		level0FileCount := threshold - 1
		initialLevel0Nums := make([]uint64, 0, level0FileCount)
		for i := 0; i < level0FileCount; i++ {
			sstable := ts.createSSTable(0, map[string]string{fmt.Sprintf("l0-key%d", i): "value"})
			ts.AddSSTable(0, sstable)
			num, err := fileNum(sstable.Path())
			require.NoError(t, err)
			initialLevel0Nums = append(initialLevel0Nums, num)
		}
		assert.Equal(t, level0FileCount, len(ts.Manager.versionSet.Levels[0]))

		// --- Test Level 1 ---
		level1FileCount := 1
		initialLevel1Nums := make([]uint64, level1FileCount)
		sstable1 := ts.createSSTable(1, map[string]string{"l1-key": "small-value"})
		ts.AddSSTable(1, sstable1)
		num1, err := fileNum(sstable1.Path())
		require.NoError(t, err)
		initialLevel1Nums[0] = num1
		assert.Equal(t, level1FileCount, len(ts.Manager.versionSet.Levels[1]))

		// --- Act ---
		err = ts.Manager.Compact(ctx)
		assert.NoError(t, err)

		// --- Assert ---
		// Level 0 should be unchanged
		assert.Equal(t, level0FileCount, len(ts.Manager.versionSet.Levels[0]))
		currentLevel0Nums := make([]uint64, 0, len(ts.Manager.versionSet.Levels[0]))
		for _, fm := range ts.Manager.versionSet.Levels[0] {
			currentLevel0Nums = append(currentLevel0Nums, fm.Number)
		}
		assert.ElementsMatch(t, initialLevel0Nums, currentLevel0Nums)

		assert.Equal(t, level1FileCount, len(ts.Manager.versionSet.Levels[1]))
		currentLevel1Nums := make([]uint64, 0, len(ts.Manager.versionSet.Levels[1]))
		for _, fm := range ts.Manager.versionSet.Levels[1] {
			currentLevel1Nums = append(currentLevel1Nums, fm.Number)
		}
		assert.ElementsMatch(t, initialLevel1Nums, currentLevel1Nums)

		assert.LessOrEqual(t, len(ts.Manager.versionSet.Levels), 3)
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

		info(ctx, "Creating SSTables for Level 1 (target > 2MB total)...")
		for i := 0; i < numFiles; i++ {
			// Use ts.CreateSSTable which uses the manager's config and FS creation
			kvs := make(map[string]string)
			pairs := generateKeyValuePairs(recordsPerFile, 10, valueSize)
			for _, p := range pairs {
				kvs[string(p[0])] = string(p[1])
			}
			sstable := ts.createSSTable(1, kvs) // Create in level 1
			ts.AddSSTable(1, sstable)

			level1Paths[i] = sstable.Path()
			fi, statErr := os.Stat(sstable.Path())
			assert.NoError(t, statErr)
			totalSize += fi.Size()
			info(ctx, "Created Level 1 SSTable %s, size: %d bytes", sstable.Path(), fi.Size())
		}

		// Calculate the threshold used in this test
		level1ThresholdBytes := int64(cfg.baseCompactionSizeMB) * int64(math.Pow(float64(cfg.levelSizeMultiplier), 1.0)) * 1024 * 1024
		info(ctx, "Total size of Level 1 files: %d bytes (%.2f MB). Threshold: %d bytes (%.2f MB)",
			totalSize, float64(totalSize)/(1024*1024),
			level1ThresholdBytes, float64(level1ThresholdBytes)/(1024*1024))

		assert.Greater(t, totalSize, level1ThresholdBytes, "Total size should exceed the lowered threshold")
		assert.Equal(t, numFiles, len(ts.Manager.versionSet.Levels[1]))

		info(ctx, "Calling Compact()...")
		err := ts.Manager.Compact(ctx)
		assert.NoError(t, err)
		info(ctx, "Compact() finished.")

		assert.Equal(t, 0, len(ts.Manager.versionSet.Levels[1]))
		if assert.GreaterOrEqual(t, len(ts.Manager.versionSet.Levels), 3) {
			assert.Equal(t, 1, len(ts.Manager.versionSet.Levels[2]))
		}

		// 2. Level 2 should exist and contain exactly one merged SSTable.
		// 3. Check if the original Level 1 files were removed.
		for _, p := range level1Paths {
			assertFileNotExists(t, p)
		}

		meta := ts.Manager.versionSet.Levels[2][0]
		mergedInfo, err := os.Stat(path.Join(ts.Manager.config.databaseDir, sstPath(meta.Number)))
		assert.NoError(t, err)
		info(ctx, "Merged Level 2 SSTable size: %d bytes", mergedInfo.Size())
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
		ts.AddSSTable(0, older)
		ts.AddSSTable(0, newer)

		nOlder, err := fileNum(older.Path())
		require.NoError(t, err)
		nNewer, err := fileNum(newer.Path())
		require.NoError(t, err)

		ssts, err := ts.Manager.GetRelevantSSTables(ctx, Bytes("a"), Bytes("z"))
		require.NoError(t, err)
		nums := make([]uint64, len(ssts))
		for i, h := range ssts {
			nums[i], err = fileNum(h.Table.Path())
			require.NoError(t, err)
			h.Unref()
		}
		assert.Equal(t, []uint64{nNewer, nOlder}, nums)
	})

	t.Run("Level 1+ returns only overlapping SSTables in oldest-first order", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		older := ts.createSSTable(1, map[string]string{"b": "1"})
		newer := ts.createSSTable(1, map[string]string{"c": "2"})
		ts.AddSSTable(1, older)
		ts.AddSSTable(1, newer)

		nOlder, err := fileNum(older.Path())
		require.NoError(t, err)
		nNewer, err := fileNum(newer.Path())
		require.NoError(t, err)

		ssts, err := ts.Manager.GetRelevantSSTables(ctx, Bytes("b"), Bytes("d"))
		require.NoError(t, err)
		nums := make([]uint64, len(ssts))
		for i, h := range ssts {
			nums[i], err = fileNum(h.Table.Path())
			require.NoError(t, err)
			h.Unref()
		}
		assert.Equal(t, []uint64{nOlder, nNewer}, nums)
	})

	t.Run("No relevant SSTables found", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		sst := ts.createSSTable(1, map[string]string{"x": "1"})
		ts.AddSSTable(1, sst)

		ssts, err := ts.Manager.GetRelevantSSTables(ctx, Bytes("a"), Bytes("b"))
		require.NoError(t, err)
		assert.Len(t, ssts, 0)
	})

	t.Run("Error opening SSTable", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		sst := ts.createSSTable(1, map[string]string{"b": "1"})
		ts.AddSSTable(1, sst)
		assert.NoError(t, os.Remove(sst.Path()))

		ssts, err := ts.Manager.GetRelevantSSTables(ctx, Bytes("a"), Bytes("z"))
		require.ErrorIs(t, err, os.ErrNotExist)
		require.Nil(t, ssts)
	})

	t.Run("Mixed levels with overlapping and non-overlapping", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// Level 0
		l0a := ts.createSSTable(0, map[string]string{"a": "1"}) // older
		l0b := ts.createSSTable(0, map[string]string{"b": "2"}) // newer
		ts.AddSSTable(0, l0a)
		ts.AddSSTable(0, l0b)

		// Level 1
		l1Overlap := ts.createSSTable(1, map[string]string{"b": "1", "c": "2"})
		l1Non := ts.createSSTable(1, map[string]string{"x": "1"})
		ts.AddSSTable(1, l1Overlap)
		ts.AddSSTable(1, l1Non)

		nL0b, err := fileNum(l0b.Path())
		require.NoError(t, err)
		nL0a, err := fileNum(l0a.Path())
		require.NoError(t, err)
		nL1, err := fileNum(l1Overlap.Path())
		require.NoError(t, err)

		ssts, err := ts.Manager.GetRelevantSSTables(ctx, Bytes("a"), Bytes("d"))
		require.NoError(t, err)
		nums := make([]uint64, len(ssts))
		for i, h := range ssts {
			nums[i], err = fileNum(h.Table.Path())
			require.NoError(t, err)
			h.Unref()
		}
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

			sm := &ssTableManager{
				versionSet:     &versionSet{Levels: [][]fileMeta{{{Number: 1, Level: 0}}}},
				config:         cfg,
				now:            func() time.Time { return current },
				diskSampler:    func() (uint64, error) { return ioVal, nil },
				minSnapshotSeq: math.MaxUint64,
			}

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

func TestSSTableManager_shouldCompact(t *testing.T) {
	ctx := context.Background()
	mb := uint64(1 << 20)

	t.Run("Level0 below threshold", func(t *testing.T) {
		cfg := NewConfig(WithLevel0CompactionThreshold(4))
		sm := &ssTableManager{config: cfg}
		files := make([]fileMeta, 3)
		assert.False(t, sm.shouldCompact(ctx, 0, files))
	})

	t.Run("Level0 at threshold", func(t *testing.T) {
		cfg := NewConfig(WithLevel0CompactionThreshold(4))
		sm := &ssTableManager{config: cfg}
		files := make([]fileMeta, 4)
		assert.True(t, sm.shouldCompact(ctx, 0, files))
	})

	t.Run("Level0 above threshold", func(t *testing.T) {
		cfg := NewConfig(WithLevel0CompactionThreshold(4))
		sm := &ssTableManager{config: cfg}
		files := make([]fileMeta, 5)
		assert.True(t, sm.shouldCompact(ctx, 0, files))
	})

	t.Run("Level1 below threshold (1MB base, 2x mult)", func(t *testing.T) {
		cfg := NewConfig(
			WithBaseCompactionSizeMB(1),
			WithLevelSizeMultiplier(2),
		)
		sm := &ssTableManager{config: cfg}
		files := []fileMeta{{Size: mb}}
		assert.False(t, sm.shouldCompact(ctx, 1, files))
	})

	t.Run("Level1 at threshold (1MB base, 2x mult)", func(t *testing.T) {
		cfg := NewConfig(
			WithBaseCompactionSizeMB(1),
			WithLevelSizeMultiplier(2),
		)
		sm := &ssTableManager{config: cfg}
		files := []fileMeta{{Size: 2 * mb}}
		assert.True(t, sm.shouldCompact(ctx, 1, files))
	})

	t.Run("Level1 above threshold (1MB base, 2x mult)", func(t *testing.T) {
		cfg := NewConfig(
			WithBaseCompactionSizeMB(1),
			WithLevelSizeMultiplier(2),
		)
		sm := &ssTableManager{config: cfg}
		files := []fileMeta{{Size: 2*mb + 1}}
		assert.True(t, sm.shouldCompact(ctx, 1, files))
	})

	t.Run("Level2 below threshold (1MB base, 2x mult)", func(t *testing.T) {
		cfg := NewConfig(
			WithBaseCompactionSizeMB(1),
			WithLevelSizeMultiplier(2),
		)
		sm := &ssTableManager{config: cfg}
		files := []fileMeta{{Size: 3 * mb}}
		assert.False(t, sm.shouldCompact(ctx, 2, files))
	})

	t.Run("Level2 at threshold (1MB base, 2x mult)", func(t *testing.T) {
		cfg := NewConfig(
			WithBaseCompactionSizeMB(1),
			WithLevelSizeMultiplier(2),
		)
		sm := &ssTableManager{config: cfg}
		files := []fileMeta{{Size: 4 * mb}}
		assert.True(t, sm.shouldCompact(ctx, 2, files))
	})

	t.Run("Level2 above threshold (1MB base, 2x mult)", func(t *testing.T) {
		cfg := NewConfig(
			WithBaseCompactionSizeMB(1),
			WithLevelSizeMultiplier(2),
		)
		sm := &ssTableManager{config: cfg}
		files := []fileMeta{{Size: 4*mb + 1}}
		assert.True(t, sm.shouldCompact(ctx, 2, files))
	})

	t.Run("empty level", func(t *testing.T) {
		cfg := NewConfig()
		sm := &ssTableManager{config: cfg}
		assert.False(t, sm.shouldCompact(ctx, 0, nil))
	})
}

func TestSSTableManager_IOLoadSampler(t *testing.T) {
	t.Run("records io load", func(t *testing.T) {
		ctx := context.Background()
		cfg := testConfig()
		cfg.databaseDir = t.TempDir()
		sm, err := InitSSTableManager(ctx, cfg, &versionSet{}, nil)
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
		sm, err := InitSSTableManager(ctx, cfg, &versionSet{}, nil)
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

		ts.AddSSTable(0, &sst1)
		ts.AddSSTable(0, &sst2)

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

func TestSSTableManager_compactLevel0(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()

	t.Run("compacts level 0 into new level when next level missing", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		l0a := ts.createSSTable(0, map[string]string{"a": "1"})
		l0b := ts.createSSTable(0, map[string]string{"b": "2"})
		ts.AddSSTable(0, l0a)
		ts.AddSSTable(0, l0b)
		require.NoError(t, l0a.Close())
		require.NoError(t, l0b.Close())

		ts.Manager.versionSet.Levels = ts.Manager.versionSet.Levels[:1]

		picked := append([]fileMeta(nil), ts.Manager.versionSet.Levels[0]...)
		err := ts.Manager.mergeIntoLevel(ctx, 1, picked)
		require.NoError(t, err)

		require.Len(t, ts.Manager.versionSet.Levels, 2)
		assert.Empty(t, ts.Manager.versionSet.Levels[0])
		assert.Len(t, ts.Manager.versionSet.Levels[1], 1)
	})

	t.Run("compacts level 0 and merges overlapping SSTables from level 1", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		l0 := ts.createSSTable(0, map[string]string{"b": "1"})
		ts.AddSSTable(0, l0)
		overlap := ts.createSSTable(1, map[string]string{"b": "old"})
		ts.AddSSTable(1, overlap)
		non := ts.createSSTable(1, map[string]string{"z": "1"})
		ts.AddSSTable(1, non)
		require.NoError(t, l0.Close())
		require.NoError(t, overlap.Close())
		require.NoError(t, non.Close())

		picked := append([]fileMeta(nil), ts.Manager.versionSet.Levels[0]...)
		overlaps, err := ts.Manager.findOverlaps(1, picked)
		require.NoError(t, err)
		inputs := append(overlaps, picked...)
		err = ts.Manager.mergeIntoLevel(ctx, 1, inputs)
		require.NoError(t, err)

		assert.Empty(t, ts.Manager.versionSet.Levels[0])
		require.Len(t, ts.Manager.versionSet.Levels[1], 2)
		numNon, err := fileNum(non.FileSystem.Path())
		require.NoError(t, err)
		numOverlap, err := fileNum(overlap.FileSystem.Path())
		require.NoError(t, err)
		var nums []uint64
		for _, fm := range ts.Manager.versionSet.Levels[1] {
			nums = append(nums, fm.Number)
		}
		assert.Contains(t, nums, numNon)
		assert.NotContains(t, nums, numOverlap)
	})

	t.Run("returns error when a level 0 SSTable cannot be opened", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		l0 := ts.createSSTable(0, map[string]string{"a": "1"})
		ts.AddSSTable(0, l0)
		require.NoError(t, l0.Close())
		require.NoError(t, os.Remove(l0.FileSystem.Path()))
		require.NoError(t, os.Mkdir(l0.FileSystem.Path(), 0o700))
		defer os.Remove(l0.FileSystem.Path())

		picked := append([]fileMeta(nil), ts.Manager.versionSet.Levels[0]...)
		err := ts.Manager.mergeIntoLevel(ctx, 1, picked)
		assert.Error(t, err)
	})

	t.Run("returns error when overlapping SSTable cannot be opened", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		l0 := ts.createSSTable(0, map[string]string{"a": "1"})
		ts.AddSSTable(0, l0)
		overlap := ts.createSSTable(1, map[string]string{"a": "old"})
		ts.AddSSTable(1, overlap)
		require.NoError(t, l0.Close())
		require.NoError(t, overlap.Close())
		require.NoError(t, os.Remove(overlap.FileSystem.Path()))
		require.NoError(t, os.Mkdir(overlap.FileSystem.Path(), 0o700))
		defer os.Remove(overlap.FileSystem.Path())

		picked := append([]fileMeta(nil), ts.Manager.versionSet.Levels[0]...)
		overlaps, err := ts.Manager.findOverlaps(1, picked)
		require.NoError(t, err)
		inputs := append(overlaps, picked...)
		err = ts.Manager.mergeIntoLevel(ctx, 1, inputs)
		assert.Error(t, err)
	})

	t.Run("returns error when new SSTable cannot be created", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		l0 := ts.createSSTable(0, map[string]string{"a": "1"})
		ts.AddSSTable(0, l0)
		require.NoError(t, l0.Close())

		next := ts.Manager.config.fileNumberAllocator.peek()
		err := os.Mkdir(path.Join(ts.Config.databaseDir, sstPath(next)), 0o700)
		require.NoError(t, err)
		picked := append([]fileMeta(nil), ts.Manager.versionSet.Levels[0]...)
		err = ts.Manager.mergeIntoLevel(ctx, 1, picked)
		assert.Error(t, err)
		_ = os.Remove(path.Join(ts.Config.databaseDir, sstPath(next)))
	})

	t.Run("cleans up target on compaction error", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		l0a := ts.createSSTable(0, map[string]string{"a": "1"})
		l0b := ts.createSSTable(0, map[string]string{"b": "2"})
		ts.AddSSTable(0, l0a)
		ts.AddSSTable(0, l0b)
		require.NoError(t, l0a.Close())
		require.NoError(t, l0b.Close())

		next := ts.Manager.config.fileNumberAllocator.peek()
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()
		err := ts.Manager.Compact(cancelCtx)
		assert.Error(t, err)

		_, statErr := os.Stat(path.Join(ts.Config.databaseDir, sstPath(next)))
		assert.True(t, os.IsNotExist(statErr), "orphaned target file exists: %v", statErr)
	})
}

func TestSSTableManager_compactHigherLevel(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()

	t.Run("compact level 1 into level 2 with overlap", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		src := ts.createSSTable(1, map[string]string{"b": "1"})
		ts.AddSSTable(1, src)
		overlap := ts.createSSTable(2, map[string]string{"b": "old"})
		ts.AddSSTable(2, overlap)
		non := ts.createSSTable(2, map[string]string{"z": "1"})
		ts.AddSSTable(2, non)
		require.NoError(t, src.Close())
		require.NoError(t, overlap.Close())
		require.NoError(t, non.Close())

		picked := []fileMeta{ts.Manager.versionSet.Levels[1][0]}
		overlaps, err := ts.Manager.findOverlaps(2, picked)
		require.NoError(t, err)
		inputs := append(overlaps, picked...)
		err = ts.Manager.mergeIntoLevel(ctx, 2, inputs)
		require.NoError(t, err)

		assert.Empty(t, ts.Manager.versionSet.Levels[1])
		require.Len(t, ts.Manager.versionSet.Levels[2], 2)
		numNon, err := fileNum(non.FileSystem.Path())
		require.NoError(t, err)
		numOverlap, err := fileNum(overlap.FileSystem.Path())
		require.NoError(t, err)
		var nums []uint64
		for _, fm := range ts.Manager.versionSet.Levels[2] {
			nums = append(nums, fm.Number)
		}
		assert.Contains(t, nums, numNon)
		assert.NotContains(t, nums, numOverlap)
	})

	t.Run("compact level 1 into level 2 without overlap", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		src := ts.createSSTable(1, map[string]string{"a": "1"})
		ts.AddSSTable(1, src)
		l2 := ts.createSSTable(2, map[string]string{"z": "1"})
		ts.AddSSTable(2, l2)
		require.NoError(t, src.Close())
		require.NoError(t, l2.Close())

		picked := []fileMeta{ts.Manager.versionSet.Levels[1][0]}
		overlaps, err := ts.Manager.findOverlaps(2, picked)
		require.NoError(t, err)
		inputs := append(overlaps, picked...)
		err = ts.Manager.mergeIntoLevel(ctx, 2, inputs)
		require.NoError(t, err)

		assert.Empty(t, ts.Manager.versionSet.Levels[1])
		require.Len(t, ts.Manager.versionSet.Levels[2], 2)
		numL2, err := fileNum(l2.FileSystem.Path())
		require.NoError(t, err)
		var nums []uint64
		for _, fm := range ts.Manager.versionSet.Levels[2] {
			nums = append(nums, fm.Number)
		}
		assert.Contains(t, nums, numL2)
	})

	t.Run("returns error when source SSTable cannot be opened", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		src := ts.createSSTable(1, map[string]string{"a": "1"})
		ts.AddSSTable(1, src)
		require.NoError(t, src.Close())
		require.NoError(t, os.Remove(src.FileSystem.Path()))
		require.NoError(t, os.Mkdir(src.FileSystem.Path(), 0o700))
		defer os.Remove(src.FileSystem.Path())

		picked := []fileMeta{ts.Manager.versionSet.Levels[1][0]}
		err := ts.Manager.mergeIntoLevel(ctx, 2, picked)
		assert.Error(t, err)
	})

	t.Run("returns error when overlapping SSTable cannot be opened", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		src := ts.createSSTable(1, map[string]string{"a": "1"})
		ts.AddSSTable(1, src)
		overlap := ts.createSSTable(2, map[string]string{"a": "old"})
		ts.AddSSTable(2, overlap)
		require.NoError(t, src.Close())
		require.NoError(t, overlap.Close())
		require.NoError(t, os.Remove(overlap.FileSystem.Path()))
		require.NoError(t, os.Mkdir(overlap.FileSystem.Path(), 0o700))
		defer os.Remove(overlap.FileSystem.Path())

		picked := []fileMeta{ts.Manager.versionSet.Levels[1][0]}
		overlaps, err := ts.Manager.findOverlaps(2, picked)
		require.NoError(t, err)
		inputs := append(overlaps, picked...)
		err = ts.Manager.mergeIntoLevel(ctx, 2, inputs)
		assert.Error(t, err)
	})

	t.Run("returns error when new SSTable cannot be created", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		src := ts.createSSTable(1, map[string]string{"a": "1"})
		ts.AddSSTable(1, src)
		require.NoError(t, src.Close())

		next := ts.Manager.config.fileNumberAllocator.peek()
		err := os.Mkdir(path.Join(ts.Config.databaseDir, sstPath(next)), 0o700)
		require.NoError(t, err)
		picked := []fileMeta{ts.Manager.versionSet.Levels[1][0]}
		err = ts.Manager.mergeIntoLevel(ctx, 2, picked)
		assert.Error(t, err)
		_ = os.Remove(path.Join(ts.Config.databaseDir, sstPath(next)))
	})
}

func TestSSTableManager_mergeIntoLevel(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()

	t.Run("creates new level and removes sources", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		src := ts.createSSTable(0, map[string]string{"a": "1"})
		ts.AddSSTable(0, src)
		require.NoError(t, src.Close())
		srcPath := src.FileSystem.Path()

		ts.Manager.versionSet.Levels = ts.Manager.versionSet.Levels[:1]

		inputs := append([]fileMeta(nil), ts.Manager.versionSet.Levels[0]...)
		err := ts.Manager.mergeIntoLevel(ctx, 1, inputs)
		require.NoError(t, err)

		require.Len(t, ts.Manager.versionSet.Levels, 2)
		assert.Empty(t, ts.Manager.versionSet.Levels[0])
		assert.Len(t, ts.Manager.versionSet.Levels[1], 1)

		_, err = os.Stat(srcPath)
		assert.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("retains sources on manifest failure", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		src := ts.createSSTable(0, map[string]string{"a": "1"})
		ts.AddSSTable(0, src)
		require.NoError(t, src.Close())
		srcPath := src.FileSystem.Path()

		ts.Manager.manifest = failingManifest{}

		inputs := append([]fileMeta(nil), ts.Manager.versionSet.Levels[0]...)
		err := ts.Manager.mergeIntoLevel(ctx, 1, inputs)
		assert.Error(t, err)

		require.Len(t, ts.Manager.versionSet.Levels[0], 1)
		_, err = os.Stat(srcPath)
		assert.NoError(t, err)
	})

	t.Run("keeps tombstone with active snapshot", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		fs := ts.newSSTableFS(0)
		mem := InitMemtable(*ts.Config)
		mem.Put(newRecord(Bytes("a"), nil, 1))
		sst, _, err := flush(ctx, *ts.Config, mem, fs)
		require.NoError(t, err)
		ts.AddSSTable(0, &sst)
		require.NoError(t, sst.Close())

		ts.Manager.minSnapshotSeq = 1

		inputs := append([]fileMeta(nil), ts.Manager.versionSet.Levels[0]...)
		err = ts.Manager.mergeIntoLevel(ctx, 1, inputs)
		require.NoError(t, err)

		meta := ts.Manager.versionSet.Levels[1][0]
		fsMerged, err := OpenExistingFS(ctx, path.Join(ts.Manager.config.databaseDir, sstPath(meta.Number)))
		require.NoError(t, err)
		merged, err := NewSSTable(ctx, cfg, fsMerged)
		require.NoError(t, err)

		_, err = merged.GetValue(ctx, Bytes("a"))
		assert.ErrorIs(t, err, ErrTombstoneFound)
	})

	t.Run("keeps tombstone when higher level exists", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		// Add dummy file to higher level to force bottom=false
		ts.Manager.versionSet.ensureLevel(2)
		ts.Manager.versionSet.Levels[2] = []fileMeta{{Number: 99, Level: 2, Smallest: InternalKey{UserKey: Bytes("x")}, Largest: InternalKey{UserKey: Bytes("x")}}}

		fs := ts.newSSTableFS(0)
		mem := InitMemtable(*ts.Config)
		mem.Put(newRecord(Bytes("a"), nil, 1))
		sst, _, err := flush(ctx, *ts.Config, mem, fs)
		require.NoError(t, err)
		ts.AddSSTable(0, &sst)
		require.NoError(t, sst.Close())

		ts.Manager.minSnapshotSeq = 2

		inputs := append([]fileMeta(nil), ts.Manager.versionSet.Levels[0]...)
		err = ts.Manager.mergeIntoLevel(ctx, 1, inputs)
		require.NoError(t, err)

		meta := ts.Manager.versionSet.Levels[1][0]
		fsMerged, err := OpenExistingFS(ctx, path.Join(ts.Manager.config.databaseDir, sstPath(meta.Number)))
		require.NoError(t, err)
		merged, err := NewSSTable(ctx, cfg, fsMerged)
		require.NoError(t, err)

		_, err = merged.GetValue(ctx, Bytes("a"))
		assert.ErrorIs(t, err, ErrTombstoneFound)
	})

	t.Run("no-op on empty sources", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		beforeVS := len(ts.Manager.versionSet.Levels)

		err := ts.Manager.mergeIntoLevel(ctx, 1, nil)
		require.NoError(t, err)

		assert.Equal(t, beforeVS, len(ts.Manager.versionSet.Levels))
	})
}

func TestSSTableManager_findOverlappingSSTables(t *testing.T) {
	cfg := testConfig()
	ctx := context.Background()
	ik := func(s string) InternalKey { return InternalKey{UserKey: Bytes(s)} }

	t.Run("Level does not exist", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		inputs := []fileMeta{{Smallest: ik("a"), Largest: ik("b")}}
		overlaps, err := ts.Manager.findOverlaps(5, inputs)
		require.NoError(t, err)
		assert.Nil(t, overlaps)
	})

	t.Run("Level is nil", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		if ts.Manager.versionSet == nil {
			ts.Manager.versionSet = &versionSet{}
		}
		ts.Manager.versionSet.ensureLevel(2)
		ts.Manager.versionSet.Levels[2] = nil

		inputs := []fileMeta{{Smallest: ik("a"), Largest: ik("b")}}
		overlaps, err := ts.Manager.findOverlaps(2, inputs)
		require.NoError(t, err)
		assert.Nil(t, overlaps)
	})

	t.Run("Level empty", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		if ts.Manager.versionSet == nil {
			ts.Manager.versionSet = &versionSet{}
		}
		ts.Manager.versionSet.ensureLevel(1)
		ts.Manager.versionSet.Levels[1] = []fileMeta{}

		inputs := []fileMeta{{Smallest: ik("a"), Largest: ik("b")}}
		overlaps, err := ts.Manager.findOverlaps(1, inputs)
		require.NoError(t, err)
		assert.Nil(t, overlaps)
	})

	t.Run("Return only overlapping SSTables", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		ts.Manager.versionSet.ensureLevel(1)
		lvl1 := []fileMeta{
			{Number: 1, Level: 1, Smallest: ik("a"), Largest: ik("c")},
			{Number: 2, Level: 1, Smallest: ik("e"), Largest: ik("g")},
			{Number: 3, Level: 1, Smallest: ik("h"), Largest: ik("j")},
		}
		ts.Manager.versionSet.Levels[1] = append([]fileMeta(nil), lvl1...)

		inputs := []fileMeta{{Number: 4, Level: 0, Smallest: ik("b"), Largest: ik("f")}}
		overlaps, err := ts.Manager.findOverlaps(1, inputs)
		require.NoError(t, err)
		require.Len(t, overlaps, 2)
		var nums []uint64
		for _, fm := range overlaps {
			nums = append(nums, fm.Number)
		}
		assert.Contains(t, nums, uint64(1))
		assert.Contains(t, nums, uint64(2))
		assert.NotContains(t, nums, uint64(3))
	})

	t.Run("No overlapping SSTables", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		ts.Manager.versionSet.ensureLevel(1)
		lvl1 := []fileMeta{
			{Number: 1, Level: 1, Smallest: ik("a"), Largest: ik("b")},
			{Number: 2, Level: 1, Smallest: ik("e"), Largest: ik("f")},
		}
		ts.Manager.versionSet.Levels[1] = append([]fileMeta(nil), lvl1...)

		inputs := []fileMeta{{Number: 3, Level: 0, Smallest: ik("c"), Largest: ik("d")}}
		overlaps, err := ts.Manager.findOverlaps(1, inputs)
		require.NoError(t, err)
		assert.Empty(t, overlaps)
	})

	t.Run("Error opening SSTable and resource cleanup", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		l0 := ts.createSSTable(0, map[string]string{"b": "1"})
		ts.AddSSTable(0, l0)
		overlap := ts.createSSTable(1, map[string]string{"b": "old"})
		ts.AddSSTable(1, overlap)
		non := ts.createSSTable(1, map[string]string{"z": "1"})
		ts.AddSSTable(1, non)
		require.NoError(t, l0.Close())
		require.NoError(t, overlap.Close())
		require.NoError(t, non.Close())

		picked := append([]fileMeta(nil), ts.Manager.versionSet.Levels[0]...)
		overlaps, err := ts.Manager.findOverlaps(1, picked)
		require.NoError(t, err)
		require.Len(t, overlaps, 1)

		require.NoError(t, os.Remove(overlap.FileSystem.Path()))
		require.NoError(t, os.Mkdir(overlap.FileSystem.Path(), 0o700))
		defer os.Remove(overlap.FileSystem.Path())

		err = ts.Manager.mergeIntoLevel(ctx, 1, append(overlaps, picked...))
		assert.Error(t, err)
	})

	t.Run("Empty sources", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		ts.Manager.versionSet.ensureLevel(1)
		ts.Manager.versionSet.Levels[1] = []fileMeta{{Number: 1, Level: 1, Smallest: ik("a"), Largest: ik("b")}}

		overlaps, err := ts.Manager.findOverlaps(1, nil)
		require.NoError(t, err)
		assert.Nil(t, overlaps)
	})

	t.Run("Only empty source sstables", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		ts.Manager.versionSet.ensureLevel(1)
		ts.Manager.versionSet.Levels[1] = []fileMeta{{Number: 1, Level: 1, Smallest: ik("a"), Largest: ik("b")}}

		empty := fileMeta{Smallest: InternalKey{}, Largest: InternalKey{}}
		overlaps, err := ts.Manager.findOverlaps(1, []fileMeta{empty})
		require.NoError(t, err)
		assert.Nil(t, overlaps)
	})

	t.Run("With empty and non-empty source sstables", func(t *testing.T) {
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		ts.Manager.versionSet.ensureLevel(1)
		ts.Manager.versionSet.Levels[1] = []fileMeta{{Number: 5, Level: 1, Smallest: ik("c"), Largest: ik("e")}}

		empty := fileMeta{Smallest: InternalKey{}, Largest: InternalKey{}}
		nonEmpty := fileMeta{Smallest: ik("b"), Largest: ik("d")}
		overlaps, err := ts.Manager.findOverlaps(1, []fileMeta{empty, nonEmpty})
		require.NoError(t, err)
		require.Len(t, overlaps, 1)
		assert.Equal(t, uint64(5), overlaps[0].Number)
	})
}

func TestRemoveFiles(t *testing.T) {
	t.Run("ignores missing files", func(t *testing.T) {
		dir := t.TempDir()
		paths := []string{
			filepath.Join(dir, sstPath(1)),
			filepath.Join(dir, sstPath(2)),
		}
		for _, p := range paths {
			f, err := os.Create(p)
			require.NoError(t, err)
			require.NoError(t, f.Close())
		}
		files := []fileMeta{{Number: 1}, {Number: 2}, {Number: 3}}
		require.NoError(t, removeFiles(dir, files))
		for _, p := range paths {
			_, err := os.Stat(p)
			require.ErrorIs(t, err, os.ErrNotExist)
		}
	})

	t.Run("returns first error", func(t *testing.T) {
		dir := t.TempDir()

		// Create a non-empty directory for file 1 so os.Remove fails.
		errDir := filepath.Join(dir, sstPath(1))
		require.NoError(t, os.Mkdir(errDir, 0o755))
		f, err := os.Create(filepath.Join(errDir, "child"))
		require.NoError(t, err)
		require.NoError(t, f.Close())

		// Create a regular file for file 2 which should be removed.
		okPath := filepath.Join(dir, sstPath(2))
		f, err = os.Create(okPath)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		files := []fileMeta{{Number: 1}, {Number: 2}}
		err = removeFiles(dir, files)
		assert.Error(t, err)

		// File 2 should be removed
		_, statErr := os.Stat(okPath)
		assert.ErrorIs(t, statErr, os.ErrNotExist)

		// Directory for file 1 should remain
		_, statErr = os.Stat(errDir)
		assert.NoError(t, statErr)
	})
}
