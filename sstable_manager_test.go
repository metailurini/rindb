package rindb

import (
	"container/list"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSSTableManager_LoadLevels(t *testing.T) {
	t.Run("SSTableManager::LoadLevels", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager()
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
	t.Run("Merging sstables", func(t *testing.T) {
		ssTableManager := SSTableManager{openedFs: list.New()}
		defer ssTableManager.Close()

		fss, closer := initTempFileSystems(t, 4)
		defer closer()

		sstables := make([]SStable, 0)
		memtable := InitMemtable()

		memtable.Put(Bytes("1"), Bytes("2"))
		memtable.Put(Bytes("2"), Bytes("3"))
		memtable.Put(Bytes("3"), Bytes("4"))
		sstable1, err := Flush(memtable, fss[0])
		assert.NoError(t, err)
		sstables = append(sstables, sstable1)

		memtable.Put(Bytes("1"), Bytes("3"))
		memtable.Put(Bytes("2"), Bytes(nil))
		memtable.Put(Bytes("4"), Bytes("5"))
		sstable2, err := Flush(memtable, fss[1])
		assert.NoError(t, err)
		sstables = append(sstables, sstable2)

		memtable.Put(Bytes("5"), Bytes("6"))
		sstable3, err := Flush(memtable, fss[2])
		assert.NoError(t, err)
		sstables = append(sstables, sstable3)

		fs := fss[3]
		newSSTable, err := mergeSSTables(fs, sstables)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(newSSTable.SparseIndex))

		sstableIterator, err := newSSTable.Iterator()
		assert.NoError(t, err)

		assert.True(t, sstableIterator.HasNext())
		record, err := sstableIterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, Bytes("1"), record.GetKey())
		assert.Equal(t, Bytes("3"), record.GetValue())

		assert.True(t, sstableIterator.HasNext())
		record, err = sstableIterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, Bytes("2"), record.GetKey())
		assert.Equal(t, Bytes(nil), record.GetValue())

		assert.True(t, sstableIterator.HasNext())
		record, err = sstableIterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, Bytes("3"), record.GetKey())
		assert.Equal(t, Bytes("4"), record.GetValue())

		assert.True(t, sstableIterator.HasNext())
		record, err = sstableIterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, Bytes("4"), record.GetKey())
		assert.Equal(t, Bytes("5"), record.GetValue())

		assert.True(t, sstableIterator.HasNext())
		record, err = sstableIterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, Bytes("5"), record.GetKey())
		assert.Equal(t, Bytes("6"), record.GetValue())

		assert.False(t, sstableIterator.HasNext())
		record, err = sstableIterator.Next()
		assert.ErrorIs(t, err, EOI)
		assert.Nil(t, record)
	})
}

func TestSSTableManager_SearchKey(t *testing.T) {
	t.Run("Key in memtable, absent in SSTables", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager()
		assert.NoError(t, err)
		defer ssTableManager.Close()

		key := randStringBytes(10)

		result, err := ssTableManager.searchKey(key)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Key in level 0 only", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager()
		assert.NoError(t, err)
		defer ssTableManager.Close()

		fs, err := ssTableManager.NewSSTableFS(0)
		assert.NoError(t, err)
		defer fs.Close()

		mem := InitMemtable()
		key := Bytes("level0-key")
		value := Bytes("level0-value")
		mem.Put(key, value)
		_, err = Flush(mem, fs)
		assert.NoError(t, err)

		if len(ssTableManager.levels) == 0 {
			ssTableManager.levels = append(ssTableManager.levels, InitLinkedList[*FileSystem]())
		}
		ssTableManager.levels[0].PushBack(fs)

		result, err := ssTableManager.searchKey(key)
		assert.NoError(t, err)
		assert.Equal(t, value, result)
	})

	t.Run("Key in level 1 overridden by level 0", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager()
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
		mem1 := InitMemtable()
		key := randStringBytes(10)
		oldValue := Bytes("old-value")
		mem1.Put(key, oldValue)
		_, err = Flush(mem1, fs1)
		assert.NoError(t, err)

		// Level 0: newer value
		fs0, err := ssTableManager.NewSSTableFS(0)
		assert.NoError(t, err)
		defer fs0.Close()
		mem0 := InitMemtable()
		newValue := Bytes("new-value")
		mem0.Put(key, newValue)
		_, err = Flush(mem0, fs0)
		assert.NoError(t, err)

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
		ssTableManager, err := InitSSTableManager()
		assert.NoError(t, err)
		backupLevels := ssTableManager.levels
		defer func() {
			ssTableManager.levels = backupLevels
			ssTableManager.Close()
		}()

		fs, err := ssTableManager.NewSSTableFS(0)
		assert.NoError(t, err)
		defer fs.Close()
		mem := InitMemtable()
		mem.Put(Bytes("some-key"), Bytes("some-value"))
		_, err = Flush(mem, fs)
		assert.NoError(t, err)

		// Override levels with new values
		ssTableManager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		ssTableManager.levels[0].PushBack(fs)

		missingKey := randStringBytes(10)
		result, err := ssTableManager.searchKey(missingKey)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Empty SSTableManager", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager()
		assert.NoError(t, err)
		defer ssTableManager.Close()

		ssTableManager.levels = nil // Explicitly empty

		result, err := ssTableManager.searchKey(Bytes("any-key"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Single SSTable in level 0", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager()
		assert.NoError(t, err)
		defer ssTableManager.Close()

		fs, err := ssTableManager.NewSSTableFS(0)
		assert.NoError(t, err)
		defer fs.Close()
		mem := InitMemtable()
		key := Bytes("single-key")
		value := Bytes("single-value")
		mem.Put(key, value)
		_, err = Flush(mem, fs)
		assert.NoError(t, err)
		ssTableManager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		ssTableManager.levels[0].PushBack(fs)

		result, err := ssTableManager.searchKey(key)
		assert.NoError(t, err)
		assert.Equal(t, value, result)
	})

	t.Run("Tombstone in level 0 overrides level 1", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager()
		assert.NoError(t, err)
		defer ssTableManager.Close()

		// Level 1: original value
		fs1, err := ssTableManager.NewSSTableFS(1)
		assert.NoError(t, err)
		defer fs1.Close()
		mem1 := InitMemtable()
		key := Bytes("tombstone-key")
		value := Bytes("original-value")
		mem1.Put(key, value)
		_, err = Flush(mem1, fs1)
		assert.NoError(t, err)

		// Level 0: tombstone
		fs0, err := ssTableManager.NewSSTableFS(0)
		assert.NoError(t, err)
		defer fs0.Close()
		mem0 := InitMemtable()
		mem0.Put(key, nil) // Tombstone
		_, err = Flush(mem0, fs0)
		assert.NoError(t, err)

		ssTableManager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem](), InitLinkedList[*FileSystem]()}
		ssTableManager.levels[0].PushBack(fs0)
		ssTableManager.levels[1].PushBack(fs1)

		result, err := ssTableManager.searchKey(key)
		assert.NoError(t, err)
		assert.Nil(t, result) // Tombstone returns nil value
	})

	t.Run("Bloom filter skips irrelevant SSTables", func(t *testing.T) {
		ssTableManager, err := InitSSTableManager()
		assert.NoError(t, err)
		defer ssTableManager.Close()

		fs, err := ssTableManager.NewSSTableFS(0)
		assert.NoError(t, err)
		defer fs.Close()
		mem := InitMemtable()
		mem.Put(Bytes("present-key"), Bytes("present-value"))
		_, err = Flush(mem, fs)
		assert.NoError(t, err)
		ssTableManager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		ssTableManager.levels[0].PushBack(fs)

		// Key not in Bloom filter
		result, err := ssTableManager.searchKey(Bytes("absent-key"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})
}

func TestSSTableManager_CompactThreshold(t *testing.T) {
	t.Run("Compaction threshold 1", func(t *testing.T) {
		ssTableManager := SSTableManager{openedFs: list.New()}
		defer ssTableManager.Close()

		fss, closer := initTempFileSystems(t, 3)
		defer closer()

		// SSTable 1: older data
		mem1 := InitMemtable()
		mem1.Put(Bytes("k1"), Bytes("v1-old"))
		mem1.Put(Bytes("k2"), Bytes("v2"))
		_, err := Flush(mem1, fss[0])
		assert.NoError(t, err)

		// SSTable 2: newer data
		mem2 := InitMemtable()
		mem2.Put(Bytes("k1"), Bytes("v1-new"))
		mem2.Put(Bytes("k2"), nil) // Tombstone
		_, err = Flush(mem2, fss[1])
		assert.NoError(t, err)

		// SSTable 3: empty
		mem3 := InitMemtable()
		mem3.Put(Bytes("k3"), Bytes("v3"))
		_, err = Flush(mem3, fss[2])
		assert.NoError(t, err)

		ssTableManager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		ssTableManager.levels[0].PushBack(fss[0])
		ssTableManager.levels[0].PushBack(fss[1])
		ssTableManager.levels[0].PushBack(fss[2])

		err = ssTableManager.Compact()
		assert.NoError(t, err)

		// Verify merged SSTable
		mergedFS := ssTableManager.levels[1].rootNode.next.Value
		sstable, err := NewSSTable(mergedFS)
		assert.NoError(t, err)

		v1, err := sstable.GetValue(Bytes("k1"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("v1-new"), v1) // Latest value
		fmt.Printf("v1: %s\n", v1)

		v2, err := sstable.GetValue(Bytes("k2"))
		assert.NoError(t, err)
		assert.Nil(t, v2) // Tombstone preserved
	})

	t.Run("Compaction threshold 2", func(t *testing.T) {
		/*
			Compaction Test Expectations:
			- 1 lvl0 <-(compact)- 1 lvl0 -> 01 lvl0
			- 1 lvl1 <-(compact)- 2 lvl0 -> 02 lvl0
			- 1 lvl2 <-(compact)- 3 lvl1 -> 06 lvl0
			- 1 lvl3 <-(compact)- 4 lvl2 -> 24 lvl0
			--------------------------------[Total]
			33 lvl0
		*/
		fss, closer := initTempFileSystems(t, 33)
		defer closer()

		h := &SSTableManager{openedFs: list.New()}
		defer h.Close()

		h.levels = []*LinkedList[*FileSystem]{
			InitLinkedList[*FileSystem](),
		}

		for _, fs := range fss {
			memtable := InitMemtable()
			memtable.Put(Bytes("1"), Bytes("2"))
			memtable.Put(Bytes("3"), Bytes("4"))
			memtable.Put(Bytes("2"), Bytes("3"))
			_, err := Flush(memtable, fs)
			assert.NoError(t, err)
			h.levels[0].PushBack(fs)
		}

		err := h.Compact()
		assert.NoError(t, err)

		assert.Equal(t, 1, h.levels[0].Len())
		assert.Equal(t, 1, h.levels[1].Len())
		assert.Equal(t, 1, h.levels[2].Len())
		assert.Equal(t, 1, h.levels[3].Len())

		for _, fs := range fss {
			_, err := os.Stat(fs.Path())
			// Only last fs in level 0 hasn't compacted, so it should be existed
			if h.levels[0].lastNode.Value.Path() == fs.Path() {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "no such file or directory")
			}
		}
	})

	t.Run("Exceed threshold and compact", func(t *testing.T) {
		ssTableManager := SSTableManager{openedFs: list.New()}
		defer ssTableManager.Close()

		fss, closer := initTempFileSystems(t, 5) // Exceed threshold
		defer closer()

		for i, fs := range fss {
			mem := InitMemtable()
			mem.Put(Bytes(fmt.Sprintf("k%d", i)), Bytes(fmt.Sprintf("v%d", i)))
			_, err := Flush(mem, fs)
			assert.NoError(t, err)
		}

		ssTableManager.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
		for _, fs := range fss {
			ssTableManager.levels[0].PushBack(fs)
		}

		err := ssTableManager.Compact()
		assert.NoError(t, err)

		assert.True(t, ssTableManager.levels[0].Len() <= 2, "Level 0 should have ≤ 2 SSTables")
		assert.NotNil(t, ssTableManager.levels[1], "Level 1 should exist")
		assert.Greater(t, ssTableManager.levels[1].Len(), 0, "Level 1 should have SSTables")
	})
}
