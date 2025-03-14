// Package rindb is key-value database
package rindb

import (
	"container/list"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

//nolint:funlen
func TestRin(t *testing.T) {
	t.Run("init::rindb", func(t *testing.T) {
		_, err := InitRinDB()
		assert.NoError(t, err)
	})

	t.Run("rindb::put", func(t *testing.T) {
		rin, err := InitRinDB()
		assert.NoError(t, err)

		err = rin.Put(Bytes("key"), Bytes("value"))
		assert.NoError(t, err)
	})

	t.Run("rindb::get", func(t *testing.T) {
		key := Bytes("key")
		rin, err := InitRinDB()
		assert.NoError(t, err)

		value, err := rin.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, Bytes("value"), value)
	})

	t.Run("rindb::remove", func(t *testing.T) {
		rin, err := InitRinDB()
		assert.NoError(t, err)

		key := Bytes("rm-key")
		err = rin.Put(key, Bytes("value"))
		assert.NoError(t, err)

		err = rin.Remove(key)
		assert.NoError(t, err)

		value, err := rin.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, Bytes(nil), value)
	})

	t.Run("flush memtable to sstable", func(t *testing.T) {
		rin, err := InitRinDB()
		assert.NoError(t, err)

		err = rin.Put(Bytes("key"), Bytes("value"))
		assert.NoError(t, err)
		err = rin.Put(Bytes("rm-key"), Bytes("value"))
		assert.NoError(t, err)
		err = rin.Remove(Bytes("rm-key"))
		assert.NoError(t, err)

		ssTableManager, err := InitSSTableManager()
		assert.NoError(t, err)
		defer ssTableManager.Close()

		newSSTableFS, err := ssTableManager.NewSSTableFS(0)
		assert.NoError(t, err)
		defer func() { _ = newSSTableFS.Close() }()

		newSStable, err := Flush(rin.memtable, newSSTableFS)
		assert.NoError(t, err)

		err = rin.wal.Clean()
		assert.NoError(t, err)

		value, err := newSStable.GetValue(Bytes("rm-key"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes(nil), value)

		value, err = newSStable.GetValue(Bytes("key"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("value"), value)
	})
}

func TestSSTableManager(t *testing.T) {
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
				assert.True(t, strings.HasPrefix(fileName, fmt.Sprintf("l%d_", levelNumb)))
				assert.True(t, strings.HasSuffix(fileName, ".sst"))
			}
		}
	})

	t.Run("SSTableManager::Compact", func(t *testing.T) {
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
}

//nolint:funlen
func Test_mergeSSTables(t *testing.T) {
	t.Run("merging sstables", func(t *testing.T) {
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

//nolint:funlen
func TestSSTableManager_searchKey(t *testing.T) {
	t.Run("Key exists in memtable but not in SSTables", func(t *testing.T) {
		// Initialize Rin with a memtable containing the key
		rin, err := InitRinDB()
		assert.NoError(t, err)
		key := RandStringBytes(10)
		value := RandStringBytes(10)
		err = rin.Put(key, value)
		assert.NoError(t, err)

		// Initialize SSTableManager with no SSTables
		ssTableManager, err := InitSSTableManager()
		assert.NoError(t, err)
		defer ssTableManager.Close()

		// Search for the key (should not find it in SSTables, test relies on Rin.Get)
		// Note: searchKey only searches SSTables, so we verify Rin.Get behavior separately
		result, err := rin.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, value, result)

		// Verify ssTableManager.searchKey doesn't find it since it's only in memtable
		result, err = ssTableManager.searchKey(key)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Key exists in SSTable at level 0", func(t *testing.T) {
		// Create a temporary SSTable at level 0
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

		// Ensure level 0 is populated
		if len(ssTableManager.levels) == 0 {
			ssTableManager.levels = append(ssTableManager.levels, InitLinkedList[*FileSystem]())
		}
		ssTableManager.levels[0].PushBack(fs)

		// Search for the key
		result, err := ssTableManager.searchKey(key)
		assert.NoError(t, err)
		assert.Equal(t, value, result)

		// Verify non-existent key
		result, err = ssTableManager.searchKey(Bytes("non-existent"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, result)
	})

	t.Run("Key exists in level 1 with newer value in level 0", func(t *testing.T) {
		t.Skip("Skip this test")
		// Initialize SSTableManager
		ssTableManager, err := InitSSTableManager()
		assert.NoError(t, err)
		defer ssTableManager.Close()

		// Create SSTable at level 1
		fs1, err := ssTableManager.NewSSTableFS(1)
		assert.NoError(t, err)
		defer fs1.Close()

		mem1 := InitMemtable()
		key := Bytes("multi-level-key")
		oldValue := Bytes("old-value")
		mem1.Put(key, oldValue)
		_, err = Flush(mem1, fs1)
		assert.NoError(t, err)

		// Create SSTable at level 0 with newer value
		fs0, err := ssTableManager.NewSSTableFS(0)
		assert.NoError(t, err)
		defer fs0.Close()

		mem0 := InitMemtable()
		newValue := Bytes("new-value")
		mem0.Put(key, newValue)
		_, err = Flush(mem0, fs0)
		assert.NoError(t, err)

		// Populate levels
		if len(ssTableManager.levels) < 2 {
			ssTableManager.levels = append(ssTableManager.levels, InitLinkedList[*FileSystem](), InitLinkedList[*FileSystem]())
		}
		ssTableManager.levels[0].PushBack(fs0)
		ssTableManager.levels[1].PushBack(fs1)

		// Search for the key, should return the newer value from level 0
		result, err := ssTableManager.searchKey(key)
		assert.NoError(t, err)
		assert.Equal(t, newValue, result, "Expected newer value from level 0")
	})
}

func RandStringBytes(i int) Bytes {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, i)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}
