// Package rindb is key-value database
package rindb

import (
	"container/list"
	"fmt"
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

		hino, err := InitHino()
		assert.NoError(t, err)
		defer hino.Close()

		newSSTableFS, err := hino.NewSSTableFS(0)
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

func TestHino(t *testing.T) {
	t.Run("hino::LoadLevels", func(t *testing.T) {
		hino, err := InitHino()
		assert.NoError(t, err)
		assert.NoError(t, hino.Compact())
		defer hino.Close()
		for levelNumb, level := range hino.levels {
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

	t.Run("hino::Compact", func(t *testing.T) {
		/*
		   Compact logic:
		   - 1 lvl0 <-(compact)- 1 lvl0 -> 01 lvl0
		   - 1 lvl1 <-(compact)- 2 lvl0 -> 02 lvl0
		   - 1 lvl2 <-(compact)- 3 lvl1 -> 06 lvl0
		   - 1 lvl3 <-(compact)- 4 lvl2 -> 24 lvl0
		   --------------------------------[Total]
		                                   33 lvl0
		*/
		fss, closer := initTempFileSystems(t, 33)
		defer closer()

		h := &Hino{openedFs: list.New()}
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
		hino := Hino{openedFs: list.New()}
		defer hino.Close()

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
