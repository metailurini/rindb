package rindb

import (
	"container/list"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSStable tests the SStable functionality.
func TestSStable(t *testing.T) {
	t.Run("FlushEmptyMemtable", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("The code did not panic")
			}
			assert.Equal(t, "empty memtable!", r)
		}()
		fss, closer := initTempFileSystems(t, 1)
		defer closer()
		fs := fss[0]
		mem := InitMemtable()
		_, err := Flush(mem, fs)
		assert.NoError(t, err)
	})
	t.Run("FlushWithElements", func(t *testing.T) {
		var sstable SStable
		fss, closer := initTempFileSystems(t, 1)
		defer closer()
		fs := fss[0]
		data := []struct {
			key   Bytes
			value Bytes
		}{
			{Bytes("a"), Bytes("1")},
			{Bytes("b"), Bytes("2")},
			{Bytes("c"), Bytes("3")},
			{Bytes("d"), Bytes("4")},
		}
		mem := InitMemtable()
		for _, v := range data {
			mem.Put(v.key, v.value)
		}
		sstable, err := Flush(mem, fs)
		assert.NoError(t, err)
		tailSSTableOffset, err := readTailSSTable(sstable.FileSystem)
		assert.NoError(t, err)
		sparseIndexOffset, err := ReadNumber(sstable)
		assert.NoError(t, err)
		assert.NotZero(t, sparseIndexOffset)
		ret, err := sstable.file.Seek(0, io.SeekStart)
		assert.NoError(t, err)
		expectedSparseIndex := make(SparseIndex, 0)
		idx := 0
		for ret < int64(sparseIndexOffset) {

			record, err := ReadRecord(sstable)
			assert.NoError(t, err)
			assert.Equal(t, data[idx].key, record.GetKey())
			assert.Equal(t, data[idx].value, record.GetValue())
			expectedSparseIndex = append(expectedSparseIndex, KeyOffset{record.GetKey(), ret})
			ret, err = sstable.CursorPos()
			assert.NoError(t, err)
			idx++
		}
		idx = 0
		for ret < tailSSTableOffset {

			record, err := ReadRecord(sstable)
			assert.NoError(t, err)
			assert.Equal(t, data[idx].key, record.GetKey())
			k := NewKeyOffset(record.GetKey(), record.GetValue())
			assert.Equal(t, expectedSparseIndex[idx].key, k.key)
			assert.Equal(t, expectedSparseIndex[idx].offset, k.offset)
			ret, err = sstable.CursorPos()
			assert.NoError(t, err)
			idx++
		}
	})
	t.Run("SparseIndexLoad", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1)
		defer closer()
		fs := fss[0]
		mem := InitMemtable()
		mem.Put(Bytes("2"), Bytes("3"))
		mem.Put(Bytes("1"), Bytes("2"))
		mem.Put(Bytes("3"), Bytes("4"))
		sstable1, err := Flush(mem, fs)
		assert.NoError(t, err)
		sstable2, err := NewSSTable(fs)
		assert.NoError(t, err)
		assert.Equal(t, sstable1.SparseIndex, sstable2.SparseIndex)
	})
	t.Run("SparseIndexOffsetAccuracy", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1)
		defer closer()
		fs := fss[0]
		mem := InitMemtable()
		mem.Put(Bytes("2"), Bytes("3"))
		mem.Put(Bytes("1"), Bytes("2"))
		mem.Put(Bytes("3"), Bytes("4"))
		_, err := Flush(mem, fs)
		assert.NoError(t, err)
		sstable, err := NewSSTable(fs)
		assert.NoError(t, err)
		sparseIndex := sstable.SparseIndex
		for idx := len(sparseIndex) - 1; idx > -1; idx-- {
			keyOffset := sparseIndex[idx]
			_, err := sstable.file.Seek(keyOffset.offset, io.SeekStart)
			assert.NoError(t, err)
			record, err := ReadRecord(sstable)
			assert.NoError(t, err)
			assert.Equal(t, keyOffset.key, record.GetKey())
		}
	})
	t.Run("FlushToSSTable", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1)
		defer closer()
		fs := fss[0]
		mem := InitMemtable()
		mem.Put(Bytes("2"), Bytes("3"))
		mem.Put(Bytes("1"), Bytes("2"))
		mem.Put(Bytes("3"), Bytes("4"))
		assert.Equal(t, uint(3), mem.data.Len())
		sstable, err := Flush(mem, fs)
		assert.NoError(t, err)
		assert.Equal(t, uint(0), mem.data.Len())
		value, err := sstable.GetValue(Bytes("2"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("3"), value)
		value, err = sstable.GetValue(Bytes("1"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("2"), value)
		value, err = sstable.GetValue(Bytes("3"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("4"), value)
		value, err = sstable.GetValue(Bytes(".3"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Equal(t, Bytes(nil), value)
	})
	t.Run("Iterator", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1)
		defer closer()
		fs := fss[0]
		mem := InitMemtable()
		mem.Put(Bytes("2"), Bytes("3"))
		mem.Put(Bytes("1"), Bytes("2"))
		mem.Put(Bytes("3"), Bytes("4"))
		sstable, err := Flush(mem, fs)
		assert.NoError(t, err)
		iterator, err := sstable.Iterator()
		assert.NoError(t, err)
		assert.True(t, iterator.HasNext())
		record, err := iterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, Bytes("1"), record.GetKey())
		assert.Equal(t, Bytes("2"), record.GetValue())
		assert.True(t, iterator.HasNext())
		record, err = iterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, Bytes("2"), record.GetKey())
		assert.Equal(t, Bytes("3"), record.GetValue())
		assert.True(t, iterator.HasNext())
		record, err = iterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, Bytes("3"), record.GetKey())
		assert.Equal(t, Bytes("4"), record.GetValue())
		assert.False(t, iterator.HasNext())
		record, err = iterator.Next()
		assert.ErrorIs(t, err, EOI)
		assert.Nil(t, record)
	})
}

// Test_genSparseIndex tests the generation of sparse index from memtable.
func Test_genSparseIndex(t *testing.T) {
	mem := InitMemtable()
	mem.Put(Bytes("1"), Bytes("2"))
	mem.Put(Bytes("2"), Bytes("3"))
	mem.Put(Bytes("3"), Bytes("4"))
	index := genSparseIndex(mem)
	assert.Equal(t, Bytes("1"), index[0].key)
	assert.Equal(t, int64(0), index[0].offset)
	assert.Equal(t, Bytes("2"), index[1].key)
	assert.Equal(t, int64(18), index[1].offset)
	assert.Equal(t, Bytes("3"), index[2].key)
	assert.Equal(t, int64(36), index[2].offset)
}

// TestSparseIndex_GetOffset tests the GetOffset method of SparseIndex.
func TestSparseIndex_GetOffset(t *testing.T) {
	type args struct {
		key Bytes
	}
	tests := []struct {
		name    string
		s       SparseIndex
		args    args
		want    int64
		wantErr error
	}{
		{
			name: "Get first key",
			s: SparseIndex{
				{Bytes("1"), 1},
				{Bytes("2"), 2},
				{Bytes("3"), 3},
				{Bytes("4"), 4},
			},
			args:    args{Bytes("1")},
			want:    1,
			wantErr: nil,
		},
		{
			name: "Get last key",
			s: SparseIndex{
				{Bytes("1"), 1},
				{Bytes("2"), 2},
				{Bytes("3"), 3},
				{Bytes("4"), 4},
			},
			args:    args{Bytes("4")},
			want:    4,
			wantErr: nil,
		},
		{
			name: "Get mid 1 key",
			s: SparseIndex{
				{Bytes("1"), 1},
				{Bytes("2"), 2},
				{Bytes("3"), 3},
				{Bytes("4"), 4},
			},
			args:    args{Bytes("3")},
			want:    3,
			wantErr: nil,
		},
		{
			name: "Get mid 2 key",
			s: SparseIndex{
				{Bytes("1"), 1},
				{Bytes("2"), 2},
				{Bytes("3"), 3},
				{Bytes("4"), 4},
			},
			args:    args{Bytes("2")},
			want:    2,
			wantErr: nil,
		},
		{
			name: "Get mid 3 key",
			s: SparseIndex{
				{Bytes("1"), 1},
				{Bytes("2"), 2},
				{Bytes("3"), 3},
				{Bytes("4"), 4},
				{Bytes("5"), 5},
			},
			args:    args{Bytes("3")},
			want:    3,
			wantErr: nil,
		},
		{
			name: "Get on-exist key less than head",
			s: SparseIndex{
				{Bytes("1"), 1},
				{Bytes("2"), 2},
				{Bytes("3"), 3},
				{Bytes("4"), 4},
				{Bytes("5"), 5},
			},
			args:    args{Bytes("0")},
			want:    0,
			wantErr: ErrKeyNotFound,
		},
		{
			name: "Get on-exist key greater than tail",
			s: SparseIndex{
				{Bytes("1"), 1},
				{Bytes("2"), 2},
				{Bytes("3"), 3},
				{Bytes("4"), 4},
				{Bytes("5"), 5},
			},
			args:    args{Bytes("6")},
			want:    0,
			wantErr: ErrKeyNotFound,
		},
		{
			name: "Get on-exist key inside range",
			s: SparseIndex{
				{Bytes("1"), 1},
				{Bytes("2"), 2},
				{Bytes("4"), 4},
				{Bytes("5"), 5},
				{Bytes("6"), 6},
			},
			args:    args{Bytes("3")},
			want:    0,
			wantErr: ErrKeyNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.s.GetOffset(tt.args.key)
			assert.Equal(t, tt.wantErr, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestFlushWithTombstones tests flushing a memtable with tombstones.
func TestFlushWithTombstones(t *testing.T) {
	fss, closer := initTempFileSystems(t, 1)
	defer closer()
	fs := fss[0]
	k1 := randStringBytes(10)
	k2 := randStringBytes(10)
	mem := InitMemtable()
	mem.Put(k1, Bytes("v1"))
	mem.Put(k2, nil)
	sstable, err := Flush(mem, fs)
	assert.NoError(t, err)
	v1, err := sstable.GetValue(k1)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v1"), v1)
	v2, err := sstable.GetValue(k2)
	assert.NoError(t, err)
	assert.Nil(t, v2)
}

// TestBloomFilterSkipsReads tests that the Bloom filter skips unnecessary reads.
func TestBloomFilterSkipsReads(t *testing.T) {
	fss, closer := initTempFileSystems(t, 1)
	defer closer()
	fs := fss[0]
	mem := InitMemtable()
	mem.Put(Bytes("k1"), Bytes("v1"))
	sstable, err := Flush(mem, fs)
	assert.NoError(t, err)
	_, err = sstable.GetValue(Bytes("k2"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	pos, err := fs.CursorPos()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pos, "File should not be read due to Bloom filter")
}

// TestSSTableManager_Compact tests the compaction process in SSTableManager.
func TestSSTableManager_Compact(t *testing.T) {
	fss, closer := initTempFileSystems(t, 33)
	defer closer()
	h := &SSTableManager{openedFs: list.New()}
	defer h.Close()
	h.levels = []*LinkedList[*FileSystem]{InitLinkedList[*FileSystem]()}
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
		if h.levels[0].lastNode.Value.Path() == fs.Path() {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "no such file or directory")
		}
	}
}

// TestSSTableManager_Merge tests merging multiple SSTables.
func TestSSTableManager_Merge(t *testing.T) {
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
}

// TestSSTableManager_CompactWithTombstones tests compaction handling of overwrites and tombstones.
func TestSSTableManager_CompactWithTombstones(t *testing.T) {
	ssTableManager := SSTableManager{openedFs: list.New()}
	defer ssTableManager.Close()
	fss, closer := initTempFileSystems(t, 3)
	defer closer()
	mem1 := InitMemtable()
	mem1.Put(Bytes("k1"), Bytes("v1-old"))
	mem1.Put(Bytes("k2"), Bytes("v2"))
	_, err := Flush(mem1, fss[0])
	assert.NoError(t, err)
	mem2 := InitMemtable()
	mem2.Put(Bytes("k1"), Bytes("v1-new"))
	mem2.Put(Bytes("k2"), nil)
	_, err = Flush(mem2, fss[1])
	assert.NoError(t, err)
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
	mergedFS := ssTableManager.levels[1].rootNode.next.Value
	sstable, err := NewSSTable(mergedFS)
	assert.NoError(t, err)
	v1, err := sstable.GetValue(Bytes("k1"))
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v1-new"), v1)
	fmt.Printf("v1: %s\n", v1)
	v2, err := sstable.GetValue(Bytes("k2"))
	assert.NoError(t, err)
	assert.Nil(t, v2)
}
