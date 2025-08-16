package rindb

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSStable tests the SStable functionality.
func TestSStable(t *testing.T) {
	cfg := testConfig()
	t.Run("FlushEmptyMemtable", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("The code did not panic")
			}
			assert.Equal(t, "empty memtable!", r)
		}()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]
		mem := InitMemtable(cfg)
		_, err := Flush(cfg, mem, fs)
		assert.NoError(t, err)
	})
	t.Run("FlushWithElements", func(t *testing.T) {
		var sstable SStable
		fss, closer := initTempFileSystems(t, 1, nil)
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
		mem := InitMemtable(cfg)
		for i, v := range data {
			mem.Put(RecordImpl{
				Key:            v.key,
				Value:          v.value,
				SequenceNumber: uint64(i),
			})
		}
		sstable, err := Flush(cfg, mem, fs)
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
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]
		mem := InitMemtable(cfg)
		mem.Put(RecordImpl{Bytes("2"), Bytes("3"), 1})
		mem.Put(RecordImpl{Bytes("1"), Bytes("2"), 2})
		mem.Put(RecordImpl{Bytes("3"), Bytes("4"), 3})
		sstable1, err := Flush(cfg, mem, fs)
		assert.NoError(t, err)
		sstable2, err := NewSSTable(cfg, fs)
		assert.NoError(t, err)
		assert.Equal(t, sstable1.SparseIndex, sstable2.SparseIndex)
	})
	t.Run("SparseIndexOffsetAccuracy", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]
		mem := InitMemtable(cfg)
		mem.Put(RecordImpl{Bytes("2"), Bytes("3"), 1})
		mem.Put(RecordImpl{Bytes("1"), Bytes("2"), 2})
		mem.Put(RecordImpl{Bytes("3"), Bytes("4"), 3})
		_, err := Flush(cfg, mem, fs)
		assert.NoError(t, err)
		sstable, err := NewSSTable(cfg, fs)
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
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]
		mem := InitMemtable(cfg)
		mem.Put(RecordImpl{Bytes("2"), Bytes("3"), 1})
		mem.Put(RecordImpl{Bytes("1"), Bytes("2"), 2})
		mem.Put(RecordImpl{Bytes("3"), Bytes("4"), 3})
		assert.Equal(t, uint(3), mem.data.Len())
		sstable, err := Flush(cfg, mem, fs)
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
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]
		mem := InitMemtable(cfg)
		mem.Put(RecordImpl{Bytes("2"), Bytes("3"), 1})
		mem.Put(RecordImpl{Bytes("1"), Bytes("2"), 2})
		mem.Put(RecordImpl{Bytes("3"), Bytes("4"), 3})
		sstable, err := Flush(cfg, mem, fs)
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
	t.Run("RangeIterator", func(t *testing.T) {
		cfg := testConfig()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]

		// Prepare data for the SSTable
		data := []struct {
			key   Bytes
			value Bytes
		}{
			{Bytes("a"), Bytes("1")},
			{Bytes("b"), Bytes("2")},
			{Bytes("c"), Bytes("3")},
			{Bytes("d"), Bytes("4")},
			{Bytes("e"), Bytes("5")},
			{Bytes("f"), Bytes("6")},
		}
		mem := InitMemtable(cfg)
		for i, v := range data {
			mem.Put(RecordImpl{
				Key:            v.key,
				Value:          v.value,
				SequenceNumber: uint64(i),
			})
		}
		sstable, err := Flush(cfg, mem, fs)
		assert.NoError(t, err)

		tests := []struct {
			name         string
			startKey     Bytes
			endKey       Bytes
			expectedKeys []Bytes
			expectError  bool
		}{
			{
				name:         "Full range",
				startKey:     Bytes("a"),
				endKey:       Bytes("f"),
				expectedKeys: []Bytes{Bytes("a"), Bytes("b"), Bytes("c"), Bytes("d"), Bytes("e"), Bytes("f")},
			},
			{
				name:         "Partial range (middle)",
				startKey:     Bytes("b"),
				endKey:       Bytes("d"),
				expectedKeys: []Bytes{Bytes("b"), Bytes("c"), Bytes("d")},
			},
			{
				name:         "Range starting at first key",
				startKey:     Bytes("a"),
				endKey:       Bytes("c"),
				expectedKeys: []Bytes{Bytes("a"), Bytes("b"), Bytes("c")},
			},
			{
				name:         "Range ending at last key",
				startKey:     Bytes("d"),
				endKey:       Bytes("f"),
				expectedKeys: []Bytes{Bytes("d"), Bytes("e"), Bytes("f")},
			},
			{
				name:         "Single element range",
				startKey:     Bytes("c"),
				endKey:       Bytes("c"),
				expectedKeys: []Bytes{Bytes("c")},
			},
			{
				name:         "Range with no matching keys (between existing)",
				startKey:     Bytes("b1"),
				endKey:       Bytes("b2"),
				expectedKeys: nil,
			},
			{
				name:         "Range completely before SSTable keys (lexicographically includes 'a')",
				startKey:     Bytes("0"),
				endKey:       Bytes("a0"),
				expectedKeys: []Bytes{Bytes("a")},
			},
			{
				name:         "Range completely after SSTable keys",
				startKey:     Bytes("g"),
				endKey:       Bytes("z"),
				expectedKeys: nil,
			},
			{
				name:         "Range overlapping start (start before, end within)",
				startKey:     Bytes("0"),
				endKey:       Bytes("b"),
				expectedKeys: []Bytes{Bytes("a"), Bytes("b")},
			},
			{
				name:         "Range overlapping end (start within, end after)",
				startKey:     Bytes("e"),
				endKey:       Bytes("z"),
				expectedKeys: []Bytes{Bytes("e"), Bytes("f")},
			},
			{
				name:         "Empty range (start > end)",
				startKey:     Bytes("d"),
				endKey:       Bytes("b"),
				expectedKeys: nil,
			},
			{
				name:         "Range with non-existent start key, but valid range",
				startKey:     Bytes("a0"),
				endKey:       Bytes("c"),
				expectedKeys: []Bytes{Bytes("b"), Bytes("c")},
			},
			{
				name:         "Range with non-existent end key, but valid range",
				startKey:     Bytes("c"),
				endKey:       Bytes("e0"),
				expectedKeys: []Bytes{Bytes("c"), Bytes("d"), Bytes("e")},
			},
			{
				name:         "Range with non-existent start and end keys, but valid range",
				startKey:     Bytes("b0"),
				endKey:       Bytes("e0"),
				expectedKeys: []Bytes{Bytes("c"), Bytes("d"), Bytes("e")},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Re-seek the file to the beginning for each test case
				_, err := sstable.file.Seek(0, io.SeekStart)
				assert.NoError(t, err)

				iterator, err := sstable.RangeIterator(tt.startKey, tt.endKey)
				if tt.expectError {
					assert.Error(t, err)
					return
				}
				assert.NoError(t, err)
				assert.NotNil(t, iterator)

				var actualKeys []Bytes
				for iterator.HasNext() {
					record, err := iterator.Next()
					assert.NoError(t, err)
					assert.NotNil(t, record)
					actualKeys = append(actualKeys, record.GetKey())
				}
				assert.Equal(t, tt.expectedKeys, actualKeys)

				// After iteration, Next() should return EOI
				record, err := iterator.Next()
				assert.ErrorIs(t, err, EOI)
				assert.Nil(t, record)
			})
		}
	})
}

// Test_genSparseIndex tests the generation of sparse index from memtable.
func Test_genSparseIndex(t *testing.T) {
	cfg := testConfig()
	mem := InitMemtable(cfg)
	mem.Put(RecordImpl{Bytes("1"), Bytes("2"), 1})
	mem.Put(RecordImpl{Bytes("2"), Bytes("3"), 2})
	mem.Put(RecordImpl{Bytes("3"), Bytes("4"), 3})
	index := genSparseIndex(mem)
	assert.Equal(t, Bytes("1"), index[0].key)
	assert.Equal(t, int64(0), index[0].offset)
	assert.Equal(t, Bytes("2"), index[1].key)
	assert.Equal(t, int64(26), index[1].offset)
	assert.Equal(t, Bytes("3"), index[2].key)
	assert.Equal(t, int64(52), index[2].offset)
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
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]
	k1 := randStringBytes(10)
	k2 := randStringBytes(10)
	mem := InitMemtable(cfg)
	mem.Put(RecordImpl{k1, Bytes("v1"), 1})
	mem.Put(RecordImpl{k2, nil, 2})
	sstable, err := Flush(cfg, mem, fs)
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
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]
	mem := InitMemtable(cfg)
	mem.Put(RecordImpl{Bytes("k1"), Bytes("v1"), 2})
	sstable, err := Flush(cfg, mem, fs)
	assert.NoError(t, err)
	_, err = sstable.GetValue(Bytes("k2"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	pos, err := fs.CursorPos()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pos, "File should not be read due to Bloom filter")
}
