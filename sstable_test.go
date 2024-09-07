package rindb

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

//nolint:funlen
func TestSStable(t *testing.T) {
	t.Run("Write empty memtable", func(t *testing.T) {
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

	t.Run("Write a memtable with few elements", func(t *testing.T) {
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
		for {
			if !(ret < int64(sparseIndexOffset)) {
				break
			}
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
		for {
			if !(ret < tailSSTableOffset) {
				break
			}
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

	t.Run("Loaded Sparse index should be the same with calculated index", func(t *testing.T) {
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

	t.Run("Sparse index must reflect accurate offset", func(t *testing.T) {
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

	t.Run("Flush memtable to file system and return sstable", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1)
		defer closer()

		fs := fss[0]

		mem := InitMemtable()
		mem.Put(Bytes("2"), Bytes("3"))
		mem.Put(Bytes("1"), Bytes("2"))
		mem.Put(Bytes("3"), Bytes("4"))
		// TODO: use size/len method of memtable instead
		assert.Equal(t, uint(3), mem.data.Len())

		sstable, err := Flush(mem, fs)
		assert.NoError(t, err)

		// TODO: use size/len method of memtable instead
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

	t.Run("Iterator for sstable", func(t *testing.T) {
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

func Test_genSparseIndex(t *testing.T) {
	mem := InitMemtable()
	mem.Put(Bytes("1"), Bytes("2"))
	mem.Put(Bytes("2"), Bytes("3"))
	mem.Put(Bytes("3"), Bytes("4"))

	index := genSparseIndex(mem)
	assert.Equal(t, Bytes("1"), index[0].key)
	assert.Equal(t, int64(0), index[0].offset)

	assert.Equal(t, Bytes("2"), index[1].key)
	// 18 = 0(previous offset) + 8(size of len key) + 8(size of len value) + 1(len of "1") + 1(len of "2")
	assert.Equal(t, int64(18), index[1].offset)

	assert.Equal(t, Bytes("3"), index[2].key)
	// 36 = 18(previous offset) + 8(size of len key) + 8(size of len value) + 1(len of "1") + 1(len of "2")
	assert.Equal(t, int64(36), index[2].offset)
}

//nolint:funlen
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
