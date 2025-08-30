package rindb

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		_, err := flush(context.Background(), cfg, mem, fs)
		assert.NoError(t, err)
	})
	t.Run("FlushWithElements", func(t *testing.T) {
		ctx := context.Background()
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
			mem.Put(newRecord(v.key, v.value, uint64(i)))
		}
		sstable, err := flush(ctx, cfg, mem, fs)
		assert.NoError(t, err)
		tailSSTableOffset, err := readTailSSTable(sstable.FileSystem)
		assert.NoError(t, err)
		reader := newOffsetReader(sstable.FileSystem, tailSSTableOffset)
		sparseIndexOffset, err := ReadNumber(reader)
		assert.NoError(t, err)
		assert.NotZero(t, sparseIndexOffset)
		reader = newOffsetReader(sstable.FileSystem, 0)
		expectedSparseIndex := make(SparseIndex, 0)
		ret := int64(0)
		idx := 0
		for ret < int64(sparseIndexOffset) {
			record, err := ReadRecord(reader)
			assert.NoError(t, err)
			assert.Equal(t, data[idx].key, record.GetKey())
			assert.Equal(t, data[idx].value, record.GetValue())
			expectedSparseIndex = append(expectedSparseIndex, KeyOffset{record.GetKey(), ret})
			ret = reader.Offset()
			idx++
		}
		idx = 0
		for ret < tailSSTableOffset {
			ko, err := readKeyOffset(reader)
			assert.NoError(t, err)
			assert.Equal(t, expectedSparseIndex[idx].key, ko.key)
			assert.Equal(t, expectedSparseIndex[idx].offset, ko.offset)
			ret = reader.Offset()
			idx++
		}
	})
	t.Run("SparseIndexLoad", func(t *testing.T) {
		ctx := context.Background()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]
		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("2"), Bytes("3"), 1))
		mem.Put(newRecord(Bytes("1"), Bytes("2"), 2))
		mem.Put(newRecord(Bytes("3"), Bytes("4"), 3))
		sstable1, err := flush(ctx, cfg, mem, fs)
		assert.NoError(t, err)
		sstable2, err := NewSSTable(ctx, cfg, fs)
		assert.NoError(t, err)
		assert.Equal(t, sstable1.SparseIndex, sstable2.SparseIndex)
	})
	t.Run("SparseIndexOffsetAccuracy", func(t *testing.T) {
		ctx := context.Background()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]
		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("2"), Bytes("3"), 1))
		mem.Put(newRecord(Bytes("1"), Bytes("2"), 2))
		mem.Put(newRecord(Bytes("3"), Bytes("4"), 3))
		_, err := flush(ctx, cfg, mem, fs)
		assert.NoError(t, err)
		sstable, err := NewSSTable(ctx, cfg, fs)
		assert.NoError(t, err)
		sparseIndex := sstable.SparseIndex
		for idx := len(sparseIndex) - 1; idx > -1; idx-- {
			keyOffset := sparseIndex[idx]
			reader := newOffsetReader(sstable.FileSystem, keyOffset.offset)
			record, err := ReadRecord(reader)
			assert.NoError(t, err)
			assert.Equal(t, keyOffset.key, record.GetKey())
		}
	})
	t.Run("FlushToSSTable", func(t *testing.T) {
		ctx := context.Background()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]
		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("2"), Bytes("3"), 1))
		mem.Put(newRecord(Bytes("1"), Bytes("2"), 2))
		mem.Put(newRecord(Bytes("3"), Bytes("4"), 3))
		assert.Equal(t, uint(3), mem.data.Len())
		sstable, err := flush(ctx, cfg, mem, fs)
		assert.NoError(t, err)
		assert.Equal(t, uint(0), mem.data.Len())
		value, err := sstable.GetValue(ctx, Bytes("2"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("3"), value)
		value, err = sstable.GetValue(ctx, Bytes("1"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("2"), value)
		value, err = sstable.GetValue(ctx, Bytes("3"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("4"), value)
		value, err = sstable.GetValue(ctx, Bytes(".3"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Equal(t, Bytes(nil), value)
	})
	t.Run("GetValueSequence", func(t *testing.T) {
		ctx := context.Background()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]
		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("a"), Bytes("old"), 1))
		mem.Put(newRecord(Bytes("a"), Bytes("new"), 2))
		sstable, err := flush(ctx, cfg, mem, fs)
		assert.NoError(t, err)
		value, err := sstable.GetValue(ctx, Bytes("a"), 1)
		assert.NoError(t, err)
		assert.Equal(t, Bytes("old"), value)
		value, err = sstable.GetValue(ctx, Bytes("a"))
		assert.NoError(t, err)
		assert.Equal(t, Bytes("new"), value)
	})
	t.Run("Iterator", func(t *testing.T) {
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]
		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("2"), Bytes("3"), 1))
		mem.Put(newRecord(Bytes("1"), Bytes("2"), 2))
		mem.Put(newRecord(Bytes("3"), Bytes("4"), 3))
		sstable, err := flush(context.Background(), cfg, mem, fs)
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
	t.Run("IRange", func(t *testing.T) {
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
			mem.Put(newRecord(v.key, v.value, uint64(i)))
		}
		sstable, err := flush(context.Background(), cfg, mem, fs)
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
				iterator, err := sstable.IRange(tt.startKey, tt.endKey)
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

// TestSSTableBuilder verifies that the builder writes sparse index and Bloom filter entries correctly.
func TestSSTableBuilder(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	recs := []Record{
		newRecord(Bytes("a"), Bytes("1"), 1),
		newRecord(Bytes("b"), Bytes("2"), 2),
		newRecord(Bytes("c"), Bytes("3"), 3),
	}
	builder, err := NewSSTableBuilder(ctx, cfg, fs, len(recs))
	assert.NoError(t, err)
	for _, r := range recs {
		assert.NoError(t, builder.Add(r))
	}

	sst, _, err := builder.Build(ctx)
	assert.NoError(t, err)

	// Verify sparse index entries and offsets
	var offset int64
	for i, r := range recs {
		assert.Equal(t, r.GetKey(), sst.SparseIndex[i].key)
		assert.Equal(t, offset, sst.SparseIndex[i].offset)
		assert.True(t, sst.Bloom.Lookup(r.GetKey()))
		offset += int64(CalOnDiskSize(r))
	}

	// Bloom filter should reject an unknown key
	assert.False(t, sst.Bloom.Lookup(Bytes("z")))
}

func TestSSTableBuilder_BuildEmpty(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	builder, err := NewSSTableBuilder(ctx, cfg, fs, 0)
	assert.NoError(t, err)

	sst, _, err := builder.Build(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, sst.Bloom)
	assert.False(t, sst.Bloom.Lookup(Bytes("a")))
}

func TestSSTableBuilder_AddEnforcesOrder(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	builder, err := NewSSTableBuilder(ctx, cfg, fs, 4)
	assert.NoError(t, err)

	// Increasing key order
	assert.NoError(t, builder.Add(newRecord(Bytes("a"), Bytes("1"), 2)))
	// Same key with lower sequence number is allowed
	assert.NoError(t, builder.Add(newRecord(Bytes("a"), Bytes("0"), 1)))
	// Non-decreasing sequence number for same key is rejected
	assert.Error(t, builder.Add(newRecord(Bytes("a"), Bytes("2"), 1)))
	// Keys must be non-decreasing
	assert.Error(t, builder.Add(newRecord(Bytes("0"), Bytes("3"), 3)))
}

func TestSSTableBuilder_TruncatesExistingFile(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	initial := bytes.Repeat([]byte("x"), 256)
	fss, closer := initTempFileSystems(t, 1, [][]byte{initial})
	defer closer()
	fs := fss[0]

	builder, err := NewSSTableBuilder(ctx, cfg, fs, 1)
	assert.NoError(t, err)
	assert.NoError(t, builder.Add(newRecord(Bytes("a"), Bytes("1"), 1)))

	_, written, err := builder.Build(ctx)
	assert.NoError(t, err)

	info, err := os.Stat(fs.Path())
	assert.NoError(t, err)
	assert.Equal(t, int64(written), info.Size())
}

// TestSSTableBuilder_Errors verifies that calling Add or Build after a successful build returns an error.
func TestSSTableBuilder_Errors(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	builder, err := NewSSTableBuilder(ctx, cfg, fs, 2)
	assert.NoError(t, err)

	rec := newRecord(Bytes("a"), Bytes("1"), 1)
	assert.NoError(t, builder.Add(rec))

	_, _, err = builder.Build(ctx)
	assert.NoError(t, err)

	t.Run("AddAfterBuild", func(t *testing.T) {
		err := builder.Add(newRecord(Bytes("b"), Bytes("2"), 2))
		assert.ErrorIs(t, err, ErrSSTableAlreadyBuilt)
	})

	t.Run("BuildTwice", func(t *testing.T) {
		_, _, err := builder.Build(ctx)
		assert.ErrorIs(t, err, ErrSSTableAlreadyBuilt)
	})
}

// TestSSTableBuilder_CleansOnWriteError ensures that a write failure triggers filesystem cleanup.
func TestSSTableBuilder_CleansOnWriteError(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "fail.sst")

	devFull, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
	if err != nil {
		t.Skip("/dev/full not available")
	}

	fs := &FileSystem{filePath: path, file: devFull}

	builder, err := NewSSTableBuilder(ctx, cfg, fs, 1)
	require.NoError(t, err)

	require.NoError(t, builder.Add(newRecord(Bytes("a"), Bytes("1"), 1)))

	_, _, err = builder.Build(ctx)
	require.Error(t, err)

	info, statErr := os.Stat(path)
	require.NoError(t, statErr)
	require.Equal(t, int64(0), info.Size())

	require.NoError(t, fs.Close())
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
	ctx := context.Background()
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]
	k1 := randStringBytes(10)
	k2 := randStringBytes(10)
	mem := InitMemtable(cfg)
	mem.Put(newRecord(k1, Bytes("v1"), 1))
	mem.Put(newRecord(k2, nil, 2))
	sstable, err := flush(ctx, cfg, mem, fs)
	assert.NoError(t, err)
	v1, err := sstable.GetValue(ctx, k1)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v1"), v1)
	v2, err := sstable.GetValue(ctx, k2)
	assert.ErrorIs(t, err, ErrTombstoneFound)
	assert.Nil(t, v2)
}

// TestBloomFilterSkipsReads tests that the Bloom filter skips unnecessary reads.
func TestBloomFilterSkipsReads(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]
	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("k1"), Bytes("v1"), 2))
	sstable, err := flush(ctx, cfg, mem, fs)
	assert.NoError(t, err)
	posBefore, err := fs.CursorPos()
	assert.NoError(t, err)
	_, err = sstable.GetValue(ctx, Bytes("k2"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	posAfter, err := fs.CursorPos()
	assert.NoError(t, err)
	assert.Equal(t, posBefore, posAfter, "File cursor should remain unchanged due to Bloom filter")
}
