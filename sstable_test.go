package rindb

import (
	"context"
	"encoding/binary"
	"io"
	"os"
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
		_, _, _ = flush(context.Background(), cfg, mem, fs)
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
		sstable, meta, err := flush(ctx, cfg, mem, fs)
		assert.NoError(t, err)
		require.NotZero(t, meta.Number)
		info, err := os.Stat(sstable.FileSystem.Path())
		assert.NoError(t, err)
		tail := info.Size() - footerSize
		f, err := readFooter(sstable.FileSystem, tail)
		assert.NoError(t, err)
		reader := newOffsetReader(sstable.FileSystem, 0)
		expectedSparseIndex := make(SparseIndex, 0)
		ret := int64(0)
		idx := 0
		for ret < int64(f.indexOffset) {
			record, err := ReadRecord(reader)
			assert.NoError(t, err)
			assert.Equal(t, data[idx].key, record.GetKey())
			assert.Equal(t, data[idx].value, record.GetValue())
			expectedSparseIndex = append(expectedSparseIndex, KeyOffset{record.GetKey(), ret})
			ret = reader.Offset()
			idx++
		}
		idx = 0
		limit := int64(f.indexOffset + f.indexSize)
		for ret < limit {
			ko, err := readKeyOffset(reader)
			assert.NoError(t, err)
			assert.Equal(t, expectedSparseIndex[idx].key, ko.key)
			assert.Equal(t, expectedSparseIndex[idx].offset, ko.offset)
			ret = reader.Offset()
			idx++
		}
		assert.Equal(t, limit, ret)
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
		sstable1, meta, err := flush(ctx, cfg, mem, fs)
		assert.NoError(t, err)
		require.NotZero(t, meta.Number)
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
		_, meta, err := flush(ctx, cfg, mem, fs)
		assert.NoError(t, err)
		require.NotZero(t, meta.Number)
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
		sstable, meta, err := flush(ctx, cfg, mem, fs)
		assert.NoError(t, err)
		require.NotZero(t, meta.Number)
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
		sstable, meta, err := flush(ctx, cfg, mem, fs)
		assert.NoError(t, err)
		require.NotZero(t, meta.Number)
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
		sstable, meta, err := flush(context.Background(), cfg, mem, fs)
		assert.NoError(t, err)
		require.NotZero(t, meta.Number)
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
		sstable, meta, err := flush(context.Background(), cfg, mem, fs)
		assert.NoError(t, err)
		require.NotZero(t, meta.Number)

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

// TestSStable_GetValueSparseIndexFallback ensures GetValue can retrieve keys
// when they are absent from the sparse index by scanning from the nearest
// preceding entry.
func TestSStable_GetValueSparseIndexFallback(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	data := []struct{ key, value Bytes }{
		{Bytes("a"), Bytes("1")},
		{Bytes("b"), Bytes("2")},
		{Bytes("c"), Bytes("3")},
		{Bytes("d"), Bytes("4")},
		{Bytes("e"), Bytes("5")},
	}
	mem := InitMemtable(cfg)
	for i, v := range data {
		mem.Put(newRecord(v.key, v.value, uint64(i)))
	}
	sst, meta, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)
	require.NotZero(t, meta.Number)

	// Reduce sparse index to only a subset of keys to simulate sparsity.
	orig := sst.SparseIndex
	sst.SparseIndex = SparseIndex{}
	for _, ko := range orig {
		if Compare(ko.key, Bytes("c")) == CmpEqual || Compare(ko.key, Bytes("e")) == CmpEqual {
			sst.SparseIndex = append(sst.SparseIndex, ko)
		}
	}

	// Insert a non-existent key into the Bloom filter to force a scan.
	sst.Bloom.Insert(Bytes("da"))

	tests := []struct {
		name    string
		key     Bytes
		value   Bytes
		wantErr error
	}{
		{
			name:  "existing key before first index entry",
			key:   Bytes("a"),
			value: Bytes("1"),
		},
		{
			name:  "existing key between index entries",
			key:   Bytes("d"),
			value: Bytes("4"),
		},
		{
			name:    "missing key between index entries",
			key:     Bytes("da"),
			wantErr: ErrKeyNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			off, err := sst.SparseIndex.GetOffset(tt.key)
			assert.ErrorIs(t, err, ErrKeyNotFound)
			assert.Equal(t, int64(0), off)

			val, err := sst.GetValue(ctx, tt.key)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, val)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.value, val)
			}
		})
	}
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
	sstable, meta, err := flush(ctx, cfg, mem, fs)
	assert.NoError(t, err)
	require.NotZero(t, meta.Number)
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
	sstable, meta, err := flush(ctx, cfg, mem, fs)
	assert.NoError(t, err)
	require.NotZero(t, meta.Number)
	posBefore, err := fs.CursorPos()
	assert.NoError(t, err)
	_, err = sstable.GetValue(ctx, Bytes("k2"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	posAfter, err := fs.CursorPos()
	assert.NoError(t, err)
	assert.Equal(t, posBefore, posAfter, "File cursor should remain unchanged due to Bloom filter")
}

// TestSStableChecksumMismatch verifies that corrupted checksums are reported.
func TestSStableChecksumMismatch(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]
	mem := InitMemtable(cfg)
	rec := newRecord(Bytes("a"), Bytes("1"), 1)
	mem.Put(rec)
	sstable, meta, err := flush(ctx, cfg, mem, fs)
	assert.NoError(t, err)
	require.NotZero(t, meta.Number)

	offset := int64(CalOnDiskSize(rec)) - checksumSize
	_, err = fs.WriteAt([]byte{0}, offset)
	assert.NoError(t, err)

	_, err = sstable.GetValue(ctx, Bytes("a"))
	assert.ErrorIs(t, err, ErrChecksumMismatch)
}

func TestFooterRoundTrip(t *testing.T) {
	tx, cleanup := newFileTx(t)
	defer cleanup()
	want := footer{indexOffset: 10, indexSize: 20, magic: magicNumber}
	err := writeFooter(tx, want)
	require.NoError(t, err)
	got, err := readFooter(tx.log, 0)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestReadFooterErrors(t *testing.T) {
	t.Run("InvalidMagic", func(t *testing.T) {
		tx, cleanup := newFileTx(t)
		defer cleanup()
		err := writeFooter(tx, footer{indexOffset: 1, indexSize: 2, magic: 0})
		require.NoError(t, err)
		_, err = readFooter(tx.log, 0)
		assert.ErrorIs(t, err, ErrMalFormedSSTable)
	})

	t.Run("InvalidPadding", func(t *testing.T) {
		tx, cleanup := newFileTx(t)
		defer cleanup()
		err := writeFooter(tx, footer{indexOffset: 1, indexSize: 2, magic: magicNumber})
		require.NoError(t, err)
		_, err = tx.log.WriteAt([]byte{1}, 16)
		require.NoError(t, err)
		_, err = readFooter(tx.log, 0)
		assert.ErrorIs(t, err, ErrMalFormedSSTable)
	})

	t.Run("ShortRead", func(t *testing.T) {
		tx, cleanup := newFileTx(t)
		defer cleanup()
		_, err := readFooter(tx.log, 0)
		assert.ErrorIs(t, err, io.EOF)
	})
}

func TestNewSSTableInvalidFooter(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()

	t.Run("OffsetTooLarge", func(t *testing.T) {
		tx, cleanup := newFileTx(t)
		defer cleanup()
		err := writeFooter(tx, footer{indexOffset: 100, indexSize: 0, magic: magicNumber})
		require.NoError(t, err)
		_, err = NewSSTable(ctx, cfg, tx.log)
		assert.ErrorIs(t, err, ErrMalFormedSSTable)
	})

	t.Run("IndexOverlapsFooter", func(t *testing.T) {
		tx, cleanup := newFileTx(t)
		defer cleanup()
		_, err := tx.write([]byte("data"))
		require.NoError(t, err)
		err = writeFooter(tx, footer{indexOffset: 1, indexSize: 10, magic: magicNumber})
		require.NoError(t, err)
		_, err = NewSSTable(ctx, cfg, tx.log)
		assert.ErrorIs(t, err, ErrMalFormedSSTable)
	})

	t.Run("ExtraBytesBetweenIndexAndFooter", func(t *testing.T) {
		tx, cleanup := newFileTx(t)
		defer cleanup()

		// write one record at offset 0
		err := writeRecord(tx, newRecord(Bytes("a"), Bytes("v"), 1))
		require.NoError(t, err)

		indexOffset := tx.size()
		err = writeKeyOffset(tx, KeyOffset{key: Bytes("a"), offset: 0})
		require.NoError(t, err)
		indexSize := tx.size() - indexOffset

		// insert extra bytes between index and footer
		_, err = tx.write([]byte{0})
		require.NoError(t, err)

		err = writeFooter(tx, footer{indexOffset: uint64(indexOffset), indexSize: uint64(indexSize), magic: magicNumber})
		require.NoError(t, err)

		_, err = NewSSTable(ctx, cfg, tx.log)
		assert.ErrorIs(t, err, ErrMalFormedSSTable)
	})
}

func TestNewSSTableEmptyIndex(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "OnlyFooter",
			data: nil,
		},
		{
			name: "DataWithoutIndex",
			data: []byte("data"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, cleanup := newFileTx(t)
			defer cleanup()

			if tt.data != nil {
				_, err := tx.write(tt.data)
				require.NoError(t, err)
			}

			err := writeFooter(tx, footer{indexOffset: uint64(len(tt.data)), indexSize: 0, magic: magicNumber})
			require.NoError(t, err)

			_, err = NewSSTable(ctx, cfg, tx.log)
			assert.ErrorIs(t, err, ErrMalFormedSSTable)
		})
	}
}

func TestLoadSparseIndexSizeMismatch(t *testing.T) {
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	key := Bytes("a")
	off := int64(123)
	buf := make([]byte, 8+len(key)+8)
	binary.BigEndian.PutUint64(buf[0:8], uint64(len(key)))
	copy(buf[8:], key)
	binary.BigEndian.PutUint64(buf[8+len(key):], uint64(off))

	_, err := fs.Write(buf)
	require.NoError(t, err)

	_, err = loadSparseIndex(fs, 0, int64(len(buf)-1))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMalFormedSSTable)
	assert.Contains(t, err.Error(), "mismatched sparse index size")
}

// TestSSTableIRangeGetValueConsistency verifies that IRange and GetValue
// return consistent key/value pairs across different SSTable contents.
func TestSSTableIRangeGetValueConsistency(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()

	type kv struct {
		key Bytes
		val Bytes
		seq uint64
	}

	tests := []struct {
		name     string
		records  []kv
		seq      uint64
		expected []kv
	}{
		{
			name: "all values",
			seq:  3,
			records: []kv{
				{Bytes("1"), Bytes("1"), 1},
				{Bytes("2"), Bytes("2"), 2},
				{Bytes("3"), Bytes("3"), 3},
			},
			expected: []kv{
				{Bytes("1"), Bytes("1"), 1},
				{Bytes("2"), Bytes("2"), 2},
				{Bytes("3"), Bytes("3"), 3},
			},
		},
		{
			name: "leading tombstone",
			seq:  3,
			records: []kv{
				{Bytes("1"), nil, 1},
				{Bytes("2"), Bytes("2"), 2},
				{Bytes("3"), Bytes("3"), 3},
			},
			expected: []kv{
				{Bytes("1"), nil, 1},
				{Bytes("2"), Bytes("2"), 2},
				{Bytes("3"), Bytes("3"), 3},
			},
		},
		{
			name: "middle tombstone",
			seq:  3,
			records: []kv{
				{Bytes("1"), Bytes("1"), 1},
				{Bytes("2"), nil, 2},
				{Bytes("3"), Bytes("3"), 3},
			},
			expected: []kv{
				{Bytes("1"), Bytes("1"), 1},
				{Bytes("2"), nil, 2},
				{Bytes("3"), Bytes("3"), 3},
			},
		},
		{
			name: "trailing tombstone",
			seq:  3,
			records: []kv{
				{Bytes("1"), Bytes("1"), 1},
				{Bytes("2"), Bytes("2"), 2},
				{Bytes("3"), nil, 3},
			},
			expected: []kv{
				{Bytes("1"), Bytes("1"), 1},
				{Bytes("2"), Bytes("2"), 2},
				{Bytes("3"), nil, 3},
			},
		},
		{
			name: "all tombstones",
			seq:  3,
			records: []kv{
				{Bytes("1"), nil, 1},
				{Bytes("2"), nil, 2},
				{Bytes("3"), nil, 3},
			},
			expected: []kv{
				{Bytes("1"), nil, 1},
				{Bytes("2"), nil, 2},
				{Bytes("3"), nil, 3},
			},
		},
		{
			name: "duplicate key with tombstone",
			seq:  4,
			records: []kv{
				{Bytes("1"), Bytes("old"), 1},
				{Bytes("1"), nil, 2},
				{Bytes("1"), Bytes("new"), 3},
				{Bytes("2"), Bytes("2"), 4},
			},
			expected: []kv{
				{Bytes("1"), Bytes("new"), 3},
				{Bytes("1"), nil, 2},
				{Bytes("1"), Bytes("old"), 1},
				{Bytes("2"), Bytes("2"), 4},
			},
		},
		{
			name: "duplicate key with tombstone filtered",
			seq:  2,
			records: []kv{
				{Bytes("1"), Bytes("old"), 1},
				{Bytes("1"), nil, 2},
				{Bytes("1"), Bytes("new"), 3},
				{Bytes("2"), Bytes("2"), 4},
			},
			expected: []kv{
				{Bytes("1"), nil, 2},
				{Bytes("1"), Bytes("old"), 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fss, closer := initTempFileSystems(t, 1, nil)
			defer closer()
			fs := fss[0]

			mem := InitMemtable(cfg)
			for _, r := range tt.records {
				mem.Put(newRecord(r.key, r.val, r.seq))
			}

			sst, meta, err := flush(ctx, cfg, mem, fs)
			require.NoError(t, err)
			require.NotZero(t, meta.Number)

			it, err := sst.IRange(tt.records[0].key, tt.records[len(tt.records)-1].key, tt.seq)
			require.NoError(t, err)

			idx := 0
			seen := make(map[string]bool)
			for it.HasNext() {
				rec, err := it.Next()
				require.NoError(t, err)
				expected := tt.expected[idx]
				assert.Equal(t, expected.key, rec.GetKey())
				assert.Equal(t, expected.seq, rec.GetSequenceNumber())
				if expected.val == nil {
					assert.Nil(t, rec.GetValue())
					assert.Equal(t, TypeDeletion, rec.GetType())
				} else {
					assert.Equal(t, expected.val, rec.GetValue())
					assert.Equal(t, TypeValue, rec.GetType())
				}

				keyStr := string(rec.GetKey())
				if !seen[keyStr] {
					gv, err := sst.GetValue(ctx, rec.GetKey(), tt.seq)
					if expected.val == nil {
						assert.ErrorIs(t, err, ErrTombstoneFound)
						assert.Nil(t, gv)
					} else {
						assert.NoError(t, err)
						assert.Equal(t, expected.val, gv)
					}
					seen[keyStr] = true
				}
				idx++
			}
			assert.Equal(t, len(tt.expected), idx)
		})
	}
}

func TestSSTableIRangePrepareEOF(t *testing.T) {
	content := []byte{1, 2, 3, 4}
	tests := []struct {
		name     string
		contents [][]byte
		dataEnd  int64
		err      error
	}{
		{
			name:     "EOI",
			contents: nil,
			dataEnd:  1,
			err:      EOI,
		},
		{
			name:     "UnexpectedEOF",
			contents: [][]byte{content},
			dataEnd:  8,
			err:      io.ErrUnexpectedEOF,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fss, closer := initTempFileSystems(t, 1, tt.contents)
			defer closer()
			fs := fss[0]
			sst := SStable{FileSystem: fs}
			sri := &sstableIRange{s: &sst, startKey: Bytes("a"), endKey: Bytes("b"), seq: 1, offset: 0, dataEnd: tt.dataEnd}

			assert.False(t, sri.HasNext())
			_, err := sri.Next()
			assert.ErrorIs(t, err, tt.err)
		})
	}
}
