package rindb

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSTableIterator_ReverseIteration(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc"), 3))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.Iterator()
	require.NoError(t, err)

	var fwd []Bytes
	for it.HasNext() {
		rec, err := it.Next()
		require.NoError(t, err)
		fwd = append(fwd, rec.GetKey())
	}

	var rev []Bytes
	for it.HasPrev() {
		rec, err := it.Prev()
		require.NoError(t, err)
		rev = append(rev, rec.GetKey())
	}

	assert.Equal(t, []Bytes{Bytes("a"), Bytes("b"), Bytes("c")}, fwd)
	assert.Equal(t, []Bytes{Bytes("c"), Bytes("b"), Bytes("a")}, rev)
}

func TestSSTableIterator_MixedDirectionIteration(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb"), 2))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.Iterator()
	require.NoError(t, err)

	assert.True(t, it.HasNext())
	rec, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())

	assert.True(t, it.HasPrev())
	rec, err = it.Prev()
	require.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())

	assert.True(t, it.HasNext())
	rec, err = it.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())

	assert.True(t, it.HasNext())
	rec, err = it.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("b"), rec.GetKey())

	assert.True(t, it.HasPrev())
	rec, err = it.Prev()
	require.NoError(t, err)
	assert.Equal(t, Bytes("b"), rec.GetKey())

	assert.True(t, it.HasPrev())
	rec, err = it.Prev()
	require.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())
	assert.False(t, it.HasPrev())
}

func TestSSTableIterator_Last(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc"), 3))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.Iterator()
	require.NoError(t, err)

	last, err := it.Last()
	require.NoError(t, err)
	assert.Equal(t, Bytes("c"), last.GetKey())
	assert.Equal(t, Bytes("vc"), last.GetValue())

	prev, err := it.Prev()
	require.NoError(t, err)
	assert.Equal(t, Bytes("b"), prev.GetKey())
}

func TestSSTableIterator_LastPrevOffsetEOF(t *testing.T) {
	t.Parallel()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	_, err := fs.WriteAt(make([]byte, mdByteSize), 0)
	require.NoError(t, err)

	iter := &sstableIterator{FileSystem: fs, dataEnd: int64(2 * mdByteSize)}

	_, err = iter.Last()
	require.ErrorIs(t, err, EOI)
}

func TestSSTableIterator_LastHandlesEOFRecord(t *testing.T) {
	t.Parallel()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	buf := make([]byte, mdByteSize)
	byteOrder.PutUint64(buf, uint64(mdByteSize))
	_, err := fs.WriteAt(buf, 0)
	require.NoError(t, err)

	iter := &sstableIterator{FileSystem: fs, dataEnd: int64(len(buf))}

	_, err = iter.Last()
	require.ErrorIs(t, err, EOI)
}

func TestSSTableIterator_LastPropagatesError(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	first := newRecord(Bytes("a"), Bytes("va"), 1)
	second := newRecord(Bytes("b"), Bytes("vb"), 2)
	mem.Put(first)
	mem.Put(second)

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	iterIface, err := sst.Iterator()
	require.NoError(t, err)
	iter := iterIface.(*sstableIterator)

	lastOffset := sst.SparseIndex[len(sst.SparseIndex)-1].offset
	checksumOffset := lastOffset + 2*int64(mdByteSize) + int64(len(EncodeInternalKey(second.GetKey(), second.GetSequenceNumber(), second.GetType()))) + int64(len(second.GetValue()))
	buf := make([]byte, checksumSize)
	_, err = fs.ReadAt(buf, checksumOffset)
	require.NoError(t, err)
	buf[0] ^= 0xFF
	_, err = fs.WriteAt(buf, checksumOffset)
	require.NoError(t, err)

	_, err = iter.Last()
	require.ErrorIs(t, err, ErrChecksumMismatch)
}

func TestSSTableIterator_NextReturnsEOIOnEOF(t *testing.T) {
	t.Parallel()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	iter := &sstableIterator{FileSystem: fs, dataEnd: int64(mdByteSize), offset: 0}

	_, err := iter.Next()
	require.ErrorIs(t, err, EOI)
}

func TestSSTableIterator_NextPropagatesError(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	rec := newRecord(Bytes("a"), Bytes("value"), 1)

	mem := InitMemtable(cfg)
	mem.Put(rec)

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	iterIface, err := sst.Iterator()
	require.NoError(t, err)
	iter := iterIface.(*sstableIterator)

	checksumOffset := int64(2*mdByteSize) + int64(len(EncodeInternalKey(rec.GetKey(), rec.GetSequenceNumber(), rec.GetType()))) + int64(len(rec.GetValue()))
	buf := make([]byte, checksumSize)
	_, err = fs.ReadAt(buf, checksumOffset)
	require.NoError(t, err)
	buf[0] ^= 0xFF
	_, err = fs.WriteAt(buf, checksumOffset)
	require.NoError(t, err)

	_, err = iter.Next()
	require.ErrorIs(t, err, ErrChecksumMismatch)
}

func TestSSTable_findOffsetLE(t *testing.T) {
	t.Parallel()

	sst := SStable{
		SparseIndex: SparseIndex{
			{key: Bytes("b"), offset: 10},
			{key: Bytes("d"), offset: 20},
			{key: Bytes("f"), offset: 30},
		},
		dataEnd: 40,
	}

	t.Run("returns-first-offset-when-before-range", func(t *testing.T) {
		prev, next, ok := sst.findOffsetLE(Bytes("aa"))
		require.True(t, ok)
		assert.Equal(t, int64(0), prev)
		assert.Equal(t, int64(10), next)
	})

	t.Run("returns-adjacent-offsets-for-middle-key", func(t *testing.T) {
		prev, next, ok := sst.findOffsetLE(Bytes("e"))
		require.True(t, ok)
		assert.Equal(t, int64(20), prev)
		assert.Equal(t, int64(30), next)
	})

	t.Run("returns-last-offset-when-after-range", func(t *testing.T) {
		prev, next, ok := sst.findOffsetLE(Bytes("g"))
		require.True(t, ok)
		assert.Equal(t, int64(30), prev)
		assert.Equal(t, int64(40), next)
	})
}

func TestSSTableIRange_ReverseIteration(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc"), 3))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.IRange(Bytes("a"), Bytes("c"), RangeAsc)
	require.NoError(t, err)

	var fwd []Bytes
	for it.HasNext() {
		rec, err := it.Next()
		require.NoError(t, err)
		fwd = append(fwd, rec.GetKey())
	}

	var rev []Bytes
	for it.HasPrev() {
		rec, err := it.Prev()
		require.NoError(t, err)
		rev = append(rev, rec.GetKey())
	}

	assert.Equal(t, []Bytes{Bytes("a"), Bytes("b"), Bytes("c")}, fwd)
	assert.Equal(t, []Bytes{Bytes("c"), Bytes("b"), Bytes("a")}, rev)
}

func TestSSTableIRange_DescendingNext(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	for idx, key := range []string{"a", "b", "c", "d"} {
		mem.Put(newRecord(Bytes(key), Bytes("v"+key), uint64(idx+1)))
	}

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.IRange(Bytes("a"), Bytes("d"), RangeDesc)
	require.NoError(t, err)

	var keys []Bytes
	for it.HasNext() {
		rec, err := it.Next()
		require.NoError(t, err)
		keys = append(keys, rec.GetKey())
	}

	assert.Equal(t, []Bytes{Bytes("d"), Bytes("c"), Bytes("b"), Bytes("a")}, keys)
}

func TestSSTableIRange_DescendingPriming(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	for idx, key := range []string{"a", "b", "c"} {
		mem.Put(newRecord(Bytes(key), Bytes("v"+key), uint64(idx+1)))
	}

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.IRange(Bytes("a"), Bytes("c"), RangeDesc)
	require.NoError(t, err)

	_, err = it.Prev()
	require.ErrorIs(t, err, EOI)

	rec, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("c"), rec.GetKey())

	rec, err = it.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("b"), rec.GetKey())

	rec, err = it.Prev()
	require.NoError(t, err)
	assert.Equal(t, Bytes("b"), rec.GetKey())

	rec, err = it.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("b"), rec.GetKey())

	rec, err = it.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())

	_, err = it.Next()
	require.ErrorIs(t, err, EOI)

	rec, err = it.Prev()
	require.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())
}

func TestSSTableIRange_DescendingOscillation(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc"), 3))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.IRange(Bytes("a"), Bytes("c"), RangeDesc)
	require.NoError(t, err)

	rec, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("c"), rec.GetKey())

	rec, err = it.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("b"), rec.GetKey())

	rec, err = it.Prev()
	require.NoError(t, err)
	assert.Equal(t, Bytes("b"), rec.GetKey())

	rec, err = it.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("b"), rec.GetKey())

	rec, err = it.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())
}

func TestSSTableIRange_Last(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va1"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb3"), 3))
	mem.Put(newRecord(Bytes("b"), Bytes("vb2"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc5"), 5))
	mem.Put(newRecord(Bytes("c"), Bytes("vc1"), 1))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.IRange(Bytes("a"), Bytes("c"), RangeAsc, 2)
	require.NoError(t, err)

	last, err := it.Last()
	require.NoError(t, err)
	assert.Equal(t, Bytes("c"), last.GetKey())
	assert.Equal(t, uint64(1), last.GetSequenceNumber())

	prev, err := it.Prev()
	require.NoError(t, err)
	assert.Equal(t, Bytes("b"), prev.GetKey())
	assert.Equal(t, uint64(2), prev.GetSequenceNumber())
}

func TestSSTableIterator_PrevFullTraversal(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	var expected []Bytes
	for i := 0; i < 10; i++ {
		key := Bytes(fmt.Sprintf("k%02d", i))
		mem.Put(newRecord(key, Bytes("v"), uint64(i+1)))
		expected = append(expected, key)
	}

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.Iterator()
	require.NoError(t, err)

	var fwd []Bytes
	for it.HasNext() {
		rec, err := it.Next()
		require.NoError(t, err)
		fwd = append(fwd, rec.GetKey())
	}

	assert.Equal(t, expected, fwd)

	for i := len(fwd) - 1; i >= 0; i-- {
		require.True(t, it.HasPrev())
		rec, err := it.Prev()
		require.NoError(t, err)
		assert.Equal(t, fwd[i], rec.GetKey())
	}

	assert.False(t, it.HasPrev())
}

func TestSSTableIRange_PrevFullTraversal(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	keys := []string{"a", "b", "c", "d", "e", "f"}
	for i, k := range keys {
		mem.Put(newRecord(Bytes(k), Bytes("v"), uint64(i+1)))
	}

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.IRange(Bytes("b"), Bytes("e"), RangeAsc)
	require.NoError(t, err)

	var fwd []Bytes
	for it.HasNext() {
		rec, err := it.Next()
		require.NoError(t, err)
		fwd = append(fwd, rec.GetKey())
	}

	assert.Equal(t, []Bytes{Bytes("b"), Bytes("c"), Bytes("d"), Bytes("e")}, fwd)

	for i := len(fwd) - 1; i >= 0; i-- {
		require.True(t, it.HasPrev())
		rec, err := it.Prev()
		require.NoError(t, err)
		assert.Equal(t, fwd[i], rec.GetKey())
	}

	assert.False(t, it.HasPrev())
}

func TestSSTableIRange_PrevRespectsSequenceFilter(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb"), 4))
	mem.Put(newRecord(Bytes("c"), Bytes("vc"), 1))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	iterIface, err := sst.IRange(Bytes("a"), Bytes("z"), RangeAsc, 1)
	require.NoError(t, err)
	iter := iterIface.(*sstableIRange)

	var forward []Bytes
	for iter.HasNext() {
		rec, err := iter.Next()
		require.NoError(t, err)
		forward = append(forward, rec.GetKey())
	}
	require.Equal(t, []Bytes{Bytes("a"), Bytes("c")}, forward)

	require.True(t, iter.HasPrev())
	rec, err := iter.Prev()
	require.NoError(t, err)
	require.Equal(t, Bytes("c"), rec.GetKey())

	require.True(t, iter.HasPrev())
	rec, err = iter.Prev()
	require.NoError(t, err)
	require.Equal(t, Bytes("a"), rec.GetKey(), "Prev should return the correct record ('a') and not a version filtered by the seq bound")

	require.False(t, iter.HasPrev())
}

func TestSSTableIterator_PrevOffsetErrorPropagation(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	rec := newRecord(Bytes("a"), Bytes("va"), 1)
	mem.Put(rec)

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	iterIface, err := sst.Iterator()
	require.NoError(t, err)
	iter := iterIface.(*sstableIterator)

	got, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, rec.GetKey(), got.GetKey())

	endOffset := iter.offset
	zeroTrailer := make([]byte, mdByteSize)
	_, err = fs.WriteAt(zeroTrailer, endOffset-int64(mdByteSize))
	require.NoError(t, err)

	_, err = iter.Prev()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid record size trailer")
}

func TestSSTableIterator_PrevReadEOFError(t *testing.T) {
	t.Parallel()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	buf := make([]byte, mdByteSize*2)
	byteOrder.PutUint64(buf[:mdByteSize], uint64(internalKeySuffixLen+1))
	byteOrder.PutUint64(buf[mdByteSize:], uint64(len(buf)))
	_, err := fs.WriteAt(buf, 0)
	require.NoError(t, err)

	iter := &sstableIterator{FileSystem: fs, offset: int64(len(buf)), dataEnd: int64(len(buf))}
	require.True(t, iter.HasPrev())

	_, err = iter.Prev()
	require.ErrorIs(t, err, EOI)
}

func TestSSTableIterator_PrevHandlesPrevOffsetEOF(t *testing.T) {
	t.Parallel()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	_, err := fs.WriteAt(make([]byte, mdByteSize), 0)
	require.NoError(t, err)

	iter := &sstableIterator{FileSystem: fs, offset: int64(2 * mdByteSize), cursor: int64(2 * mdByteSize), dataEnd: int64(2 * mdByteSize)}

	_, err = iter.Prev()
	require.ErrorIs(t, err, EOI)
}

func TestSSTableIterator_PrevPropagatesPrevOffsetError(t *testing.T) {
	t.Parallel()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	_, err := fs.WriteAt(make([]byte, mdByteSize/2), 0)
	require.NoError(t, err)

	iter := &sstableIterator{FileSystem: fs, offset: int64(mdByteSize / 2), cursor: int64(mdByteSize / 2), dataEnd: int64(mdByteSize / 2)}

	_, err = iter.Prev()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "smaller than trailer size")
}

func TestSSTableIRange_PrepareErrorPropagation(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	rec := newRecord(Bytes("a"), Bytes("value"), 1)
	mem.Put(rec)

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	iterIface, err := sst.IRange(Bytes("a"), Bytes("z"), RangeAsc)
	require.NoError(t, err)
	iter := iterIface.(*sstableIRange)

	startOffset := iter.offset
	checksumOffset := startOffset + 2*int64(mdByteSize) + int64(len(EncodeInternalKey(rec.GetKey(), rec.GetSequenceNumber(), rec.GetType()))) + int64(len(rec.GetValue()))
	buf := make([]byte, checksumSize)
	_, err = fs.ReadAt(buf, checksumOffset)
	require.NoError(t, err)
	buf[0] ^= 0xFF
	_, err = fs.WriteAt(buf, checksumOffset)
	require.NoError(t, err)

	assert.False(t, iter.HasNext())
	_, err = iter.Next()
	require.ErrorIs(t, err, ErrChecksumMismatch)
}

func TestSSTableIRange_PrevOffsetEOFError(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	rec := newRecord(Bytes("a"), Bytes("value"), 1)
	mem.Put(rec)

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	iterIface, err := sst.IRange(Bytes("a"), Bytes("z"), RangeAsc)
	require.NoError(t, err)
	iter := iterIface.(*sstableIRange)

	require.True(t, iter.HasNext())
	got, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, rec.GetKey(), got.GetKey())

	cursor := iter.cursor
	require.True(t, iter.HasPrev())
	require.NoError(t, fs.file.Truncate(cursor-1))

	_, err = iter.Prev()
	require.ErrorIs(t, err, EOI)
}

func TestSSTableIRange_PrevReadEOFError(t *testing.T) {
	t.Parallel()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	buf := make([]byte, mdByteSize*2)
	byteOrder.PutUint64(buf[:mdByteSize], uint64(internalKeySuffixLen+1))
	byteOrder.PutUint64(buf[mdByteSize:], uint64(len(buf)))
	_, err := fs.WriteAt(buf, 0)
	require.NoError(t, err)

	sst := &SStable{FileSystem: fs}
	iter := &sstableIRange{
		s:              sst,
		cursor:         int64(len(buf)),
		lowerBound:     0,
		haveLowerBound: true,
		dataEnd:        int64(len(buf)),
	}
	require.True(t, iter.HasPrev())

	_, err = iter.Prev()
	require.ErrorIs(t, err, EOI)
}

func TestSSTableIRange_DescendingReplayAdvancesCursor(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc"), 3))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	it, err := sst.IRange(Bytes("a"), Bytes("c"), RangeDesc)
	require.NoError(t, err)

	rec, err := it.Next()
	require.NoError(t, err)
	require.Equal(t, Bytes("c"), rec.GetKey())

	rec, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, Bytes("b"), rec.GetKey())

	rec, err = it.Prev()
	require.NoError(t, err)
	require.Equal(t, Bytes("b"), rec.GetKey(), "prev should surface the boundary record once when switching directions")

	rec, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, Bytes("b"), rec.GetKey(), "next should replay the boundary record after rewinding")

	rec, err = it.Prev()
	require.NoError(t, err)
	require.Equal(t, Bytes("b"), rec.GetKey(), "prev should not skip the replayed boundary record")
}

func TestSSTableIRange_DescendingOffsetFallback(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb"), 2))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	sst.SparseIndex = nil

	iterIface, err := sst.IRange(Bytes("a"), Bytes("z"), RangeDesc)
	require.NoError(t, err)

	iter := iterIface.(*sstableIRange)
	assert.Equal(t, iter.dataEnd, iter.offset)
	assert.Equal(t, iter.dataEnd, iter.cursor)
}

func TestSSTableIRange_PrimeNextDescendingPrevOffsetErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		offset      int64
		dataEnd     int64
		payloadSize int
		wantErr     error
		errContains string
	}{
		{
			name:        "prev-offset-eof",
			offset:      int64(2 * mdByteSize),
			dataEnd:     int64(2 * mdByteSize),
			payloadSize: mdByteSize,
			wantErr:     EOI,
		},
		{
			name:        "prev-offset-invalid",
			offset:      int64(mdByteSize / 2),
			dataEnd:     int64(mdByteSize / 2),
			payloadSize: mdByteSize / 2,
			errContains: "smaller than trailer size",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fss, closer := initTempFileSystems(t, 1, nil)
			defer closer()
			fs := fss[0]

			if tc.payloadSize > 0 {
				_, err := fs.WriteAt(make([]byte, tc.payloadSize), 0)
				require.NoError(t, err)
			}

			sri := &sstableIRange{
				s:        &SStable{FileSystem: fs},
				startKey: Bytes("a"),
				endKey:   Bytes("z"),
				seq:      math.MaxUint64,
				offset:   tc.offset,
				dataEnd:  tc.dataEnd,
				order:    RangeDesc,
			}

			sri.primeNextDescending()

			if tc.wantErr != nil {
				require.ErrorIs(t, sri.err, tc.wantErr)
			}
			if tc.errContains != "" {
				require.Error(t, sri.err)
				assert.Contains(t, sri.err.Error(), tc.errContains)
			}
		})
	}
}

func TestSSTableIRange_PrimeNextDescendingRecordErrors(t *testing.T) {
	t.Parallel()

	t.Run("io-eof", func(t *testing.T) {
		t.Parallel()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]

		buf := make([]byte, mdByteSize)
		byteOrder.PutUint64(buf, uint64(mdByteSize))
		_, err := fs.WriteAt(buf, 0)
		require.NoError(t, err)

		sri := &sstableIRange{
			s:        &SStable{FileSystem: fs},
			startKey: Bytes("a"),
			endKey:   Bytes("z"),
			seq:      math.MaxUint64,
			offset:   int64(len(buf)),
			dataEnd:  int64(len(buf)),
			order:    RangeDesc,
		}

		sri.primeNextDescending()
		require.ErrorIs(t, sri.err, EOI)
	})

	t.Run("checksum-error", func(t *testing.T) {
		t.Parallel()
		cfg := testConfig(t)
		ctx := context.Background()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]

		rec := newRecord(Bytes("a"), Bytes("va"), 1)
		mem := InitMemtable(cfg)
		mem.Put(rec)

		sst, _, err := flush(ctx, cfg, mem, fs)
		require.NoError(t, err)

		iterIface, err := sst.IRange(Bytes("a"), Bytes("z"), RangeDesc)
		require.NoError(t, err)
		sri := iterIface.(*sstableIRange)

		checksumOffset := 2*int64(mdByteSize) + int64(len(EncodeInternalKey(rec.GetKey(), rec.GetSequenceNumber(), rec.GetType()))) + int64(len(rec.GetValue()))
		buf := make([]byte, checksumSize)
		_, err = fs.ReadAt(buf, checksumOffset)
		require.NoError(t, err)
		buf[0] ^= 0xFF
		_, err = fs.WriteAt(buf, checksumOffset)
		require.NoError(t, err)

		sri.offset = sri.dataEnd
		sri.cursor = sri.dataEnd
		sri.primeNextDescending()
		require.ErrorIs(t, sri.err, ErrChecksumMismatch)
	})
}

func TestSSTableIRange_PrimeNextDescendingKeyAndSeqFilters(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc"), 3))
	mem.Put(newRecord(Bytes("d"), Bytes("vd"), 4))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	iterIface, err := sst.IRange(Bytes("b"), Bytes("c"), RangeDesc, 2)
	require.NoError(t, err)
	sri := iterIface.(*sstableIRange)

	sri.offset = sst.dataEnd
	sri.cursor = sst.dataEnd

	sri.primeNextDescending()
	require.True(t, sri.prepared)
	require.NotNil(t, sri.next)
	assert.Equal(t, Bytes("b"), sri.next.GetKey())
	require.NoError(t, sri.err)

	sri.prepared = false
	sri.next = nil
	sri.primeNextDescending()
	require.ErrorIs(t, sri.err, EOI)
}

func TestSSTableIRange_EnsureLowerBoundErrors(t *testing.T) {
	t.Parallel()

	t.Run("no-records-in-range", func(t *testing.T) {
		t.Parallel()
		cfg := testConfig(t)
		ctx := context.Background()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]

		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("a"), Bytes("va"), 1))

		sst, _, err := flush(ctx, cfg, mem, fs)
		require.NoError(t, err)

		iterIface, err := sst.IRange(Bytes("z"), Bytes("zz"), RangeAsc)
		require.NoError(t, err)
		sri := iterIface.(*sstableIRange)

		err = sri.ensureLowerBound()
		require.ErrorIs(t, err, EOI)
	})

	t.Run("end-boundary", func(t *testing.T) {
		t.Parallel()
		cfg := testConfig(t)
		ctx := context.Background()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]

		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("b"), Bytes("vb"), 1))

		sst, _, err := flush(ctx, cfg, mem, fs)
		require.NoError(t, err)

		iterIface, err := sst.IRange(Bytes("a"), Bytes("a"), RangeAsc)
		require.NoError(t, err)
		sri := iterIface.(*sstableIRange)

		err = sri.ensureLowerBound()
		require.ErrorIs(t, err, EOI)
	})

	t.Run("checksum-error", func(t *testing.T) {
		t.Parallel()
		cfg := testConfig(t)
		ctx := context.Background()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]

		rec := newRecord(Bytes("a"), Bytes("va"), 1)
		mem := InitMemtable(cfg)
		mem.Put(rec)

		sst, _, err := flush(ctx, cfg, mem, fs)
		require.NoError(t, err)

		iterIface, err := sst.IRange(Bytes("a"), Bytes("z"), RangeAsc)
		require.NoError(t, err)
		sri := iterIface.(*sstableIRange)

		startOffset := sri.startOffset
		checksumOffset := startOffset + 2*int64(mdByteSize) + int64(len(EncodeInternalKey(rec.GetKey(), rec.GetSequenceNumber(), rec.GetType()))) + int64(len(rec.GetValue()))
		buf := make([]byte, checksumSize)
		_, err = fs.ReadAt(buf, checksumOffset)
		require.NoError(t, err)
		buf[0] ^= 0xFF
		_, err = fs.WriteAt(buf, checksumOffset)
		require.NoError(t, err)

		err = sri.ensureLowerBound()
		require.ErrorIs(t, err, ErrChecksumMismatch)
	})
}

func TestSSTableIRange_PrimePrevFilters(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	fss, closer := initTempFileSystems(t, 1, nil)
	defer closer()
	fs := fss[0]

	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc"), 3))
	mem.Put(newRecord(Bytes("d"), Bytes("vd"), 4))

	sst, _, err := flush(ctx, cfg, mem, fs)
	require.NoError(t, err)

	sri := &sstableIRange{
		s:              &sst,
		startKey:       Bytes("b"),
		endKey:         Bytes("c"),
		seq:            2,
		offset:         sst.dataEnd,
		cursor:         sst.dataEnd,
		dataEnd:        sst.dataEnd,
		haveLowerBound: true,
		lowerBound:     sst.SparseIndex[1].offset,
	}

	sri.primePrev()
	require.True(t, sri.prevPrepared)
	require.NotNil(t, sri.prev)
	assert.Equal(t, Bytes("b"), sri.prev.GetKey())
	require.NoError(t, sri.prevErr)

	sri.prevPrepared = false
	sri.prev = nil
	sri.primePrev()
	require.ErrorIs(t, sri.prevErr, EOI)
}
