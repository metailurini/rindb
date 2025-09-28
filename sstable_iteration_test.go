package rindb

import (
	"context"
	"fmt"
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
