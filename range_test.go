package rindb

import (
	"context"
	"errors"
	"io"
	"log"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rec(k, v string, seq uint64, typ RecordType) Record {
	var nv Bytes
	if v != "" {
		nv = Bytes(v)
	}
	return RecordImpl{Key: Bytes(k), Value: nv, SequenceNumber: seq, Type: typ}
}

func TestRangeIterator_Next(t *testing.T) {
	t.Parallel()
	recL := func(k, v string, seq uint64, typ RecordType) Record {
		var nv Bytes = nil
		if v != "" {
			nv = Bytes(v)
		}
		return RecordImpl{Key: Bytes(k), Value: nv, SequenceNumber: seq, Type: typ}
	}

	type iterSpec struct {
		records []Record
		failIdx int
	}

	type exp struct {
		k, v string
	}

	tests := []struct {
		name    string
		iters   []iterSpec
		want    []exp
		wantErr string
	}{
		{
			name: "basic next across sources",
			iters: []iterSpec{
				{records: []Record{recL("a", "1", 1, TypeValue), recL("b", "2", 2, TypeValue)}, failIdx: -1},
			},
			want: []exp{{"a", "1"}, {"b", "2"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var iterators []Iterator[Record]
			for _, spec := range tt.iters {
				iterators = append(iterators, &errIterator{records: spec.records, failIdx: spec.failIdx})
			}
			mi, err := NewMergingIterator(iterators, nil, RangeAsc)
			assert.NoError(t, err)
			iter := NewRangeIterator(mi, RangeAsc)

			var got []exp
			for iter.HasNext() {
				r, err := iter.Next()
				assert.NoError(t, err)
				got = append(got, exp{string(r.GetKey()), string(r.GetValue())})
			}
			assert.Equal(t, tt.want, got)

			_, err = iter.Next()
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
			} else {
				assert.ErrorIs(t, err, EOI)
			}
		})
	}
}

func TestRangeIterator_FilterTombstonesAndDuplicates(t *testing.T) {

	t.Parallel()
	type iterSpec struct {
		records []Record
		failIdx int
	}

	type exp struct {
		k, v string
	}

	tests := []struct {
		name    string
		iters   []iterSpec
		want    []exp
		wantErr string
	}{
		{
			name: "filters tombstones and duplicates",
			iters: []iterSpec{
				{records: []Record{rec("a", "v2", 2, TypeValue), rec("c", "c2", 2, TypeValue)}, failIdx: -1},
				{records: []Record{rec("a", "v1", 1, TypeValue)}, failIdx: -1},
				{records: []Record{rec("b", "", 3, TypeDeletion), rec("b", "vb", 1, TypeValue)}, failIdx: -1},
			},
			want: []exp{{"a", "v2"}, {"c", "c2"}},
		},
		{
			name: "iterator error",
			iters: []iterSpec{
				{records: []Record{rec("a", "1", 1, TypeValue), rec("b", "2", 2, TypeValue), rec("c", "3", 3, TypeValue)}, failIdx: 2},
			},
			want:    []exp{{"a", "1"}, {"b", "2"}},
			wantErr: "boom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var iterators []Iterator[Record]
			for _, spec := range tt.iters {
				iterators = append(iterators, &errIterator{records: spec.records, failIdx: spec.failIdx})
			}
			mi, err := NewMergingIterator(iterators, nil, RangeAsc)
			assert.NoError(t, err)
			iter := NewRangeIterator(mi, RangeAsc)

			var got []exp
			for iter.HasNext() {
				r, err := iter.Next()
				assert.NoError(t, err)
				got = append(got, exp{string(r.GetKey()), string(r.GetValue())})
			}
			assert.Equal(t, tt.want, got)

			_, err = iter.Next()
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
			} else {
				assert.ErrorIs(t, err, EOI)
			}
		})
	}
}

func TestRangeIterator_ReverseIteration(t *testing.T) {
	t.Parallel()
	type iterSpec struct {
		records []Record
		failIdx int
	}
	type exp struct {
		k, v string
	}
	iters := []iterSpec{
		{records: []Record{rec("a", "va", 1, TypeValue)}, failIdx: -1},
		{records: []Record{rec("b", "vb", 1, TypeValue)}, failIdx: -1},
		{records: []Record{rec("c", "vc", 1, TypeValue)}, failIdx: -1},
	}
	var iterators []Iterator[Record]
	for _, spec := range iters {
		iterators = append(iterators, &errIterator{records: spec.records, failIdx: spec.failIdx})
	}
	mi, err := NewMergingIterator(iterators, nil, RangeAsc)
	assert.NoError(t, err)
	iter := NewRangeIterator(mi, RangeAsc)

	var forward []exp
	for iter.HasNext() {
		r, err := iter.Next()
		assert.NoError(t, err)
		forward = append(forward, exp{string(r.GetKey()), string(r.GetValue())})
	}
	assert.Equal(t, []exp{{"a", "va"}, {"b", "vb"}, {"c", "vc"}}, forward)

	var backward []exp
	for {
		r, err := iter.Prev()
		if errors.Is(err, EOI) {
			break
		}
		assert.NoError(t, err)
		backward = append(backward, exp{string(r.GetKey()), string(r.GetValue())})
	}
	assert.Equal(t, []exp{{"c", "vc"}, {"b", "vb"}, {"a", "va"}}, backward)
}

func TestRangeIterator_PrevBeforeNext(t *testing.T) {
	t.Parallel()
	it := &errIterator{records: []Record{rec("a", "va", 1, TypeValue)}, failIdx: -1}
	mi, err := NewMergingIterator([]Iterator[Record]{it}, nil, RangeAsc)
	assert.NoError(t, err)

	iter := NewRangeIterator(mi, RangeAsc)
	_, err = iter.Prev()
	assert.ErrorIs(t, err, EOI)
}

func TestRangeIteratorPrev_SuppressesOlderVersion(t *testing.T) {
	t.Parallel()
	iter := buildRangeIter(t,
		[]kv{{key: "k", value: "v3", seq: 3}},
		[]kv{{key: "k", value: "v2", seq: 2}},
	)

	mustNextValue(t, iter, "k", "v3")
	mustPrevValue(t, iter, "k", "v3")
	mustPrevEOI(t, iter)
}

func TestRangeIteratorPrev_ReturnsLatestAfterDirectionChange(t *testing.T) {
	t.Parallel()
	iter := buildRangeIter(t,
		[]kv{
			{key: "k", value: "v2", seq: 2},
			{key: "z", value: "vz", seq: 1},
		},
		[]kv{{key: "k", value: "v1", seq: 1}},
	)

	mustNextValue(t, iter, "k", "v2")
	mustNextValue(t, iter, "z", "vz")
	mustNextEOI(t, iter)

	mustPrevValue(t, iter, "z", "vz")
	mustPrevValue(t, iter, "k", "v2")
	mustPrevEOI(t, iter)
}

func TestRangeIterator_LastPositionsPrevBeforeTail(t *testing.T) {
	t.Parallel()
	iter := buildRangeIter(t,
		[]kv{
			{key: "a", value: "va1", seq: 1},
			{key: "c", value: "vc5", seq: 5},
		},
		[]kv{
			{key: "b", value: "vb2", seq: 2},
			{key: "c", value: "vc3", seq: 3},
		},
	)

	last, err := iter.Last()
	require.NoError(t, err)
	assert.Equal(t, "c", string(last.GetKey()))
	assert.Equal(t, "vc5", string(last.GetValue()))

	prev, err := iter.Prev()
	require.NoError(t, err)
	assert.Equal(t, "b", string(prev.GetKey()))
	assert.Equal(t, "vb2", string(prev.GetValue()))
}

func TestRangeIterator_AlternatingNextPrev(t *testing.T) {
	t.Parallel()
	iters := []Iterator[Record]{
		&errIterator{records: []Record{rec("a", "va", 1, TypeValue)}, failIdx: -1},
		&errIterator{records: []Record{rec("b", "vb", 1, TypeValue)}, failIdx: -1},
		&errIterator{records: []Record{rec("c", "vc", 1, TypeValue)}, failIdx: -1},
	}
	mi, err := NewMergingIterator(iters, nil, RangeAsc)
	assert.NoError(t, err)
	iter := NewRangeIterator(mi, RangeAsc)

	type exp struct{ k, v string }
	ops := []struct {
		next bool
		want exp
	}{
		{true, exp{"a", "va"}},
		{true, exp{"b", "vb"}},
		{false, exp{"b", "vb"}},
		{true, exp{"b", "vb"}},
		{true, exp{"c", "vc"}},
		{false, exp{"c", "vc"}},
		{false, exp{"b", "vb"}},
	}

	for i, op := range ops {
		var r Record
		if op.next {
			r, err = iter.Next()
		} else {
			r, err = iter.Prev()
		}
		assert.NoError(t, err, "step %d", i)
		assert.Equal(t, op.want.k, string(r.GetKey()), "step %d", i)
		assert.Equal(t, op.want.v, string(r.GetValue()), "step %d", i)
	}
}

func TestRangeIterator_DescendingPrevNextRegression(t *testing.T) {
	t.Parallel()

	iter := buildRangeIterOrder(t, RangeDesc,
		[]kv{{key: "k1", value: "v1", seq: 1}},
		[]kv{{key: "k2", value: "v2", seq: 2}},
		[]kv{{key: "k3", value: "v3", seq: 3}},
	)

	_, err := iter.Prev()
	require.ErrorIs(t, err, EOI)

	rec, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, "k3", string(rec.GetKey()))

	rec, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, "k2", string(rec.GetKey()))

	rec, err = iter.Prev()
	require.NoError(t, err)
	require.Equal(t, "k2", string(rec.GetKey()))

	rec, err = iter.Prev()
	require.NoError(t, err)
	require.Equal(t, "k3", string(rec.GetKey()))

	_, err = iter.Prev()
	require.ErrorIs(t, err, EOI)

	rec, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, "k3", string(rec.GetKey()))

	rec, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, "k2", string(rec.GetKey()))
}

func TestRangeIterator_DescendingPrevAfterOscillation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	db, err := InitRinDB(ctx,
		WithDatabaseDir(dir),
		WithCacheBytes(32<<20),
		WithLogger(NewStdLogger(log.New(io.Discard, "", 0))),
		WithLogLevel(LogLevelInfo),
	)
	if err != nil {
		t.Fatalf("InitRinDB: %v", err)
	}
	defer db.Close()

	mustPut := func(key string) {
		if err := db.Put(ctx, Bytes(key), Bytes("v")); err != nil {
			t.Fatalf("put %q: %v", key, err)
		}
	}
	mustDel := func(key string) {
		if err := db.Remove(ctx, Bytes(key)); err != nil {
			t.Fatalf("del %q: %v", key, err)
		}
	}

	mustPut("k07")
	mustPut("k07")
	mustPut("k08")
	mustPut("k03")
	mustDel("k14")
	mustPut("k03")
	mustDel("k04")
	mustPut("k08")
	mustPut("k08")
	mustPut("k03")
	mustPut("k05")
	mustPut("k05")
	mustPut("k09")
	mustPut("k11")
	mustPut("k06")
	mustDel("k11")
	mustDel("k13")
	mustPut("k10")
	mustPut("k05")
	mustDel("k09")
	mustDel("k02")

	snap, err := db.NewSnapshot(ctx)
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snap.Release(ctx)

	iter, err := db.IRange(ctx,
		Bytes("k01"),
		Bytes("k12"),
		IRangeOrder(RangeDesc),
		IRangeSnapshot(snap.Sequence()),
	)
	if err != nil {
		t.Fatalf("IRange: %v", err)
	}
	defer iter.Close()

	expect := []string{"k10", "k08", "k07", "k06", "k05", "k03"}
	idx := 0

	next := func(step int, want string) {
		rec, err := iter.Next()
		assert.NoError(t, err, "step %d Next error", step)
		assert.Equal(t, want, string(rec.GetKey()), "step %d Next key mismatch", step)
		idx++
	}
	prev := func(step int, want string) {
		rec, err := iter.Prev()
		assert.NoError(t, err, "step %d Prev error", step)
		idx--
		assert.Equal(t, want, string(rec.GetKey()), "step %d Prev key mismatch", step)
	}

	next(0, expect[0]) // k10
	next(1, expect[1]) // k08
	prev(2, expect[1]) // k08
	next(3, expect[1]) // k08
	prev(4, expect[1]) // k08
	next(5, expect[1]) // k08
	prev(6, expect[1]) // k08
	prev(7, expect[0]) // k10
}

func TestRangeIterator_HasPrevAfterHasNextPeek(t *testing.T) {
	t.Parallel()

	iter := buildRangeIter(t,
		[]kv{
			{key: "a", value: "va", seq: 1},
			{key: "b", value: "vb", seq: 2},
		},
	)

	require.True(t, iter.HasNext(), "expected HasNext to report data present")

	assert.True(t, iter.HasPrev(), "peeking backward after a forward peek should surface the staged element")
}

func TestRangeIterator_PrevAfterHasNext(t *testing.T) {
	t.Parallel()

	iter := buildRangeIter(t,
		[]kv{
			{key: "a", value: "va", seq: 1},
			{key: "b", value: "vb", seq: 2},
		},
	)

	require.True(t, iter.HasNext())

	rec, err := iter.Prev()
	require.NoError(t, err)
	require.Equal(t, "a", string(rec.GetKey()))
	require.Equal(t, "va", string(rec.GetValue()))
}

func TestRangeIterator_EmptyIterator(t *testing.T) {
	t.Parallel()
	mi, err := NewMergingIterator([]Iterator[Record]{}, nil, RangeAsc)
	assert.NoError(t, err)

	iter := NewRangeIterator(mi, RangeAsc)
	assert.False(t, iter.HasNext())
	assert.False(t, iter.HasPrev())
	_, err = iter.Next()
	assert.ErrorIs(t, err, EOI)
	_, err = iter.Prev()
	assert.ErrorIs(t, err, EOI)
}

func TestIRange_CloseReleasesSSTables(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	ctx := context.Background()
	ts := newTestRindbSetup(t, ctx, &cfg)
	defer ts.Cleanup()

	mem1 := InitMemtable(cfg)
	mem1.Put(newRecord(Bytes("a"), Bytes("sstA"), 1))
	sst1, meta, err := flush(ctx, cfg, mem1, ts.newSSTableFS())
	assert.NoError(t, err)
	require.NotZero(t, meta.Number)
	ts.AddSSTable(0, &sst1)

	iter, err := ts.RinDB.IRange(ctx, Bytes("a"), Bytes("z"))
	assert.NoError(t, err)

	num, err := fileNum(sst1.Path())
	require.NoError(t, err)
	h, ok := ts.Manager.cache.tryGet(ctx, tableKey{FileNum: num})
	require.True(t, ok)
	assert.True(t, h.Table.IsOpened())
	assert.Equal(t, int32(2), h.refs.Load())
	h.unref()
	assert.Equal(t, int32(1), h.refs.Load())
	assert.NoError(t, iter.Close())
	assert.Equal(t, int32(0), h.refs.Load())
	h, ok = ts.Manager.cache.tryGet(ctx, tableKey{FileNum: num})
	require.True(t, ok)
	assert.Equal(t, int32(1), h.refs.Load())
	h.unref()
	assert.Equal(t, int32(0), h.refs.Load())
}

func TestRangeIterator_Prepare(t *testing.T) {
	t.Parallel()
	rec := func(k, v string, seq uint64) Record {
		var nv Bytes = nil
		if v != "" {
			nv = Bytes(v)
		}
		return newRecord(Bytes(k), nv, seq)
	}

	type testCase struct {
		name         string
		records      []Record
		failIdx      int
		wantPrepared bool
		wantKey      string
		wantValue    string
		wantErr      string
	}

	tests := []testCase{
		{
			name:         "skips duplicates in same iterator",
			records:      []Record{rec("a", "v2", 2), rec("a", "v1", 1), rec("b", "vb", 1)},
			failIdx:      -1,
			wantPrepared: true,
			wantKey:      "b",
			wantValue:    "vb",
		},
		{
			name:         "propagates iterator error",
			records:      []Record{rec("a", "v2", 2), rec("a", "v1", 1)},
			failIdx:      1,
			wantPrepared: false,
			wantErr:      "boom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := &errIterator{records: tt.records, failIdx: tt.failIdx}
			mi, err := NewMergingIterator([]Iterator[Record]{it}, nil, RangeAsc)
			assert.NoError(t, err)

			iter := NewRangeIterator(mi, RangeAsc)
			// Emulate internal preparation flow used by HasNext/Next
			iter.primeNext()
			_, err = iter.Next()
			assert.NoError(t, err)

			// Prepare the next element and validate expectations
			iter.primeNext()
			if tt.wantErr != "" {
				assert.EqualError(t, iter.err, tt.wantErr)
				iter.nextPrepared = false
				iter.primeNext()
				assert.False(t, iter.nextPrepared)
			} else {
				assert.Equal(t, tt.wantPrepared, iter.nextPrepared)
				assert.Equal(t, tt.wantKey, string(iter.next.GetKey()))
				assert.Equal(t, tt.wantValue, string(iter.next.GetValue()))
			}
		})
	}
}

type kv struct {
	key   string
	value string
	seq   uint64
	typ   RecordType
}

func buildRangeIter(t *testing.T, sources ...[]kv) *RangeIterator {
	return buildRangeIterOrder(t, RangeAsc, sources...)
}

func buildRangeIterOrder(t *testing.T, order RangeOrder, sources ...[]kv) *RangeIterator {
	t.Helper()

	var iterators []Iterator[Record]
	for _, src := range sources {
		records := make([]Record, 0, len(src))
		for _, entry := range src {
			typ := entry.typ
			if typ == 0 {
				typ = TypeValue
			}
			records = append(records, rec(entry.key, entry.value, entry.seq, typ))
		}
		iterators = append(iterators, &errIterator{records: records, failIdx: -1})
	}

	mi, err := NewMergingIterator(iterators, nil, order)
	require.NoError(t, err)
	return NewRangeIterator(mi, order)
}

func TestRangeIterator_DescendingFirstNext(t *testing.T) {
	t.Parallel()

	iter := buildRangeIterOrder(t, RangeDesc, []kv{{key: "k1", value: "v1", seq: 1}, {key: "k2", value: "v2", seq: 2}, {key: "k3", value: "v3", seq: 3}})

	mustNextValue(t, iter, "k3", "v3")
	mustNextValue(t, iter, "k2", "v2")
	mustNextValue(t, iter, "k1", "v1")
	mustNextEOI(t, iter)
}

func TestRangeIteratorDescending_HasNextSkipsTombstones(t *testing.T) {
	t.Parallel()

	iter := buildRangeIterOrder(t, RangeDesc, []kv{{key: "k", seq: 1, typ: TypeDeletion}})

	assert.False(t, iter.HasNext(), "descending HasNext should not report tombstones as live records")

	_, err := iter.Next()
	assert.ErrorIs(t, err, EOI)
}

func TestRangeIterator_DescendingOscillation(t *testing.T) {
	t.Parallel()

	iter := buildRangeIterOrder(t, RangeDesc, []kv{{key: "k1", value: "v1", seq: 1}, {key: "k2", value: "v2", seq: 2}, {key: "k3", value: "v3", seq: 3}})

	mustNextValue(t, iter, "k3", "v3")
	mustNextValue(t, iter, "k2", "v2")
	mustPrevValue(t, iter, "k2", "v2")
	mustNextValue(t, iter, "k2", "v2")
	mustNextValue(t, iter, "k1", "v1")
}

func TestRangeIterator_DescendingPrevBeforeNext(t *testing.T) {
	t.Parallel()

	iter := buildRangeIterOrder(t, RangeDesc, []kv{{key: "k1", value: "v1", seq: 1}, {key: "k2", value: "v2", seq: 2}, {key: "k3", value: "v3", seq: 3}})

	_, err := iter.Prev()
	require.ErrorIs(t, err, EOI)
}

func TestRangeIterator_DescendingHasNextRecoversAfterPrev(t *testing.T) {
	t.Parallel()

	iter := buildRangeIterOrder(t, RangeDesc,
		[]kv{{key: "k1", value: "v1", seq: 1}},
		[]kv{{key: "k2", value: "v2", seq: 2}},
		[]kv{{key: "k3", value: "v3", seq: 3}},
	)

	mustNextValue(t, iter, "k3", "v3")
	mustNextValue(t, iter, "k2", "v2")
	mustNextValue(t, iter, "k1", "v1")

	assert.False(t, iter.HasNext(), "boundary probe should report exhaustion")

	mustPrevValue(t, iter, "k1", "v1")

	assert.True(t, iter.HasNext(), "descending HasNext should recover once Prev requeues data")
}

func TestRangeIteratorDescending_PrevNextAcrossTombstone(t *testing.T) {
	t.Parallel()

	iter := buildRangeIterOrder(t, RangeDesc,
		[]kv{{key: "a", value: "va", seq: 1}},
		[]kv{{key: "c", value: "vc", seq: 2}},
		[]kv{{key: "c", typ: TypeDeletion, seq: 3}},
		[]kv{{key: "d", value: "vd", seq: 4}},
	)

	mustNextValue(t, iter, "d", "vd")
	mustNextValue(t, iter, "a", "va")
	mustPrevValue(t, iter, "a", "va")
	mustPrevValue(t, iter, "d", "vd")
	mustNextValue(t, iter, "d", "vd")
}

func TestRangeIterator_LastHonorsOrder(t *testing.T) {
	t.Parallel()

	iter := buildRangeIterOrder(t, RangeDesc,
		[]kv{{key: "a", value: "va", seq: 1}},
		[]kv{{key: "b", value: "vb", seq: 2}},
		[]kv{{key: "c", value: "vc", seq: 3}},
	)

	rec, err := iter.Last()
	require.NoError(t, err)
	assert.Equal(t, "a", string(rec.GetKey()))
	assert.Equal(t, "va", string(rec.GetValue()))
}

func TestRangeIterator_DescendingLastThenPrev(t *testing.T) {
	t.Parallel()

	t.Run("basic", func(t *testing.T) {
		iter := buildRangeIterOrder(t, RangeDesc,
			[]kv{{key: "a", value: "va", seq: 1}},
			[]kv{{key: "b", value: "vb", seq: 2}},
			[]kv{{key: "c", value: "vc", seq: 3}},
		)

		rec, err := iter.Last()
		require.NoError(t, err)
		require.Equal(t, "a", string(rec.GetKey()))
		require.Equal(t, "va", string(rec.GetValue()))

		mustPrevValue(t, iter, "b", "vb")
		mustPrevValue(t, iter, "c", "vc")
		mustPrevEOI(t, iter)
	})

	t.Run("with duplicates", func(t *testing.T) {
		iter := buildRangeIterOrder(t, RangeDesc,
			[]kv{{key: "a", value: "va", seq: 1}},
			[]kv{{key: "b", value: "vb2", seq: 3}, {key: "b", value: "vb1", seq: 2}},
			[]kv{{key: "c", value: "vc", seq: 4}},
		)

		rec, err := iter.Last()
		require.NoError(t, err)
		require.Equal(t, "a", string(rec.GetKey()))
		require.Equal(t, "va", string(rec.GetValue()))

		mustPrevValue(t, iter, "b", "vb2")
		mustPrevValue(t, iter, "c", "vc")
		mustPrevEOI(t, iter)
	})
}

func TestRangeIterator_DescendingSkipsTombstonedKey(t *testing.T) {
	t.Parallel()

	iter := buildRangeIterOrder(t, RangeDesc,
		[]kv{{key: "a", value: "va", seq: 2}, {key: "c", value: "vc", seq: 2}},
		[]kv{{key: "a", seq: 4, typ: TypeDeletion}, {key: "a", value: "va", seq: 2}, {key: "b", value: "vb", seq: 3}, {key: "d", value: "vd", seq: 3}},
	)

	mustNextValue(t, iter, "d", "vd")
	mustNextValue(t, iter, "c", "vc")
	mustNextValue(t, iter, "b", "vb")
	rec, err := iter.Next()
	if err == nil {
		t.Fatalf("unexpected record %s@%d", rec.GetKey(), rec.GetSequenceNumber())
	}
	require.ErrorIs(t, err, EOI)
}

func TestRangeIterator_DescendingLastSkipsTombstone(t *testing.T) {
	t.Parallel()

	t.Run("skips newest tombstone", func(t *testing.T) {
		iter := buildRangeIterOrder(t, RangeDesc,
			[]kv{{key: "a", value: "va", seq: 1}},
			[]kv{{key: "b", seq: 3, typ: TypeDeletion}, {key: "b", value: "vb", seq: 2}},
		)

		rec, err := iter.Last()
		require.NoError(t, err)
		require.Equal(t, "a", string(rec.GetKey()))
		require.Equal(t, "va", string(rec.GetValue()))
	})

	t.Run("all keys tombstoned", func(t *testing.T) {
		iter := buildRangeIterOrder(t, RangeDesc,
			[]kv{{key: "a", seq: 2, typ: TypeDeletion}, {key: "a", value: "va", seq: 1}},
			[]kv{{key: "b", seq: 4, typ: TypeDeletion}, {key: "b", value: "vb", seq: 3}},
		)

		_, err := iter.Last()
		require.ErrorIs(t, err, EOI)
	})
}

func TestRangeIterator_DescendingNextPrevOscillation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := InitRinDB(ctx, WithDatabaseDir(t.TempDir()), WithMaxMemtableSize(1000))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	require.NoError(t, db.Put(ctx, Bytes("k4"), Bytes("v")))
	require.NoError(t, db.Put(ctx, Bytes("k3"), Bytes("v")))
	require.NoError(t, db.Put(ctx, Bytes("k4"), Bytes("v")))
	require.NoError(t, db.Put(ctx, Bytes("k4"), Bytes("v")))
	require.NoError(t, db.Put(ctx, Bytes("k2"), Bytes("v")))
	require.NoError(t, db.Remove(ctx, Bytes("k1")))
	require.NoError(t, db.Put(ctx, Bytes("k5"), Bytes("v")))
	require.NoError(t, db.Put(ctx, Bytes("k2"), Bytes("v")))

	iter, err := db.IRange(ctx, Bytes("k1"), Bytes("k5"), IRangeOrder(RangeDesc))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, iter.Close()) })

	rec, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, "k5", string(rec.GetKey()))

	rec, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, "k4", string(rec.GetKey()))

	rec, err = iter.Prev()
	require.NoError(t, err)
	require.Equal(t, "k4", string(rec.GetKey()))

	rec, err = iter.Prev()
	require.NoError(t, err)
	require.Equal(t, "k5", string(rec.GetKey()))

	_, err = iter.Prev()
	require.ErrorIs(t, err, EOI)
}

func TestRangeIterator_DescendingSnapshotBeforeTombstone(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := InitRinDB(ctx, WithDatabaseDir(filepath.Join(dir, "db")))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	put := func(key, value string) {
		require.NoError(t, db.Put(ctx, Bytes(key), Bytes(value)))
	}
	del := func(key string) {
		require.NoError(t, db.Remove(ctx, Bytes(key)))
	}

	put("k09", "svKc5jdbimDZLIkOuTqj6cf3kZ2Grs128Y0TOev") // seq 1
	put("k11", "svDmka3zav2cVE21UREP6xCwg7xO5S6zg2ev")    // seq 2
	put("k09", "svv4MvnZlPf8g6Ch76lvwuhGZb1faYZ5hzHYiUuSxev")
	del("k04")
	del("k06")
	del("k08")
	del("k09")
	put("k02", "svaCcjRwv8bBNjlrgA1W1YimdKEzP67qHIjwR9YTVtrc5bY4fKFH9pjev")
	del("k01")
	put("k11", "svKXg1VreSQkdSyk7q0ls9zRs95ev")
	put("k02", "svJnwZ4s1Vy1Kei5V56Y2BVlGU7mMBzZcUkWYlrnNioVgDbqFWiSLedFG1eoZblHhCYIc4dk3Mcvjev")
	put("k03", "svPVNxhpZNchkeLqOJI7zZyyhi7nopDvm40url4I18kmc6Mb9b0NsB6oglPvflnszyvWOzYDfPPn8iEpaev")
	put("k07", "svNzayUXM0Y1Y1EwJ5xI9qvmmLAeqtrH0MejaUfIlpKCiMP5spKe5IrxelUgKBtDgJtS00Wt0Kd7LqlbFev")
	del("k11") // seq 14 tombstone
	put("k02", "svDQE4kvGuVL5BFHmttHDOsI6W1noIjaJZZByKY9tHh8oVQkC2vpMjhIvvUOnEFaqxd4QSev")
	put("k07", "svyZFVaXG4YFd3uQYyj5tMYC7DMEZmuXKv6oGev")
	put("k03", "svczWK529B3ngaVWcDgveGiaC0HQFBYFkpgqXceKkW0oEP5i9t6cDWiKACrt1Zvev")
	put("k05", "svlI9V9xPfV4GZImqPFeFzWD0NrnNgeY478zNev")

	iter, err := db.IRange(
		ctx,
		Bytes("k10"),
		Bytes("k12"),
		IRangeOrder(RangeDesc),
		IRangeSnapshot(7),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, iter.Close()) })

	rec, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, "k11", string(rec.GetKey()))
	require.Equal(t, "svDmka3zav2cVE21UREP6xCwg7xO5S6zg2ev", string(rec.GetValue()))
}

func mustNextValue(t *testing.T, iter *RangeIterator, key, value string) {
	t.Helper()

	rec, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, key, string(rec.GetKey()))
	require.Equal(t, value, string(rec.GetValue()))
}

func mustNextEOI(t *testing.T, iter *RangeIterator) {
	t.Helper()

	_, err := iter.Next()
	require.ErrorIs(t, err, EOI)
}

func mustPrevValue(t *testing.T, iter *RangeIterator, key, value string) {
	t.Helper()

	rec, err := iter.Prev()
	require.NoError(t, err)
	require.Equal(t, key, string(rec.GetKey()))
	require.Equal(t, value, string(rec.GetValue()))
}

func mustPrevEOI(t *testing.T, iter *RangeIterator) {
	t.Helper()

	_, err := iter.Prev()
	require.ErrorIs(t, err, EOI)
}
