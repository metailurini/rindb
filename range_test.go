package rindb

import (
	"context"
	"errors"
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

func TestRangeIterator_NextAfterHasPrevPeeking(t *testing.T) {
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

	_, err := iter.Prev()
	require.ErrorIs(t, err, EOI)
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
