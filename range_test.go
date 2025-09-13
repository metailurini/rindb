package rindb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRangeIterator(t *testing.T) {
	rec := func(k, v string, seq uint64, typ RecordType) Record {
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
			mi, err := NewMergingIterator(iterators, nil)
			assert.NoError(t, err)
			iter := NewRangeIterator(mi)

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

func TestRangeIteratorReverse(t *testing.T) {
	rec := func(k, v string, seq uint64, typ RecordType) Record {
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
	iters := []iterSpec{
		{records: []Record{rec("a", "va", 1, TypeValue)}, failIdx: -1},
		{records: []Record{rec("b", "vb", 1, TypeValue)}, failIdx: -1},
		{records: []Record{rec("c", "vc", 1, TypeValue)}, failIdx: -1},
	}
	var iterators []Iterator[Record]
	for _, spec := range iters {
		iterators = append(iterators, &errIterator{records: spec.records, failIdx: spec.failIdx})
	}
	mi, err := NewMergingIterator(iterators, nil)
	assert.NoError(t, err)
	iter := NewRangeIterator(mi)

	var forward []exp
	for iter.HasNext() {
		r, err := iter.Next()
		assert.NoError(t, err)
		forward = append(forward, exp{string(r.GetKey()), string(r.GetValue())})
	}
	assert.Equal(t, []exp{{"a", "va"}, {"b", "vb"}, {"c", "vc"}}, forward)

	var backward []exp
	for iter.HasPrev() {
		r, err := iter.Prev()
		assert.NoError(t, err)
		backward = append(backward, exp{string(r.GetKey()), string(r.GetValue())})
	}
	assert.Equal(t, []exp{{"b", "vb"}, {"a", "va"}}, backward)
}

func TestIRangeCloseReleasesSSTables(t *testing.T) {
	cfg := testConfig()
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

func TestRangeIteratorPrepare(t *testing.T) {
	rec := func(k, v string, seq uint64) Record {
		var nv Bytes = nil
		if v != "" {
			nv = Bytes(v)
		}
		return newRecord(Bytes(k), nv, seq)
	}

	t.Run("skips duplicates in same iterator", func(t *testing.T) {
		it := &errIterator{
			records: []Record{
				rec("a", "v2", 2),
				rec("a", "v1", 1),
				rec("b", "vb", 1),
			},
			failIdx: -1,
		}
		mi, err := NewMergingIterator([]Iterator[Record]{it}, nil)
		assert.NoError(t, err)

		iter := NewRangeIterator(mi)
		iter.prepareNext()
		_, err = iter.Next()
		assert.NoError(t, err)

		iter.prepareNext()
		assert.True(t, iter.nextPrepared)
		assert.Equal(t, "b", string(iter.next.GetKey()))
		assert.Equal(t, "vb", string(iter.next.GetValue()))
	})

	t.Run("propagates iterator error", func(t *testing.T) {
		it := &errIterator{
			records: []Record{
				rec("a", "v2", 2),
				rec("a", "v1", 1),
			},
			failIdx: 1,
		}
		mi, err := NewMergingIterator([]Iterator[Record]{it}, nil)
		assert.NoError(t, err)

		iter := NewRangeIterator(mi)
		iter.prepareNext()
		_, err = iter.Next()
		assert.NoError(t, err)

		iter.prepareNext()
		assert.EqualError(t, iter.err, "boom")

		iter.nextPrepared = false
		iter.prepareNext()
		assert.False(t, iter.nextPrepared)
	})
}
