package rindb

import (
	"context"
	"testing"

	"errors"

	"github.com/stretchr/testify/assert"
)

// errIterator injects an error at a specified index.
type errIterator struct {
	records []Record
	idx     int
	failIdx int
}

func (e *errIterator) HasNext() bool { return e.idx < len(e.records) }

func (e *errIterator) Next() (Record, error) {
	if e.idx == e.failIdx {
		e.idx++
		var empty Record
		return empty, errors.New("boom")
	}
	rec := e.records[e.idx]
	e.idx++
	return rec, nil
}

func TestRangeIterator(t *testing.T) {
	rec := func(k, v string, seq uint64) Record {
		return NewRecord(Bytes(k), Bytes(v), seq)
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
			name: "basic order",
			iters: []iterSpec{
				{records: []Record{rec("a", "1", 1)}, failIdx: -1},
				{records: []Record{rec("b", "2", 1)}, failIdx: -1},
			},
			want: []exp{{"a", "1"}, {"b", "2"}},
		},
		{
			name: "skip duplicates",
			iters: []iterSpec{
				{records: []Record{rec("a", "v2", 2), rec("b", "vb", 1)}, failIdx: -1},
				{records: []Record{rec("a", "v1", 1)}, failIdx: -1},
			},
			want: []exp{{"a", "v2"}, {"b", "vb"}},
		},
		{
			name: "skip tombstones",
			iters: []iterSpec{
				{records: []Record{rec("a", "", 2), rec("b", "2", 1)}, failIdx: -1},
				{records: []Record{rec("a", "1", 1)}, failIdx: -1},
			},
			want: []exp{{"b", "2"}},
		},
		{
			name: "iterator error",
			iters: []iterSpec{
				{records: []Record{rec("a", "1", 1), rec("b", "2", 2), rec("c", "3", 3)}, failIdx: 2},
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
			pq, err := buildRangePQ(iterators)
			assert.NoError(t, err)
			cleaned := false
			iter := &RangeIterator{pq: pq, cleanup: func() { cleaned = true }}

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
				assert.True(t, cleaned)
			} else {
				assert.ErrorIs(t, err, EOI)
			}
		})
	}
}

func TestBuildRangePQInitialError(t *testing.T) {
	r := NewRecord(Bytes("a"), Bytes("1"), 1)
	it := &errIterator{records: []Record{r}, failIdx: 0}
	pq, err := buildRangePQ([]Iterator[Record]{it})
	assert.Nil(t, pq)
	assert.EqualError(t, err, "boom")
}

func TestIRangeCloseReleasesSSTables(t *testing.T) {
	cfg := testConfig()
	ctx := context.Background()
	ts := newTestRindbSetup(t, ctx, &cfg)
	defer ts.Cleanup()

	mem1 := InitMemtable(cfg)
	mem1.Put(NewRecord(Bytes("a"), Bytes("sstA"), 1))
	sst1, err := flush(ctx, cfg, mem1, ts.newSSTableFS(0))
	assert.NoError(t, err)
	ts.AddSSTableToLevel(0, &sst1)

	iter, err := ts.RinDB.IRange(ctx, Bytes("a"), Bytes("z"))
	assert.NoError(t, err)

	if iter.HasNext() {
		_, err = iter.Next()
		assert.NoError(t, err)
	}
	assert.True(t, sst1.IsOpened())
	assert.NoError(t, iter.Close())
	assert.False(t, sst1.IsOpened())
}
