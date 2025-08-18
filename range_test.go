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

func TestRangeIteratorErrorPropagates(t *testing.T) {
	r1 := NewRecord(Bytes("a"), Bytes("1"), 1)
	r2 := NewRecord(Bytes("b"), Bytes("2"), 2)
	it := &errIterator{records: []Record{r1, r2}, failIdx: 1}
	pq, err := buildRangePQ([]Iterator[Record]{it})
	assert.NoError(t, err)
	cleaned := false
	iter := &RangeIterator{pq: pq, cleanup: func() { cleaned = true }}

	assert.True(t, iter.HasNext())
	rec, err := iter.Next()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())

	assert.False(t, iter.HasNext())
	_, err = iter.Next()
	assert.EqualError(t, err, "boom")
	assert.True(t, cleaned)
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
