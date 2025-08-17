package rindb

import (
	"context"
	"testing"

	"errors"

	"github.com/stretchr/testify/assert"
)

// errIterator yields one record then returns an error on subsequent Next calls.
type errIterator struct {
	records []Record
	idx     int
}

func (e *errIterator) HasNext() bool { return e.idx < len(e.records) }

func (e *errIterator) Next() (Record, error) {
	if e.idx == 1 {
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
	it := &errIterator{records: []Record{r1, r2}}
	pq := buildRangePQ([]Iterator[Record]{it})
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

func TestIRangeCloseReleasesSSTables(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	ts := newTestRindbSetup(t, ctx, &cfg)
	defer ts.Cleanup()

	mem1 := InitMemtable(ts.Manager.config)
	mem1.Put(NewRecord(Bytes("a"), Bytes("sstA"), 1))
	sst1, err := Flush(ctx, ts.Manager.config, mem1, ts.newSSTableFS(0))
	assert.NoError(t, err)
	ts.AddSSTableToLevel(0, &sst1)

	mem := InitMemtable(ts.Manager.config)
	r := Rindb{memtable: mem, ssTableManager: ts.Manager, config: ts.Manager.config}
	iter, err := r.IRange(ctx, Bytes("a"), Bytes("z"))
	assert.NoError(t, err)

	if iter.HasNext() {
		_, err = iter.Next()
		assert.NoError(t, err)
	}
	assert.True(t, sst1.IsOpened())
	assert.NoError(t, iter.Close())
	assert.False(t, sst1.IsOpened())
}
