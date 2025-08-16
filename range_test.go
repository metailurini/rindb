package rindb

import (
	"testing"

	"errors"
	"github.com/stretchr/testify/assert"
)

func TestRindbRange(t *testing.T) {
	ts := newTestRindbSetup(t, nil)
	defer ts.Cleanup()

	// Create SSTable with older records
	mem1 := InitMemtable(ts.Manager.config)
	mem1.Put(NewRecord(Bytes("a"), Bytes("sstA"), 1))
	mem1.Put(NewRecord(Bytes("b"), Bytes("sstB"), 2))
	sst1, err := Flush(ts.Manager.config, mem1, ts.newSSTableFS(0))
	assert.NoError(t, err)
	ts.AddSSTableToLevel(0, &sst1)

	// Create SSTable with tombstone and another record
	mem2 := InitMemtable(ts.Manager.config)
	mem2.Put(NewRecord(Bytes("k"), Bytes(""), 3)) // tombstone
	mem2.Put(NewRecord(Bytes("z"), Bytes("sstZ"), 4))
	sst2, err := Flush(ts.Manager.config, mem2, ts.newSSTableFS(0))
	assert.NoError(t, err)
	ts.AddSSTableToLevel(0, &sst2)

	// Memtable with latest updates
	mem := InitMemtable(ts.Manager.config)
	mem.Put(NewRecord(Bytes("a"), Bytes("memA"), 5))
	mem.Put(NewRecord(Bytes("b"), Bytes(""), 6)) // delete b
	mem.Put(NewRecord(Bytes("k"), Bytes("memK"), 7))

	r := Rindb{
		memtable:       mem,
		ssTableManager: ts.Manager,
		config:         ts.Manager.config,
	}

	iter, err := r.RangeIterator(Bytes("a"), Bytes("z"))
	assert.NoError(t, err)

	expected := []Record{
		NewRecord(Bytes("a"), Bytes("memA"), 5),
		NewRecord(Bytes("k"), Bytes("memK"), 7),
		NewRecord(Bytes("z"), Bytes("sstZ"), 4),
	}
	assertIteratorRecords(t, iter, expected)
}

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

func TestMergedRangeIteratorErrorPropagates(t *testing.T) {
	r1 := NewRecord(Bytes("a"), Bytes("1"), 1)
	r2 := NewRecord(Bytes("b"), Bytes("2"), 2)
	it := &errIterator{records: []Record{r1, r2}}
	pq := buildRangePQ([]Iterator[Record]{it})
	cleaned := false
	iter := &mergedRangeIterator{pq: pq, cleanup: func() { cleaned = true }}

	assert.True(t, iter.HasNext())
	rec, err := iter.Next()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())

	assert.False(t, iter.HasNext())
	_, err = iter.Next()
	assert.EqualError(t, err, "boom")
	assert.True(t, cleaned)
}

func TestRangeIteratorCloseReleasesSSTables(t *testing.T) {
	ts := newTestRindbSetup(t, nil)
	defer ts.Cleanup()

	mem1 := InitMemtable(ts.Manager.config)
	mem1.Put(NewRecord(Bytes("a"), Bytes("sstA"), 1))
	sst1, err := Flush(ts.Manager.config, mem1, ts.newSSTableFS(0))
	assert.NoError(t, err)
	ts.AddSSTableToLevel(0, &sst1)

	mem := InitMemtable(ts.Manager.config)
	r := Rindb{memtable: mem, ssTableManager: ts.Manager, config: ts.Manager.config}
	iter, err := r.RangeIterator(Bytes("a"), Bytes("z"))
	assert.NoError(t, err)

	if iter.HasNext() {
		_, err = iter.Next()
		assert.NoError(t, err)
	}
	assert.True(t, sst1.IsOpened())
	CloseIterator(iter)
	assert.False(t, sst1.IsOpened())
}
