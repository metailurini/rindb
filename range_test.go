package rindb

import (
	"testing"

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

	records, err := r.Range(Bytes("a"), Bytes("z"))
	assert.NoError(t, err)

	expected := map[string]string{"a": "memA", "k": "memK", "z": "sstZ"}
	assert.Equal(t, len(expected), len(records))
	for _, rec := range records {
		key := string(rec.GetKey())
		val := string(rec.GetValue())
		expectedVal, ok := expected[key]
		assert.True(t, ok, "unexpected key %s", key)
		assert.Equal(t, expectedVal, val)
		delete(expected, key)
	}
	assert.Empty(t, expected)
}
