package rindb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemtable_BasicOperations(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	pairs := generateKeyValuePairs(1000, 10, 20)
	mem := populateMemtable(cfg, pairs...)

	// Verify generated pairs
	for _, pair := range pairs {
		key := pair[0]
		expectedValue := pair[1]
		got, err := mem.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, expectedValue, got)
	}
}

func TestMemtable_MaxSequenceNumber(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	cases := []struct {
		name    string
		records []struct {
			key   string
			value string
			seq   uint64
		}
		want uint64
	}{
		{name: "empty memtable", want: 0},
		{
			name: "single record",
			records: []struct {
				key   string
				value string
				seq   uint64
			}{{"key1", "value1", 10}},
			want: 10,
		},
		{
			name: "increasing sequence numbers",
			records: []struct {
				key   string
				value string
				seq   uint64
			}{
				{"key1", "value1", 1},
				{"key2", "value2", 5},
				{"key3", "value3", 10},
			},
			want: 10,
		},
		{
			name: "decreasing sequence numbers",
			records: []struct {
				key   string
				value string
				seq   uint64
			}{
				{"key1", "value1", 20},
				{"key2", "value2", 15},
				{"key3", "value3", 10},
			},
			want: 20,
		},
		{
			name: "mixed sequence numbers",
			records: []struct {
				key   string
				value string
				seq   uint64
			}{
				{"key1", "value1", 5},
				{"key2", "value2", 20},
				{"key3", "value3", 10},
				{"key4", "value4", 1},
			},
			want: 20,
		},
		{
			name: "all zero sequence numbers",
			records: []struct {
				key   string
				value string
				seq   uint64
			}{
				{"key1", "value1", 0},
				{"key2", "value2", 0},
			},
			want: 0,
		},
		{
			name: "mixed including zero",
			records: []struct {
				key   string
				value string
				seq   uint64
			}{
				{"key1", "value1", 0},
				{"key2", "value2", 5},
				{"key3", "value3", 0},
				{"key4", "value4", 10},
			},
			want: 10,
		},
		{
			name: "duplicate max sequence numbers",
			records: []struct {
				key   string
				value string
				seq   uint64
			}{
				{"key1", "value1", 5},
				{"key2", "value2", 20},
				{"key3", "value3", 10},
				{"key4", "value4", 20},
			},
			want: 20,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mem := InitMemtable(cfg)
			for _, r := range tt.records {
				mem.Put(newRecord(Bytes(r.key), Bytes(r.value), r.seq))
			}
			maxSeqNum, err := getMaxSequenceNumberFromMemtable(mem)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, maxSeqNum)
		})
	}
}

func TestMemtable_ByteSize(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	entryOverhead := slNodeOverhead + internalKeySuffixLen

	t.Run("Empty", func(t *testing.T) {
		mem := InitMemtable(cfg)
		assert.Equal(t, 0, mem.ByteSize(), "Empty memtable should have size 0")
	})

	t.Run("SingleEntry", func(t *testing.T) {
		mem := InitMemtable(cfg)
		key := Bytes("key1")
		value := Bytes("value1")
		expectedSize := len(key) + len(value) + entryOverhead

		mem.Put(newRecord(key, value, 1))
		assert.Equal(t, expectedSize, mem.ByteSize(), "Size mismatch after single entry")
	})

	t.Run("MultipleEntries", func(t *testing.T) {
		mem := InitMemtable(cfg)
		key1 := Bytes("key1")
		value1 := Bytes("value1")
		key2 := Bytes("key22")
		value2 := Bytes("value222")
		expectedSize := 0
		expectedSize += len(key1) + len(value1) + entryOverhead
		expectedSize += len(key2) + len(value2) + entryOverhead

		mem.Put(newRecord(key1, value1, 1))
		mem.Put(newRecord(key2, value2, 2))
		assert.Equal(t, expectedSize, mem.ByteSize(), "Size mismatch after multiple entries")
	})

	t.Run("UpdateEntry", func(t *testing.T) {
		mem := InitMemtable(cfg)
		key := Bytes("key1")
		value1 := Bytes("value1")
		value2 := Bytes("new_value_longer")

		// Initial put
		mem.Put(newRecord(key, value1, 1))
		initialSize := len(key) + len(value1) + entryOverhead
		assert.Equal(t, initialSize, mem.ByteSize(), "Size mismatch after initial put")

		// Update
		mem.Put(newRecord(key, value2, 2))
		expectedSize := initialSize + len(key) + len(value2) + entryOverhead
		assert.Equal(t, expectedSize, mem.ByteSize(), "Size mismatch after updating entry")
	})

	t.Run("Tombstone", func(t *testing.T) {
		mem := InitMemtable(cfg)
		key := Bytes("key1")
		value := Bytes("value1")

		// Initial put
		mem.Put(newRecord(key, value, 1))
		initialSize := len(key) + len(value) + entryOverhead
		assert.Equal(t, initialSize, mem.ByteSize(), "Size mismatch after initial put")

		// Put tombstone
		mem.Put(newRecord(key, nil, 2))
		expectedSize := initialSize + len(key) + 0 + entryOverhead
		assert.Equal(t, expectedSize, mem.ByteSize(), "Size mismatch after putting tombstone")
	})

	t.Run("Clear", func(t *testing.T) {
		mem := InitMemtable(cfg)
		key1 := Bytes("key1")
		value1 := Bytes("value1")
		key2 := Bytes("key2")
		value2 := Bytes("value2")

		mem.Put(newRecord(key1, value1, 1))
		mem.Put(newRecord(key2, value2, 2))
		assert.NotEqual(t, 0, mem.ByteSize(), "Size should not be 0 before clear")

		mem.Clear()
		assert.Equal(t, 0, mem.ByteSize(), "Size should be 0 after clear")
	})
}

func TestMemtable_Tombstone(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	pairs := generateKeyValuePairs(1000, 10, 20)
	mem := populateMemtable(cfg, pairs...)

	// Add tombstones for every 3rd generated key
	tombstoneKeys := make(map[string]struct{})
	for i, pair := range pairs {
		if i%3 == 0 {
			key := pair[0]
			mem.Put(newRecord(key, nil, uint64(i+1001))) // Add tombstone with higher seq num
			tombstoneKeys[string(key)] = struct{}{}
		}
	}

	// Verify values and tombstones using the original generated pairs
	for _, pair := range pairs {
		key := pair[0]
		expectedValue := pair[1]

		got, err := mem.Get(key)
		if _, isTombstone := tombstoneKeys[string(key)]; isTombstone {
			assert.ErrorIs(t, err, ErrKeyNotFound)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, expectedValue, got, "Value mismatch for key %s", string(key))
		}
	}
}

func TestMemtable_GetAtAndCleanup(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	mem := InitMemtable(cfg)
	key := Bytes("k")
	mem.Put(newRecord(key, Bytes("v1"), 1))
	mem.Put(newRecord(key, Bytes("v2"), 2))
	mem.Put(newRecord(key, nil, 3))

	v, err := mem.GetAt(key, 1)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v1"), v)

	v, err = mem.GetAt(key, 2)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v2"), v)

	_, err = mem.GetAt(key, 3)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	mem.Cleanup(3)
	_, err = mem.GetAt(key, 2)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	mem.Cleanup(4)
	_, err = mem.GetAt(key, 4)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestMemtable_Cleanup(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	entryOverhead := slNodeOverhead + internalKeySuffixLen

	t.Run("RetainsSnapshotVisibleRecordAndRemovesOlderVersions", func(t *testing.T) {
		mem := InitMemtable(cfg)

		key1 := Bytes("key1")
		key2 := Bytes("key2")

		testData := []struct {
			rec  Record
			keep bool
		}{
			{rec: newRecord(key1, Bytes("v3"), 9), keep: true},
			{rec: newRecord(key1, Bytes("v2"), 6), keep: true},
			{rec: newRecord(key1, Bytes("v1"), 4), keep: true},
			{rec: newRecord(key1, Bytes("v0"), 1), keep: false},
			{rec: newRecord(key2, nil, 8), keep: true},
			{rec: newRecord(key2, nil, 5), keep: true},
			{rec: newRecord(key2, Bytes("legacy"), 2), keep: false},
		}

		var expectedTotal, expectedAfterCleanup int
		for _, td := range testData {
			mem.Put(td.rec)
			entrySize := len(td.rec.GetKey()) + len(td.rec.GetValue()) + entryOverhead
			expectedTotal += entrySize
			if td.keep {
				expectedAfterCleanup += entrySize
			}
		}
		assert.Equal(t, expectedTotal, mem.ByteSize())

		mem.Cleanup(5)

		assert.Equal(t, expectedAfterCleanup, mem.ByteSize())

		v, err := mem.Get(key1)
		assert.NoError(t, err)
		assert.Equal(t, Bytes("v3"), v)

		v, err = mem.GetAt(key1, 5)
		assert.NoError(t, err)
		assert.Equal(t, Bytes("v1"), v)

		_, err = mem.GetAt(key1, 1)
		assert.ErrorIs(t, err, ErrKeyNotFound)

		_, err = mem.Get(key2)
		assert.ErrorIs(t, err, ErrKeyNotFound)

		_, err = mem.GetAt(key2, 5)
		assert.ErrorIs(t, err, ErrKeyNotFound)

		_, err = mem.GetAt(key2, 7)
		assert.ErrorIs(t, err, ErrKeyNotFound)

		_, err = mem.GetAt(key2, 2)
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("KeepsLatestRecord", func(t *testing.T) {
		mem := InitMemtable(cfg)
		key := Bytes("k")
		mem.Put(newRecord(key, Bytes("v1"), 1))
		mem.Put(newRecord(key, Bytes("v2"), 2))

		mem.Cleanup(3)

		v, err := mem.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, Bytes("v2"), v)
	})
}

func TestMemtable_CleanupRetainsSnapshotVisibleVersion(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	snapshotSeq := uint64(2)

	newMem := func() memtable {
		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("primary"), Bytes("v1"), 1))
		mem.Put(newRecord(Bytes("bump"), Bytes("noop"), snapshotSeq))
		mem.Put(newRecord(Bytes("primary"), Bytes("v2"), 3))
		return mem
	}

	t.Run("GetAt retains snapshot-visible version", func(t *testing.T) {
		mem := newMem()

		mem.Cleanup(snapshotSeq)

		v, err := mem.GetAt(Bytes("primary"), snapshotSeq)
		require.NoError(t, err)
		assert.Equal(t, Bytes("v1"), v)
	})

	t.Run("IRange retains snapshot-visible version", func(t *testing.T) {
		mem := newMem()

		mem.Cleanup(snapshotSeq)

		iter := mem.IRange(Bytes("primary"), Bytes("primary"), snapshotSeq, RangeAsc)
		require.True(t, iter.HasNext(), "expected snapshot-visible version to remain after cleanup")
		rec, err := iter.Next()
		require.NoError(t, err)
		assert.Equal(t, Bytes("v1"), rec.GetValue())
	})
}

func TestBytes_Clone(t *testing.T) {
	t.Parallel()
	t.Run("NonEmptyBytes", func(t *testing.T) {
		original := Bytes("hello world")
		cloned := original.Clone()

		assert.Equal(t, original, cloned, "Cloned bytes should be equal to original")
		assert.False(t, &original[0] == &cloned[0], "Cloned bytes should be a different underlying array")

		// Modify original and ensure cloned remains unchanged
		original[0] = 'H'
		assert.Equal(t, Bytes("Hello world"), original, "Original should be modified")
		assert.Equal(t, Bytes("hello world"), cloned, "Cloned should remain unchanged")
	})

	t.Run("EmptyBytes", func(t *testing.T) {
		original := Bytes("")
		cloned := original.Clone()

		assert.Equal(t, original, cloned, "Cloned empty bytes should be equal to original")
		assert.False(t, &original == &cloned, "Cloned empty bytes should be a different underlying array")
		assert.Len(t, cloned, 0, "Cloned empty bytes should have length 0")
	})

	t.Run("NilBytes", func(t *testing.T) {
		var original Bytes // This will be nil
		cloned := original.Clone()

		assert.Nil(t, original, "Original should be nil")
		assert.NotNil(t, cloned, "Cloned should not be nil, but an empty slice")
		assert.Len(t, cloned, 0, "Cloned nil bytes should have length 0")
		assert.Equal(t, Bytes{}, cloned, "Cloned nil bytes should be an empty slice")
	})
}

func TestMemtableIRange_Prepare(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("k"), Bytes("v7"), 7))
	mem.Put(newRecord(Bytes("k"), Bytes("v5"), 5))

	t.Run("skip higher seq", func(t *testing.T) {
		it := mem.IRange(Bytes("k"), Bytes("k"), 6, RangeAsc)
		mi, ok := it.(*memtableIRange)
		assert.True(t, ok)

		mi.prepareNext()
		assert.True(t, mi.preparedNext)
		assert.Equal(t, uint64(5), mi.next.GetSequenceNumber())
		assert.NoError(t, mi.err)
	})

	t.Run("no matching seq", func(t *testing.T) {
		it := mem.IRange(Bytes("k"), Bytes("k"), 4, RangeAsc)
		mi, ok := it.(*memtableIRange)
		assert.True(t, ok)

		mi.prepareNext()
		assert.False(t, mi.preparedNext)
		assert.NoError(t, mi.err)
	})
}

func TestMemtableIRange_HasNextNext(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va3"), 3))
	mem.Put(newRecord(Bytes("b"), Bytes("vb5"), 5))
	mem.Put(newRecord(Bytes("b"), nil, 4))
	mem.Put(newRecord(Bytes("b"), Bytes("vb1"), 1))
	mem.Put(newRecord(Bytes("c"), Bytes("vc2"), 2))

	it := mem.IRange(Bytes("a"), Bytes("c"), 4, RangeAsc)
	mi, ok := it.(*memtableIRange)
	assert.True(t, ok)

	var got []Record
	for mi.HasNext() {
		rec, err := mi.Next()
		assert.NoError(t, err)
		got = append(got, rec)
	}
	assert.Equal(t, 4, len(got))
	assert.Equal(t, Bytes("a"), got[0].GetKey())
	assert.Equal(t, uint64(3), got[0].GetSequenceNumber())
	assert.Equal(t, Bytes("b"), got[1].GetKey())
	assert.Nil(t, got[1].GetValue())
	assert.Equal(t, uint64(4), got[1].GetSequenceNumber())
	assert.Equal(t, Bytes("b"), got[2].GetKey())
	assert.Equal(t, uint64(1), got[2].GetSequenceNumber())
	assert.Equal(t, Bytes("c"), got[3].GetKey())
	assert.Equal(t, uint64(2), got[3].GetSequenceNumber())

	_, err := mi.Next()
	assert.ErrorIs(t, err, EOI)
	assert.False(t, mi.HasNext())
}

func TestMemtableIRange_Reverse(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va1"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb2"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc3"), 3))

	it := mem.IRange(Bytes("a"), Bytes("c"), 2, RangeAsc)
	for it.HasNext() {
		_, err := it.Next()
		assert.NoError(t, err)
	}

	var keys []Bytes
	for it.HasPrev() {
		rec, err := it.Prev()
		assert.NoError(t, err)
		keys = append(keys, rec.GetKey())
	}

	assert.Equal(t, []Bytes{Bytes("b"), Bytes("a")}, keys)
}

func TestMemtableIRange_DescendingPrev(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va1"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb2"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc3"), 3))

	it := mem.IRange(Bytes("a"), Bytes("c"), 3, RangeDesc)

	var keys []Bytes
	for it.HasPrev() {
		rec, err := it.Prev()
		assert.NoError(t, err)
		keys = append(keys, rec.GetKey())
	}

	assert.Equal(t, []Bytes{Bytes("c"), Bytes("b"), Bytes("a")}, keys)
}

func TestMemtableIRange_DescendingNext(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va1"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb2"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc3"), 3))

	it := mem.IRange(Bytes("a"), Bytes("c"), 3, RangeDesc)

	rec, err := it.Prev()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("c"), rec.GetKey())

	rec, err = it.Prev()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("b"), rec.GetKey())

	rec, err = it.Next()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("b"), rec.GetKey())

	rec, err = it.Next()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("c"), rec.GetKey())

	rec, err = it.Prev()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())

	_, err = it.Prev()
	assert.ErrorIs(t, err, EOI)

	_, err = it.Next()
	assert.ErrorIs(t, err, EOI)
}

func TestMemtableIRange_DescendingSequenceFilter(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va1"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb2"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc3"), 3))
	mem.Put(newRecord(Bytes("b"), Bytes("vb4"), 4))

	it := mem.IRange(Bytes("a"), Bytes("c"), 2, RangeDesc)

	var keys []Bytes
	for it.HasPrev() {
		rec, err := it.Prev()
		assert.NoError(t, err)
		keys = append(keys, rec.GetKey())
	}

	assert.Equal(t, []Bytes{Bytes("b"), Bytes("a")}, keys)
}

func TestMemtableIRange_Last(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va1"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb2"), 2))
	mem.Put(newRecord(Bytes("c"), Bytes("vc3"), 3))
	mem.Put(newRecord(Bytes("c"), Bytes("vc4"), 4))

	it := mem.IRange(Bytes("a"), Bytes("c"), 3, RangeAsc)
	mi, ok := it.(*memtableIRange)
	assert.True(t, ok)

	last, err := mi.Last()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("c"), last.GetKey())
	assert.Equal(t, uint64(3), last.GetSequenceNumber())

	prev, err := mi.Prev()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("b"), prev.GetKey())
	assert.Equal(t, uint64(2), prev.GetSequenceNumber())
}

func TestMemtableIRange_PrevBeforeNext(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va1"), 1))

	it := mem.IRange(Bytes("a"), Bytes("a"), 1, RangeAsc)
	assert.False(t, it.HasPrev())
	_, err := it.Prev()
	assert.ErrorIs(t, err, EOI)
}

func TestMemtableIRange_Alternating(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	mem := InitMemtable(cfg)
	mem.Put(newRecord(Bytes("a"), Bytes("va1"), 1))
	mem.Put(newRecord(Bytes("b"), Bytes("vb2"), 2))

	it := mem.IRange(Bytes("a"), Bytes("b"), 2, RangeAsc)

	rec, err := it.Next()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())

	rec, err = it.Prev()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())

	rec, err = it.Next()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("a"), rec.GetKey())

	rec, err = it.Next()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("b"), rec.GetKey())

	rec, err = it.Prev()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("b"), rec.GetKey())
}

func TestMemtableIRange_Empty(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	mem := InitMemtable(cfg)

	it := mem.IRange(Bytes("x"), Bytes("z"), 1, RangeAsc)
	assert.False(t, it.HasNext())
	assert.False(t, it.HasPrev())

	_, err := it.Next()
	assert.ErrorIs(t, err, EOI)
	_, err = it.Prev()
	assert.ErrorIs(t, err, EOI)
}
