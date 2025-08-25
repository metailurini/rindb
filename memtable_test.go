package rindb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemtable_Basic(t *testing.T) {
	cfg := testConfig()
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

func TestGetMaxSequenceNumberFromMemtable(t *testing.T) {
	cfg := testConfig()

	t.Run("Empty Memtable", func(t *testing.T) {
		mem := InitMemtable(cfg)
		maxSeqNum, err := getMaxSequenceNumberFromMemtable(mem)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), maxSeqNum)
	})

	t.Run("Single Record", func(t *testing.T) {
		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("key1"), Bytes("value1"), 10))
		maxSeqNum, err := getMaxSequenceNumberFromMemtable(mem)
		assert.NoError(t, err)
		assert.Equal(t, uint64(10), maxSeqNum)
	})

	t.Run("Multiple Records - Increasing Sequence Numbers", func(t *testing.T) {
		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("key1"), Bytes("value1"), 1))
		mem.Put(newRecord(Bytes("key2"), Bytes("value2"), 5))
		mem.Put(newRecord(Bytes("key3"), Bytes("value3"), 10))
		maxSeqNum, err := getMaxSequenceNumberFromMemtable(mem)
		assert.NoError(t, err)
		assert.Equal(t, uint64(10), maxSeqNum)
	})

	t.Run("Multiple Records - Decreasing Sequence Numbers", func(t *testing.T) {
		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("key1"), Bytes("value1"), 20))
		mem.Put(newRecord(Bytes("key2"), Bytes("value2"), 15))
		mem.Put(newRecord(Bytes("key3"), Bytes("value3"), 10))
		maxSeqNum, err := getMaxSequenceNumberFromMemtable(mem)
		assert.NoError(t, err)
		assert.Equal(t, uint64(20), maxSeqNum)
	})

	t.Run("Multiple Records - Mixed Sequence Numbers", func(t *testing.T) {
		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("key1"), Bytes("value1"), 5))
		mem.Put(newRecord(Bytes("key2"), Bytes("value2"), 20))
		mem.Put(newRecord(Bytes("key3"), Bytes("value3"), 10))
		mem.Put(newRecord(Bytes("key4"), Bytes("value4"), 1))
		maxSeqNum, err := getMaxSequenceNumberFromMemtable(mem)
		assert.NoError(t, err)
		assert.Equal(t, uint64(20), maxSeqNum)
	})

	t.Run("All Records Have Sequence Number 0", func(t *testing.T) {
		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("key1"), Bytes("value1"), 0))
		mem.Put(newRecord(Bytes("key2"), Bytes("value2"), 0))
		maxSeqNum, err := getMaxSequenceNumberFromMemtable(mem)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), maxSeqNum)
	})

	t.Run("Mixed Sequence Numbers Including 0", func(t *testing.T) {
		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("key1"), Bytes("value1"), 0))
		mem.Put(newRecord(Bytes("key2"), Bytes("value2"), 5))
		mem.Put(newRecord(Bytes("key3"), Bytes("value3"), 0))
		mem.Put(newRecord(Bytes("key4"), Bytes("value4"), 10))
		maxSeqNum, err := getMaxSequenceNumberFromMemtable(mem)
		assert.NoError(t, err)
		assert.Equal(t, uint64(10), maxSeqNum)
	})

	t.Run("Duplicate Maximum Sequence Numbers", func(t *testing.T) {
		mem := InitMemtable(cfg)
		mem.Put(newRecord(Bytes("key1"), Bytes("value1"), 5))
		mem.Put(newRecord(Bytes("key2"), Bytes("value2"), 20))
		mem.Put(newRecord(Bytes("key3"), Bytes("value3"), 10))
		mem.Put(newRecord(Bytes("key4"), Bytes("value4"), 20))
		maxSeqNum, err := getMaxSequenceNumberFromMemtable(mem)
		assert.NoError(t, err)
		assert.Equal(t, uint64(20), maxSeqNum)
	})
}

func TestMemtable_ByteSize(t *testing.T) {
	cfg := testConfig()
	entryOverhead := 83 // As defined in memtable.go Put method

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
		expectedSize := len(key) + len(value2) + entryOverhead // Only the latest entry size counts
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
		expectedSize := len(key) + 0 + entryOverhead // Value length is 0 for tombstone
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
	cfg := testConfig()
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
		assert.NoError(t, err)

		if _, isTombstone := tombstoneKeys[string(key)]; isTombstone {
			assert.Nil(t, got, "Expected nil (tombstone) for key %s", string(key))
		} else {
			assert.Equal(t, expectedValue, got, "Value mismatch for key %s", string(key))
		}
	}
}
