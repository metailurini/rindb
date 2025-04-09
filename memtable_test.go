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

		mem.Put(key, value)
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

		mem.Put(key1, value1)
		mem.Put(key2, value2)
		assert.Equal(t, expectedSize, mem.ByteSize(), "Size mismatch after multiple entries")
	})

	t.Run("UpdateEntry", func(t *testing.T) {
		mem := InitMemtable(cfg)
		key := Bytes("key1")
		value1 := Bytes("value1")
		value2 := Bytes("new_value_longer")

		// Initial put
		mem.Put(key, value1)
		initialSize := len(key) + len(value1) + entryOverhead
		assert.Equal(t, initialSize, mem.ByteSize(), "Size mismatch after initial put")

		// Update
		mem.Put(key, value2)
		expectedSize := len(key) + len(value2) + entryOverhead // Only the latest entry size counts
		assert.Equal(t, expectedSize, mem.ByteSize(), "Size mismatch after updating entry")
	})

	t.Run("Tombstone", func(t *testing.T) {
		mem := InitMemtable(cfg)
		key := Bytes("key1")
		value := Bytes("value1")

		// Initial put
		mem.Put(key, value)
		initialSize := len(key) + len(value) + entryOverhead
		assert.Equal(t, initialSize, mem.ByteSize(), "Size mismatch after initial put")

		// Put tombstone
		mem.Put(key, nil)
		expectedSize := len(key) + 0 + entryOverhead // Value length is 0 for tombstone
		assert.Equal(t, expectedSize, mem.ByteSize(), "Size mismatch after putting tombstone")
	})

	t.Run("Clear", func(t *testing.T) {
		mem := InitMemtable(cfg)
		key1 := Bytes("key1")
		value1 := Bytes("value1")
		key2 := Bytes("key2")
		value2 := Bytes("value2")

		mem.Put(key1, value1)
		mem.Put(key2, value2)
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
			mem.Put(key, nil) // Add tombstone
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
