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
