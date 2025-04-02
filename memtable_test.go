package rindb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemtable_Basic(t *testing.T) {
	cfg := testConfig()
	pairs := make([][2]Bytes, 1_000)
	for i := 0; i < 1_000; i++ {
		pairs[i] = [2]Bytes{
			Bytes(fmt.Sprintf("key%d", i)),
			Bytes(fmt.Sprintf("value%d", i)),
		}
	}
	mem := populateMemtable(cfg, pairs...)

	for i := 0; i < 1_000; i++ {
		key := Bytes(fmt.Sprintf("key%d", i))
		expectedValue := Bytes(fmt.Sprintf("value%d", i))
		got, err := mem.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, expectedValue, got)
	}
}

func TestMemtable_Tombstone(t *testing.T) {
	cfg := testConfig()
	pairs := make([][2]Bytes, 1_000)
	for i := 0; i < 1_000; i++ {
		pairs[i] = [2]Bytes{
			Bytes(fmt.Sprintf("key%d", i)),
			Bytes(fmt.Sprintf("value%d", i)),
		}
	}
	mem := populateMemtable(cfg, pairs...)

	// Add tombstones
	for i := 0; i < 1_000; i++ {
		if i%3 == 0 {
			key := Bytes(fmt.Sprintf("key%d", i))
			mem.Put(key, nil) // Add tombstone
		}
	}

	// Verify values and tombstones
	for i := 0; i < 1_000; i++ {
		key := Bytes(fmt.Sprintf("key%d", i))
		expectedValue := Bytes(fmt.Sprintf("value%d", i))

		got, err := mem.Get(key)
		assert.NoError(t, err)

		if i%3 == 0 {
			assert.Nil(t, got, "Expected nil (tombstone) for key %s", string(key))
		} else {
			assert.Equal(t, expectedValue, got, "Value mismatch for key %s", string(key))
		}
	}
}
