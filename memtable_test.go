package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_memtable(t *testing.T) {
	mem := initMemtable()

	// Insert 1 million elements
	for i := 0; i < 1_000_000; i++ {
		key := Bytes(fmt.Sprintf("key%d", i))
		value := Bytes(fmt.Sprintf("value%d", i))
		mem.put(key, value)
	}

	// Retrieve and assert values of 1 million elements
	for i := 0; i < 1_000_000; i++ {
		key := Bytes(fmt.Sprintf("key%d", i))
		expectedValue := Bytes(fmt.Sprintf("value%d", i))
		got, err := mem.get(key)
		assert.NoError(t, err)
		assert.Equal(t, expectedValue, got)
	}

	// Delete elements where i % 3 == 0
	for i := 0; i < 1_000_000; i++ {
		if i%3 == 0 {
			key := Bytes(fmt.Sprintf("key%d", i))
			mem.put(key, nil)
		}
	}

	// Assert that the deleted elements are no longer retrievable
	for i := 0; i < 1_000_000; i++ {
		key := Bytes(fmt.Sprintf("key%d", i))
		expectedValue := Bytes(fmt.Sprintf("value%d", i))

		got, err := mem.get(key)
		assert.NoError(t, err)

		if i%3 == 0 {
			assert.Nil(t, got)
		} else {
			assert.Equal(t, expectedValue, got)
		}
	}
}
