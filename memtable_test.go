package rindb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_memtable(t *testing.T) {
	mem := InitMemtable()

	for i := 0; i < 1_000; i++ {
		key := Bytes(fmt.Sprintf("key%d", i))
		value := Bytes(fmt.Sprintf("value%d", i))
		mem.Put(key, value)
	}

	for i := 0; i < 1_000; i++ {
		key := Bytes(fmt.Sprintf("key%d", i))
		expectedValue := Bytes(fmt.Sprintf("value%d", i))
		got, err := mem.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, expectedValue, got)
	}

	for i := 0; i < 1_000; i++ {
		if i%3 == 0 {
			key := Bytes(fmt.Sprintf("key%d", i))
			mem.Put(key, nil)
		}
	}

	for i := 0; i < 1_000; i++ {
		key := Bytes(fmt.Sprintf("key%d", i))
		expectedValue := Bytes(fmt.Sprintf("value%d", i))

		got, err := mem.Get(key)
		assert.NoError(t, err)

		if i%3 == 0 {
			assert.Nil(t, got)
		} else {
			assert.Equal(t, expectedValue, got)
		}
	}
}
