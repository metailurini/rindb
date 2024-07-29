package main

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_rw(t *testing.T) {
	t.Parallel()

	t.Run("write key and value with size > 255", func(t *testing.T) {
		buf := bytes.NewBufferString("")

		testKey := ""
		testValue := ""
		for i := 0; i < 500; i++ {
			testKey += fmt.Sprintf("test key %d ", i)
			testValue += fmt.Sprintf("test value %d ", i)
		}

		err := write(buf, Bytes(testKey), Bytes(testValue))
		assert.NoError(t, err)

		key, value, err := read(buf)
		assert.NoError(t, err)
		assert.Equal(t, testKey, string(key))
		assert.Equal(t, testValue, string(value))
	})

	t.Run("write key and value with size < 255", func(t *testing.T) {
		buf := bytes.NewBufferString("")

		err := write(buf, Bytes("key"), Bytes("value"))
		assert.NoError(t, err)

		key, value, err := read(buf)
		assert.NoError(t, err)
		assert.Equal(t, "key", string(key))
		assert.Equal(t, "value", string(value))
	})
}
