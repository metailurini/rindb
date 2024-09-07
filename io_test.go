package rindb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_rw(t *testing.T) {
	t.Run("write key with size = 0", func(t *testing.T) {
		buf := bytes.NewBufferString("")

		testKey := ""
		testValue := "value"

		err := WriteRecord(buf, RecordImpl{Bytes(testKey), Bytes(testValue)})
		assert.NoError(t, err)

		record, err := ReadRecord(buf)
		assert.NoError(t, err)
		assert.Equal(t, testKey, string(record.GetKey()))
		assert.Equal(t, testValue, string(record.GetValue()))
	})

	t.Run("write key and value with size = 0", func(t *testing.T) {
		buf := bytes.NewBufferString("")

		testKey := ""
		testValue := ""

		err := WriteRecord(buf, RecordImpl{Bytes(testKey), Bytes(testValue)})
		assert.NoError(t, err)

		record, err := ReadRecord(buf)
		assert.NoError(t, err)
		assert.Equal(t, testKey, string(record.GetKey()))
		assert.Equal(t, testValue, string(record.GetValue()))
	})

	t.Run("write key and value with size > 255", func(t *testing.T) {
		buf := bytes.NewBufferString("")

		testKey := ""
		testValue := ""
		for i := 0; i < 500; i++ {
			testKey += fmt.Sprintf("test key %d ", i)
			testValue += fmt.Sprintf("test value %d ", i)
		}

		err := WriteRecord(buf, RecordImpl{Bytes(testKey), Bytes(testValue)})
		assert.NoError(t, err)

		record, err := ReadRecord(buf)
		assert.NoError(t, err)
		assert.Equal(t, testKey, string(record.GetKey()))
		assert.Equal(t, testValue, string(record.GetValue()))
	})

	t.Run("write key and value with size < 255", func(t *testing.T) {
		buf := bytes.NewBufferString("")

		err := WriteRecord(buf, RecordImpl{Bytes("key"), Bytes("value")})
		assert.NoError(t, err)

		record, err := ReadRecord(buf)
		assert.NoError(t, err)
		assert.Equal(t, "key", string(record.GetKey()))
		assert.Equal(t, "value", string(record.GetValue()))
	})
}
