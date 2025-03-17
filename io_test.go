package rindb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_rw(t *testing.T) {
	t.Run("Write key with size = 0", func(t *testing.T) {
		tx := NewTransactionManager().Begin()

		testKey := ""
		testValue := "value"

		err := WriteRecord(tx, RecordImpl{Bytes(testKey), Bytes(testValue)})
		assert.NoError(t, err)

		record, err := ReadRecord(tx.buffer)
		assert.NoError(t, err)
		assert.Equal(t, testKey, string(record.GetKey()))
		assert.Equal(t, testValue, string(record.GetValue()))
	})

	t.Run("Write key and value with size = 0", func(t *testing.T) {
		tx := NewTransactionManager().Begin()

		testKey := ""
		testValue := ""

		err := WriteRecord(tx, RecordImpl{Bytes(testKey), Bytes(testValue)})
		assert.NoError(t, err)

		record, err := ReadRecord(tx.buffer)
		assert.NoError(t, err)
		assert.Equal(t, testKey, string(record.GetKey()))
		assert.Equal(t, testValue, string(record.GetValue()))
	})

	t.Run("Write key and value with size > 255", func(t *testing.T) {
		tx := NewTransactionManager().Begin()

		testKey := ""
		testValue := ""
		for i := 0; i < 500; i++ {
			testKey += fmt.Sprintf("test key %d ", i)
			testValue += fmt.Sprintf("test value %d ", i)
		}

		err := WriteRecord(tx, RecordImpl{Bytes(testKey), Bytes(testValue)})
		assert.NoError(t, err)

		record, err := ReadRecord(tx.buffer)
		assert.NoError(t, err)
		assert.Equal(t, testKey, string(record.GetKey()))
		assert.Equal(t, testValue, string(record.GetValue()))
	})

	t.Run("Write key and value with size < 255", func(t *testing.T) {
		tx := NewTransactionManager().Begin()

		err := WriteRecord(tx, RecordImpl{Bytes("key"), Bytes("value")})
		assert.NoError(t, err)

		record, err := ReadRecord(tx.buffer)
		assert.NoError(t, err)
		assert.Equal(t, "key", string(record.GetKey()))
		assert.Equal(t, "value", string(record.GetValue()))
	})
}
