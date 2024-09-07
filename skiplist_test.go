package rindb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_initSkipList(t *testing.T) {
	t.Run("init with invalid key type", func(t *testing.T) {
		list, err := initSkipList[struct{ int }, int]()
		assert.ErrorIs(t, ErrUnsupportedType, err)
		assert.Nil(t, list)
	})

	t.Run("init with correct key type", func(t *testing.T) {
		list, err := initSkipList[string, int]()
		assert.NoError(t, err)

		data := []int{6, 3, 5, 8, 1, 2, 8}
		for _, v := range data {
			list.put(fmt.Sprintf("k:%d", v), v)
			list.put(fmt.Sprintf("k:%d", v), v)
		}

		for _, v := range data {
			list.put(fmt.Sprintf("k:%d", v), v)
			_v, err := list.get(fmt.Sprintf("k:%d", v))
			assert.Equal(t, v, _v)
			assert.NoError(t, err)
		}

		assertOrderedList(t, list.head())

		v, err := list.get("k:8")
		assert.Equal(t, 8, v)
		assert.NoError(t, err)

		err = list.remove("k:2")
		assert.NoError(t, err)

		v, err = list.get("k:2")
		assert.Empty(t, v)
		assert.ErrorIs(t, ErrKeyNotFound, err)
	})
}

func Test_skipList_put(t *testing.T) {
	list, err := initSkipList[string, int]()
	assert.NoError(t, err)

	data := []int{6, 3, 5, 8, 1, 2, 8}
	for _, v := range data {
		list.put(fmt.Sprintf("k:%d", v), v)
	}

	t.Run("assert all added values", func(t *testing.T) {
		for _, v := range data {
			k := fmt.Sprintf("k:%d", v)
			_v, err := list.get(k)
			assert.Equal(t, v, _v)
			assert.NoError(t, err)
		}

		// should be 6 because data has 2 "8"
		assert.Equal(t, uint(6), list.len())
		assertOrderedList(t, list.head())
	})

	t.Run("override existing key", func(t *testing.T) {
		list.put("k:3", 300)
		v, err := list.get("k:3")
		assert.Equal(t, 300, v)
		assert.NoError(t, err)

		// should be 6 because no new key
		assert.Equal(t, uint(6), list.len())
		assertOrderedList(t, list.head())
	})
}

func Test_skipList_get(t *testing.T) {
	list, err := initSkipList[string, int]()
	assert.NoError(t, err)

	data := []int{6, 3, 5, 8, 1, 2, 8}
	for _, v := range data {
		list.put(fmt.Sprintf("k:%d", v), v)
	}

	v, err := list.get("k:100")
	assert.Empty(t, v)
	assert.ErrorIs(t, ErrKeyNotFound, err)

	v, err = list.get("k:8")
	assert.Equal(t, 8, v)
	assert.NoError(t, err)
}

func Test_skipList_remove(t *testing.T) {
	list, err := initSkipList[string, int]()
	assert.NoError(t, err)

	data := []int{6, 3, 5, 8, 1, 2, 9}
	actualLength := uint(len(data))

	for _, v := range data {
		list.put(fmt.Sprintf("k:%d", v), v)
	}

	tests := []struct {
		name, key string
		existing  bool
	}{
		{
			name:     "remove first value",
			key:      "k:1",
			existing: true,
		},
		{
			name:     "remove mid value",
			key:      "k:3",
			existing: true,
		},
		{
			name:     "remove last value",
			key:      "k:8",
			existing: true,
		},
		{
			name:     "remove last value",
			key:      "k:100",
			existing: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := list.remove(tt.key)
			if tt.existing {
				actualLength--
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, ErrKeyNotFound, err)
			}

			v, err := list.get(tt.key)
			assert.Empty(t, v)
			assert.ErrorIs(t, ErrKeyNotFound, err)
			assert.Equal(t, actualLength, list.len())
			assertOrderedList(t, list.head())
		})
	}
}

func assertOrderedList[K, V Comparable](t *testing.T, head *node[K, V]) {
	for head.next() != nil {
		n := head.next()
		assert.Less(t, head.key, n.key)
		head = head.next()
	}
}
