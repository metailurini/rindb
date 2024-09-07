package rindb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func debugList[K Comparable, V any](list *SkipList[K, V]) {
	DEBUG("--header--: %v", list.headNote)
	r := list.headNote.Next()
	for r != nil {
		DEBUG("[%v<>%v] ", r.Key, r.Value)
		for _, v := range r.forwards {
			if v == nil {
				continue
			}
			DEBUG("[%v<>%v] ", v.Key, v.Value)
		}
		fmt.Println()
		r = r.Next()
	}
}

func TestInitSkipList(t *testing.T) {
	t.Run("Init with invalid key type", func(t *testing.T) {
		list, err := InitSkipList[struct{ int }, int]()
		assert.ErrorIs(t, err, ErrUnsupportedType)
		assert.Nil(t, list)
	})

	t.Run("Init with custom type", func(t *testing.T) {
		list, err := InitSkipList[customCmpType, customCmpType]()
		assert.NoError(t, err)
		list.Put(customCmpType{1}, customCmpType{2})
		list.Put(customCmpType{3}, customCmpType{4})
		assertOrderedList(t, list.Head())
		assert.GreaterOrEqual(t, list.level, uint(2))

		value, err := list.Get(customCmpType{1})
		assert.NoError(t, err)
		assert.Equal(t, customCmpType{2}, value)

		value, err = list.Get(customCmpType{3})
		assert.NoError(t, err)
		assert.Equal(t, customCmpType{4}, value)
	})

	t.Run("Init with correct key type", func(t *testing.T) {
		list, err := InitSkipList[string, int]()
		assert.NoError(t, err)

		data := []int{6, 3, 5, 8, 1, 2, 8}
		for _, v := range data {
			list.Put(fmt.Sprintf("k:%d", v), v)
			list.Put(fmt.Sprintf("k:%d", v), v)
		}

		assert.GreaterOrEqual(t, list.level, uint(2))
		debugList(list)

		for _, v := range data {
			list.Put(fmt.Sprintf("k:%d", v), v)
			_v, err := list.Get(fmt.Sprintf("k:%d", v))
			assert.Equal(t, v, _v)
			assert.NoError(t, err)
		}

		assertOrderedList(t, list.Head())

		v, err := list.Get("k:8")
		assert.Equal(t, 8, v)
		assert.NoError(t, err)

		err = list.Remove("k:2")
		assert.NoError(t, err)

		v, err = list.Get("k:2")
		assert.Empty(t, v)
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})
}

//nolint:funlen
func TestSkipListPut(t *testing.T) {
	t.Run("Assert all added values", func(t *testing.T) {
		list, err := InitSkipList[string, int]()
		assert.NoError(t, err)

		data := []int{6, 3, 5, 8, 1, 2, 8}
		for _, v := range data {
			list.Put(fmt.Sprintf("k:%d", v), v)
		}

		for _, v := range data {
			k := fmt.Sprintf("k:%d", v)
			_v, err := list.Get(k)
			assert.Equal(t, v, _v)
			assert.NoError(t, err)
		}

		// should be 6 because data has 2 "8"
		assert.Equal(t, uint(6), list.Len())
		assertOrderedList(t, list.Head())
	})

	t.Run("Override existing key", func(t *testing.T) {
		list, err := InitSkipList[string, int]()
		assert.NoError(t, err)

		data := []int{6, 3, 5, 8, 1, 2, 8}
		for _, v := range data {
			list.Put(fmt.Sprintf("k:%d", v), v)
		}

		list.Put("k:3", 300)
		v, err := list.Get("k:3")
		assert.Equal(t, 300, v)
		assert.NoError(t, err)

		// should be 6 because no new key
		assert.Equal(t, uint(6), list.Len())
		assertOrderedList(t, list.Head())
	})

	t.Run("Empty key or value", func(t *testing.T) {
		// TODO: fix this case
		t.Skip()

		list, err := InitSkipList[Bytes, Bytes]()
		assert.NoError(t, err)

		list.Put(nil, nil)
		value, err := list.Get(nil)
		assert.NoError(t, err)
		assert.Equal(t, Bytes(nil), value)

		list.Put(nil, Bytes(""))
		value, err = list.Get(nil)
		assert.NoError(t, err)
		assert.Equal(t, Bytes(""), value)

		list.Put(Bytes(""), nil)
		value, err = list.Get(nil)
		assert.NoError(t, err)
		assert.Equal(t, Bytes(nil), value)

		list.Put(Bytes(""), Bytes(""))
		value, err = list.Get(nil)
		assert.NoError(t, err)
		assert.Equal(t, Bytes(""), value)

		debugList(list)
		assert.Equal(t, uint(2), list.Len())
		assertOrderedList(t, list.Head())
	})
}

func TestSkipListGet(t *testing.T) {
	list, err := InitSkipList[string, int]()
	assert.NoError(t, err)

	data := []int{6, 3, 5, 8, 1, 2, 8}
	for _, v := range data {
		list.Put(fmt.Sprintf("k:%d", v), v)
	}

	v, err := list.Get("k:100")
	assert.Empty(t, v)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	v, err = list.Get("k:8")
	assert.Equal(t, 8, v)
	assert.NoError(t, err)
}

func TestSkipListRemove(t *testing.T) {
	list, err := InitSkipList[string, int]()
	assert.NoError(t, err)

	data := []int{6, 3, 5, 8, 1, 2, 9}
	actualLength := uint(len(data))

	for _, v := range data {
		list.Put(fmt.Sprintf("k:%d", v), v)
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
			debugList(list)
			err := list.Remove(tt.key)
			debugList(list)
			if tt.existing {
				actualLength--
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, ErrKeyNotFound)
			}

			v, err := list.Get(tt.key)
			assert.Empty(t, v)
			keyShouldNotExists(t, tt.key, list)
			assert.ErrorIs(t, err, ErrKeyNotFound)
			assert.Equal(t, actualLength, list.Len())
			assertOrderedList(t, list.Head())
		})
	}
}

func TestSkipListClear(t *testing.T) {
	t.Run("Clear list properly", func(t *testing.T) {
		list, err := InitSkipList[string, int]()
		assert.NoError(t, err)

		list.Put("1", 1)
		list.Put("2", 2)
		list.Put("3", 3)

		assert.Equal(t, uint(3), list.Len())
		list.Clear()
		assert.Equal(t, uint(0), list.Len())
	})

	t.Run("expect error when clearing list was not init properly",
		func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Errorf("The code did not panic")
				}
				assert.Equal(t, ErrMalformedList, r)
			}()

			var list *SkipList[struct{}, struct{}]
			list.Clear()
		})
}

func assertOrderedList[K, V Comparable](t *testing.T, head *SLNode[K, V]) {
	for head.Next() != nil {
		n := head.Next()
		assert.Equal(t, -1, Compare(head.Key, n.Key))
		head = head.Next()
	}
}

func keyShouldNotExists[K, V Comparable](t *testing.T, key K, list *SkipList[K, V]) {
	r := list.headNote.Next()
	for r != nil {
		for _, v := range r.forwards {
			if v == nil {
				continue
			}
			if Compare(key, v.Key) == CmpEqual {
				t.Errorf("Key %v should not exist in the skiplist", key)
			}
		}
		fmt.Println()
		r = r.Next()
	}
}
