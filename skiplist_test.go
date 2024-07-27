package main

import (
	"cmp"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func next[K, V cmp.Ordered](current *node[K, V]) *node[K, V] {
	if current == nil {
		return nil
	}
	return current.forwards[0]
}

func Test_initSkipList(t *testing.T) {
	list := initSkipList[string, int]()

	data := []int{6, 3, 5, 8, 1, 2, 8}
	for _, v := range data {
		list.insert(fmt.Sprintf("k:%d", v), v)
	}

	for _, v := range data {
		list.insert(fmt.Sprintf("k:%d", v), v)
		_v, err := list.get(fmt.Sprintf("k:%d", v))
		assert.Equal(t, v, _v)
		assert.NoError(t, err)
	}

	c := list.head
	for next(c) != nil {
		n := next(c)
		assert.Less(t, c.key, n.key)
		c = next(c)
	}

	show(*list)

	v, err := list.get("k:8")
	assert.Equal(t, 8, v)
	assert.NoError(t, err)

	err = list.remove("k:2")
	assert.NoError(t, err)

	show(*list)

	v, err = list.get("k:2")
	assert.Empty(t, v)
	assert.ErrorIs(t, ErrKeyNotFound, err)
}

func show[K, V cmp.Ordered](list skipList[K, V]) {
	println()
	r := list.head
	for r != nil {
		fmt.Printf("r: '%v' ", r.key)
		for _, _v := range r.forwards[1:] {
			if _v == nil {
				break
			}
			fmt.Printf(" '%v' ", _v.key)
		}
		r = r.forwards[0]
		println()
	}
}

func Test_skipList_insert(t *testing.T) {
	list := initSkipList[string, int]()

	data := []int{6, 3, 5, 8, 1, 2, 8}
	for _, v := range data {
		list.insert(fmt.Sprintf("k:%d", v), v)
	}

	t.Run("assert all added values", func(t *testing.T) {
		for _, v := range data {
			k := fmt.Sprintf("k:%d", v)
			_v, err := list.get(k)
			assert.Equal(t, v, _v)
			assert.NoError(t, err)
		}
		assertOrderedList(t, list.head)
	})

	t.Run("override existing key", func(t *testing.T) {
		list.insert("k:3", 300)
		v, err := list.get("k:3")
		assert.Equal(t, 300, v)
		assert.NoError(t, err)
		assertOrderedList(t, list.head)
	})
}

func Test_skipList_get(t *testing.T) {
	list := initSkipList[string, int]()

	data := []int{6, 3, 5, 8, 1, 2, 8}
	for _, v := range data {
		list.insert(fmt.Sprintf("k:%d", v), v)
	}

	v, err := list.get("k:100")
	assert.Empty(t, v)
	assert.ErrorIs(t, ErrKeyNotFound, err)

	v, err = list.get("k:8")
	assert.Equal(t, 8, v)
	assert.NoError(t, err)
}

func assertOrderedList[K, V cmp.Ordered](t *testing.T, head *node[K, V]) {
	for next(head) != nil {
		n := next(head)
		assert.Less(t, head.key, n.key)
		head = next(head)
	}
}

func Test_skipList_remove(t *testing.T) {
	list := initSkipList[string, int]()

	data := []int{6, 3, 5, 8, 1, 2, 8}
	for _, v := range data {
		list.insert(fmt.Sprintf("k:%d", v), v)
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := list.remove(tt.key)
			if tt.existing {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, ErrKeyNotFound, err)
			}

			v, err := list.get(tt.key)
			assert.Empty(t, v)
			assert.ErrorIs(t, ErrKeyNotFound, err)
			assertOrderedList(t, list.head)
		})
	}
}
