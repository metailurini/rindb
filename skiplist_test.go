package rindb

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSkipList_Init(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	t.Run("Init with invalid key type", func(t *testing.T) {
		list, err := InitSkipList[struct{ int }, int](cfg)
		assert.ErrorIs(t, err, ErrUnsupportedType)
		assert.Nil(t, list)
	})

	t.Run("Init with custom type", func(t *testing.T) {
		list, err := InitSkipList[customCmpType, customCmpType](cfg)
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
		list, err := InitSkipList[string, int](cfg)
		assert.NoError(t, err)

		data := []int{6, 3, 5, 8, 1, 2, 8}
		for _, v := range data {
			list.Put(fmt.Sprintf("k:%d", v), v)
			list.Put(fmt.Sprintf("k:%d", v), v)
		}

		assert.GreaterOrEqual(t, list.level, uint(2))
		debugSkipList(list)

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
func TestSkipList_Put(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	t.Run("Assert all added values", func(t *testing.T) {
		list, err := InitSkipList[string, int](cfg)
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
		list, err := InitSkipList[string, int](cfg)
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

		list, err := InitSkipList[Bytes, Bytes](cfg)
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

		debugSkipList(list)
		assert.Equal(t, uint(2), list.Len())
		assertOrderedList(t, list.Head())
	})
}

func TestSkipList_Get(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[string, int](cfg)
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

func TestSkipList_Remove(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[string, int](cfg)
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
			debugSkipList(list)
			err := list.Remove(tt.key)
			debugSkipList(list)
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

func TestSkipList_IteratorReverse(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	t.Run("basic", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		for i := 1; i <= 3; i++ {
			list.Put(i, i)
		}

		it, ok := list.Iterator().(*slIterator[int, int])
		assert.True(t, ok)

		for it.HasNext() {
			_, err := it.Next()
			assert.NoError(t, err)
		}

		var rev []int
		for it.HasPrev() {
			v, err := it.Prev()
			assert.NoError(t, err)
			rev = append(rev, v)
		}

		assert.Equal(t, []int{3, 2, 1}, rev)
	})

	t.Run("after remove", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		for i := 1; i <= 3; i++ {
			list.Put(i, i)
		}
		assert.NoError(t, list.Remove(2))

		it, ok := list.Iterator().(*slIterator[int, int])
		assert.True(t, ok)
		for it.HasNext() {
			_, err := it.Next()
			assert.NoError(t, err)
		}
		var rev []int
		for it.HasPrev() {
			v, err := it.Prev()
			assert.NoError(t, err)
			rev = append(rev, v)
		}
		assert.Equal(t, []int{3, 1}, rev)
	})
}

func TestSkipList_IteratorMixed(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	assert.NoError(t, err)

	for i := 1; i <= 3; i++ {
		list.Put(i, i)
	}

	it, ok := list.Iterator().(*slIterator[int, int])
	assert.True(t, ok)

	// Move forward one step then backward again. Prev returns the same
	// element that Next produced.
	assert.True(t, it.HasNext())
	v, err := it.Next()
	assert.NoError(t, err)
	assert.Equal(t, 1, v)

	assert.True(t, it.HasPrev())
	v, err = it.Prev()
	assert.NoError(t, err)
	assert.Equal(t, 1, v)

	// Next after Prev yields the same element again.
	assert.True(t, it.HasNext())
	v, err = it.Next()
	assert.NoError(t, err)
	assert.Equal(t, 1, v)

	// Advance once more to move past the first element.
	assert.True(t, it.HasNext())
	v, err = it.Next()
	assert.NoError(t, err)
	assert.Equal(t, 2, v)
}

func TestSkipList_IteratorLast(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	assert.NoError(t, err)

	for i := 1; i <= 4; i++ {
		list.Put(i, i)
	}

	it, ok := list.Iterator().(*slIterator[int, int])
	assert.True(t, ok)

	last, err := it.Last()
	assert.NoError(t, err)
	assert.Equal(t, 4, last)

	prev, err := it.Prev()
	assert.NoError(t, err)
	assert.Equal(t, 3, prev)
}

func TestSkipList_IteratorConcurrentMutations(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	assert.NoError(t, err)

	for _, v := range []int{1, 3, 5, 7} {
		list.Put(v, v)
	}

	it, ok := list.Iterator().(*slIterator[int, int])
	assert.True(t, ok)

	v, err := it.Next()
	assert.NoError(t, err)
	assert.Equal(t, 1, v)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		list.Put(2, 2)
		_ = list.Remove(5)
	}()
	wg.Wait()

	forward := []int{1}
	for it.HasNext() {
		v, err := it.Next()
		assert.NoError(t, err)
		forward = append(forward, v)
	}
	assert.Equal(t, []int{1, 2, 3, 7}, forward)

	var backward []int
	for it.HasPrev() {
		v, err := it.Prev()
		assert.NoError(t, err)
		backward = append(backward, v)
	}
	assert.Equal(t, []int{7, 3, 2, 1}, backward)
	assertOrderedList(t, list.Head())
}

func TestSkipList_IRangeReverse(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	assert.NoError(t, err)

	for i := 1; i <= 5; i++ {
		list.Put(i, i)
	}

	it, ok := list.IRange(2, 4).(*slIRange[int, int])
	assert.True(t, ok)

	for it.HasNext() {
		_, err := it.Next()
		assert.NoError(t, err)
	}

	var rev []int
	for it.HasPrev() {
		v, err := it.Prev()
		assert.NoError(t, err)
		rev = append(rev, v)
	}

	assert.Equal(t, []int{4, 3, 2}, rev)
}

func TestSkipList_IRangeMixed(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	assert.NoError(t, err)

	for i := 1; i <= 5; i++ {
		list.Put(i, i)
	}

	it, ok := list.IRange(2, 4).(*slIRange[int, int])
	assert.True(t, ok)

	assert.True(t, it.HasNext())
	v, err := it.Next()
	assert.NoError(t, err)
	assert.Equal(t, 2, v)

	assert.True(t, it.HasPrev())
	v, err = it.Prev()
	assert.NoError(t, err)
	assert.Equal(t, 2, v)

	// Resume forward iteration
	// Next after Prev returns the same element again.
	assert.True(t, it.HasNext())
	v, err = it.Next()
	assert.NoError(t, err)
	assert.Equal(t, 2, v)

	// Further Next calls resume forward iteration.
	assert.True(t, it.HasNext())
	v, err = it.Next()
	assert.NoError(t, err)
	assert.Equal(t, 3, v)
}

func TestSkipList_IRangeLast(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	list, err := InitSkipList[int, int](cfg)
	assert.NoError(t, err)

	for i := 1; i <= 5; i++ {
		list.Put(i, i)
	}

	it, ok := list.IRange(2, 4).(*slIRange[int, int])
	assert.True(t, ok)

	last, err := it.Last()
	assert.NoError(t, err)
	assert.Equal(t, 4, last)

	prev, err := it.Prev()
	assert.NoError(t, err)
	assert.Equal(t, 3, prev)
}

func TestSkipList_Clear(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	t.Run("Clear list properly", func(t *testing.T) {
		list, err := InitSkipList[string, int](cfg)
		assert.NoError(t, err)

		list.Put("1", 1)
		list.Put("2", 2)
		list.Put("3", 3)

		assert.Equal(t, uint(3), list.Len())
		list.Clear()
		assert.Equal(t, uint(0), list.Len())
	})

	t.Run("Expect error when clearing list was not init properly",
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

func TestSkipList_IRange(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	t.Run("Empty list", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		it := list.IRange(1, 5)
		assert.False(t, it.HasNext())
		_, err = it.Next()
		assert.ErrorIs(t, err, EOI)
	})

	t.Run("Range covers all elements", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		data := []int{1, 2, 3, 4, 5}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(0, 6)
		expected := []int{10, 20, 30, 40, 50}
		actual := []int{}
		for it.HasNext() {
			val, err := it.Next()
			assert.NoError(t, err)
			actual = append(actual, val)
		}
		assert.Equal(t, expected, actual)
		assert.False(t, it.HasNext())
		_, err = it.Next()
		assert.ErrorIs(t, err, EOI)
	})

	t.Run("Range completely outside (before)", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		data := []int{10, 20, 30}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(1, 5)
		assert.False(t, it.HasNext())
		_, err = it.Next()
		assert.ErrorIs(t, err, EOI)
	})

	t.Run("Range completely outside (after)", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		data := []int{10, 20, 30}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(35, 40)
		assert.False(t, it.HasNext())
		_, err = it.Next()
		assert.ErrorIs(t, err, EOI)
	})

	t.Run("Range partially overlaps (start before, end within)", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		data := []int{10, 20, 30, 40, 50}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(5, 30)
		expected := []int{100, 200, 300}
		actual := []int{}
		for it.HasNext() {
			val, err := it.Next()
			assert.NoError(t, err)
			actual = append(actual, val)
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("Range partially overlaps (start within, end after)", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		data := []int{10, 20, 30, 40, 50}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(30, 55)
		expected := []int{300, 400, 500}
		actual := []int{}
		for it.HasNext() {
			val, err := it.Next()
			assert.NoError(t, err)
			actual = append(actual, val)
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("Range completely within", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		data := []int{10, 20, 30, 40, 50}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(20, 40)
		expected := []int{200, 300, 400}
		actual := []int{}
		for it.HasNext() {
			val, err := it.Next()
			assert.NoError(t, err)
			actual = append(actual, val)
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("Start and end keys are the same (single element)", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		data := []int{10, 20, 30, 40, 50}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(30, 30)
		expected := []int{300}
		actual := []int{}
		for it.HasNext() {
			val, err := it.Next()
			assert.NoError(t, err)
			actual = append(actual, val)
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("Start and end keys are the same (element not present)", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		data := []int{10, 20, 30, 40, 50}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(25, 25)
		assert.False(t, it.HasNext())
		_, err = it.Next()
		assert.ErrorIs(t, err, EOI)
	})

	t.Run("Start key greater than end key", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		data := []int{10, 20, 30, 40, 50}
		for _, v := range data {
			list.Put(v, v*10)
		}

		it := list.IRange(30, 20)
		assert.False(t, it.HasNext())
		_, err = it.Next()
		assert.ErrorIs(t, err, EOI)
	})

	t.Run("List with duplicate keys (should only return one value per key)", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		list.Put(10, 100)
		list.Put(20, 200)
		list.Put(10, 101) // Overwrites 100
		list.Put(30, 300)

		it := list.IRange(5, 35)
		expected := []int{101, 200, 300}
		actual := []int{}
		for it.HasNext() {
			val, err := it.Next()
			assert.NoError(t, err)
			actual = append(actual, val)
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("Iterator behavior after reaching end", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		list.Put(1, 10)
		list.Put(2, 20)

		it := list.IRange(1, 2)
		val1, err := it.Next()
		assert.NoError(t, err)
		assert.Equal(t, 10, val1)
		assert.True(t, it.HasNext())

		val2, err := it.Next()
		assert.NoError(t, err)
		assert.Equal(t, 20, val2)
		assert.False(t, it.HasNext())

		_, err = it.Next()
		assert.ErrorIs(t, err, EOI)
		assert.False(t, it.HasNext()) // Should still be false
	})
}

func TestSkipList_FindGreaterOrEqual(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)

	t.Run("Empty list", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		node, err := list.FindGreaterOrEqual(5)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Nil(t, node)
	})

	t.Run("Existing elements", func(t *testing.T) {
		list, err := InitSkipList[int, int](cfg)
		assert.NoError(t, err)

		for _, k := range []int{10, 20, 30, 40, 50} {
			list.Put(k, k*10)
		}

		tests := []struct {
			name    string
			search  int
			wantKey int
			wantVal int
			wantErr error
		}{
			{
				name:    "exact match on first element",
				search:  10,
				wantKey: 10,
				wantVal: 100,
			},
			{
				name:    "exact match",
				search:  30,
				wantKey: 30,
				wantVal: 300,
			},
			{
				name:    "exact match on last element",
				search:  50,
				wantKey: 50,
				wantVal: 500,
			},
			{
				name:    "between keys",
				search:  25,
				wantKey: 30,
				wantVal: 300,
			},
			{
				name:    "before first",
				search:  5,
				wantKey: 10,
				wantVal: 100,
			},
			{
				name:    "after last",
				search:  60,
				wantErr: ErrKeyNotFound,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				node, err := list.FindGreaterOrEqual(tt.search)
				if tt.wantErr != nil {
					assert.ErrorIs(t, err, tt.wantErr)
					assert.Nil(t, node)
					return
				}

				assert.NoError(t, err)
				if assert.NotNil(t, node) {
					assert.Equal(t, tt.wantKey, node.Key)
					assert.Equal(t, tt.wantVal, node.Value)
				}
			})
		}
	})
}
