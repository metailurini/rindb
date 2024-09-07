package rindb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitLinkedList(t *testing.T) {
	t.Run("On the initialization state last node is the same with root node", func(t *testing.T) {
		l := InitLinkedList[int]()
		assert.Equal(t, l.lastNode, l.rootNode)

		l.lastNode.Value = 1
		assert.Equal(t, 1, l.rootNode.Value)
		assert.Equal(t, 1, l.lastNode.Value)
	})
}

func TestLinkedListPushBack(t *testing.T) {
	t.Run("push value from a slice", func(t *testing.T) {
		l := InitLinkedList[int]()
		slice := []int{1, 2, 3, 4, 5, 6}
		for _, v := range slice {
			l.PushBack(v)
		}
		assert.Equal(t, len(slice), l.Len())

		ri := 0
		iterator := l.Iterator()
		for iterator.HasNext() {
			value, err := iterator.Next()
			assert.NoError(t, err)
			assert.Equal(t, slice[ri], value)
			ri += 1
		}
	})
}

//nolint:funlen
func TestLinkedListIterator(t *testing.T) {
	t.Run("Check Iterator for the empty linked list", func(t *testing.T) {
		l := InitLinkedList[int]()
		iterator := l.Iterator()
		assert.False(t, iterator.HasNext())
		value, err := iterator.Next()
		assert.ErrorIs(t, err, EOI)
		assert.Equal(t, 0, value)
	})

	t.Run("Check Iterator run after pushing back to linked list", func(t *testing.T) {
		l := InitLinkedList[int]()
		l.PushBack(1)
		l.PushBack(2)
		l.PushBack(3)
		iterator := l.Iterator()

		assert.True(t, iterator.HasNext())
		value, err := iterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, 1, value)

		value, err = iterator.NextValue()
		assert.NoError(t, err)
		assert.Equal(t, 2, value)

		assert.True(t, iterator.HasNext())
		value, err = iterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, 2, value)

		value, err = iterator.NextValue()
		assert.NoError(t, err)
		assert.Equal(t, 3, value)

		assert.True(t, iterator.HasNext())
		value, err = iterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, 3, value)

		value, err = iterator.NextValue()
		assert.ErrorIs(t, err, EOI)
		assert.Equal(t, 0, value)

		assert.False(t, iterator.HasNext())
		value, err = iterator.Next()
		assert.ErrorIs(t, err, EOI)
		assert.Equal(t, 0, value)
	})

	t.Run("Remove next node", func(t *testing.T) {
		l := InitLinkedList[int]()
		l.PushBack(1)
		l.PushBack(2)
		l.PushBack(3)
		iterator := l.Iterator()

		_, err := iterator.Next() // 1
		assert.NoError(t, err)

		assert.Equal(t, 3, l.Len())
		err = iterator.RemoveNext() // remove 2
		assert.NoError(t, err)
		assert.Equal(t, 1, iterator.Value())
		assert.Equal(t, 2, l.Len())

		value, err := iterator.Next() // 3
		assert.NoError(t, err)
		assert.Equal(t, 3, value)
	})

	t.Run("pick next node", func(t *testing.T) {
		l := InitLinkedList[int]()
		l.PushBack(1)
		l.PushBack(2)
		l.PushBack(3)
		iterator := l.Iterator()

		_, err := iterator.Next() // 1
		assert.NoError(t, err)

		assert.Equal(t, 3, l.Len())
		value, err := iterator.PickNext()
		assert.NoError(t, err)
		assert.Equal(t, 2, value)
		assert.Equal(t, 2, l.Len())

		value, err = iterator.Next() // 3
		assert.NoError(t, err)
		assert.Equal(t, 3, value)
	})

	t.Run("test", func(t *testing.T) {
		l := InitLinkedList[int]()
		l.PushBack(1)
		l.PushBack(2)
		l.PushBack(3)
		iterator := l.Iterator()
		run(iterator)
	})
}

func run(iterator *LLIterator[int]) {
	if iterator.HasNext() {
		_, err := iterator.Next()
		if err != nil {
			return
		}
		currentValue := iterator.Value()
		run(iterator)
		fmt.Printf("iterator.Value(): %v\n", currentValue)
	}
}
