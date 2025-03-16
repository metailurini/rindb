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

	t.Run("Remove last node updates lastNode", func(t *testing.T) {
		l := InitLinkedList[int]()
		values := []int{1, 2, 3}
		for _, v := range values {
			l.PushBack(v)
		}

		iterator := l.Iterator()
		_, err := iterator.Next() // Move to 1
		assert.NoError(t, err)
		_, err = iterator.Next() // Move to 2
		assert.NoError(t, err)

		// Remove 3 (last node)
		err = iterator.RemoveNext()
		assert.NoError(t, err)

		// Verify lastNode is updated
		assert.Equal(t, 2, l.lastNode.Value)
		assert.Equal(t, 2, l.Len())

		// Verify backward traversal
		bottomIterator := l.IteratorFromBottom()
		assert.Equal(t, 2, bottomIterator.Value())
		value, err := bottomIterator.Prev()
		assert.NoError(t, err)
		assert.Equal(t, 1, value)
	})

	t.Run("Remove only node resets to sentinel", func(t *testing.T) {
		l := InitLinkedList[int]()
		l.PushBack(42)

		iterator := l.Iterator()
		assert.Equal(t, 1, l.Len())

		err := iterator.RemoveNext()
		assert.NoError(t, err)

		// Verify list state
		assert.Equal(t, 0, l.Len())
		assert.Equal(t, l.rootNode, l.lastNode)
		assert.False(t, iterator.HasNext())
	})
}

func TestLinkedListConcurrentModification(t *testing.T) {
	t.Run("PushBack during iteration", func(t *testing.T) {
		l := InitLinkedList[int]()
		l.PushBack(1)
		l.PushBack(2)

		iterator := l.Iterator()
		_, err := iterator.Next() // Move to 1
		assert.NoError(t, err)

		// Modify list during iteration
		l.PushBack(3)

		// Iterator should still work with original nodes
		value, err := iterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, 2, value)

		// Can reach new node
		value, err = iterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, 3, value)
	})
}

func TestSentinelNodeValue(t *testing.T) {
	t.Run("Sentinel node value on empty list", func(t *testing.T) {
		l := InitLinkedList[int]()
		iterator := l.Iterator()

		// Verify sentinel behavior
		assert.Equal(t, 0, iterator.Value()) // Zero value for int
		assert.False(t, iterator.HasNext())
	})
}

func TestBackwardTraversalAfterRemoval(t *testing.T) {
	t.Run("Backward traversal after removal", func(t *testing.T) {
		l := InitLinkedList[int]()
		values := []int{1, 2, 3, 4}
		for _, v := range values {
			l.PushBack(v)
		}

		// Remove node 3
		iterator := l.Iterator()
		_, err := iterator.Next() // Move to 1
		assert.NoError(t, err)
		_, err = iterator.Next() // Move to 2
		assert.NoError(t, err)
		err = iterator.RemoveNext() // Remove 3
		assert.NoError(t, err)

		// Verify backward traversal
		bottomIterator := l.IteratorFromBottom()
		assert.Equal(t, 4, bottomIterator.Value())

		value, err := bottomIterator.Prev()
		assert.NoError(t, err)
		assert.Equal(t, 2, value)

		value, err = bottomIterator.Prev()
		assert.NoError(t, err)
		assert.Equal(t, 1, value)
	})
}

func TestIteratorFromBottomAfterModification(t *testing.T) {
	t.Run("IteratorFromBottom after last node removal", func(t *testing.T) {
		l := InitLinkedList[int]()
		values := []int{1, 2, 3}
		for _, v := range values {
			l.PushBack(v)
		}

		// Remove last node using forward iterator
		iterator := l.Iterator()
		_, err := iterator.Next() // Move to 1
		assert.NoError(t, err)
		_, err = iterator.Next() // Move to 2
		assert.NoError(t, err)
		err = iterator.RemoveNext() // Remove 3
		assert.NoError(t, err)

		// Verify IteratorFromBottom starts at new last node
		bottomIterator := l.IteratorFromBottom()
		assert.Equal(t, 2, bottomIterator.Value())

		value, err := bottomIterator.Prev()
		assert.NoError(t, err)
		assert.Equal(t, 1, value)
	})
}

func TestLinkedListBackwardTraversal(t *testing.T) {
	t.Run("backward traversal after pushing values", func(t *testing.T) {
		l := InitLinkedList[int]()
		values := []int{1, 2, 3, 4, 5}
		for _, v := range values {
			l.PushBack(v)
		}

		// Move iterator to the end
		iterator := l.Iterator()
		for iterator.HasNext() {
			_, err := iterator.Next()
			assert.NoError(t, err)
		}

		// Check last value is 5
		assert.Equal(t, values[len(values)-1], iterator.Value())

		// Traverse backward
		// 4, 3, 2, 1
		for i := len(values) - 2; i >= 0; i-- {
			assert.True(t, iterator.HasPrev())
			value, err := iterator.Prev()
			assert.NoError(t, err)
			assert.Equal(t, values[i], value)
		}

		// Should not be able to go back further
		assert.False(t, iterator.HasPrev())
		_, err := iterator.Prev()
		assert.ErrorIs(t, err, EOI)
	})
}

func TestIteratorFromBottom(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		l := InitLinkedList[int]()
		iterator := l.IteratorFromBottom()
		assert.False(t, iterator.HasNext())
		assert.False(t, iterator.HasPrev())
		value, err := iterator.Next()
		assert.ErrorIs(t, err, EOI)
		assert.Equal(t, 0, value)
		value, err = iterator.Prev()
		assert.ErrorIs(t, err, EOI)
		assert.Equal(t, 0, value)
	})

	t.Run("single node", func(t *testing.T) {
		l := InitLinkedList[int]()
		l.PushBack(42)
		iterator := l.IteratorFromBottom()
		assert.Equal(t, 42, iterator.Value())
		assert.False(t, iterator.HasNext())
		assert.False(t, iterator.HasPrev())
		value, err := iterator.Prev()
		assert.ErrorIs(t, err, EOI)
		assert.Equal(t, 0, value)
	})

	t.Run("full backward traversal", func(t *testing.T) {
		l := InitLinkedList[int]()
		values := []int{1, 2, 3}
		for _, v := range values {
			l.PushBack(v)
		}
		iterator := l.IteratorFromBottom()
		assert.Equal(t, 3, iterator.Value())
		for i := len(values) - 2; i >= 0; i-- {
			assert.True(t, iterator.HasPrev())
			value, err := iterator.Prev()
			assert.NoError(t, err)
			assert.Equal(t, values[i], value)
		}
		assert.False(t, iterator.HasPrev())
	})

	t.Run("mixed traversal", func(t *testing.T) {
		l := InitLinkedList[int]()
		l.PushBack(1)
		l.PushBack(2)
		l.PushBack(3)
		iterator := l.IteratorFromBottom()
		assert.Equal(t, 3, iterator.Value())
		value, err := iterator.Prev()
		assert.NoError(t, err)
		assert.Equal(t, 2, value)
		value, err = iterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, 3, value)
	})

	t.Run("value before traversal on empty list", func(t *testing.T) {
		l := InitLinkedList[int]()
		iterator := l.IteratorFromBottom()
		assert.Equal(t, 0, iterator.Value()) // Note: uninitialized value
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
