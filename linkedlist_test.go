package rindb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLinkedList(t *testing.T) {
	t.Run("On the initialization state last node is the same with root node", func(t *testing.T) {
		l := newLinkedList[int]()
		assert.Equal(t, l.lastNode, l.rootNode)

		l.lastNode.Value = 1
		assert.Equal(t, 1, l.rootNode.Value)
		assert.Equal(t, 1, l.lastNode.Value)
	})
}

func TestLinkedListPushBack(t *testing.T) {
	t.Run("Push value from a slice", func(t *testing.T) {
		l := newLinkedList[int]()
		slice := []int{1, 2, 3, 4, 5, 6}
		for _, v := range slice {
			l.pushBack(v)
		}
		assert.Equal(t, len(slice), l.size())

		assertLinkedListContents(t, l, slice)
	})
}

func TestLinkedListPushFront(t *testing.T) {
	t.Run("Push to an empty list", func(t *testing.T) {
		l := newLinkedList[int]()
		l.pushFront(10)

		assert.Equal(t, 1, l.size())
		assert.Equal(t, 10, l.rootNode.next.Value)
		assert.Equal(t, 10, l.lastNode.Value)
		assert.Equal(t, l.rootNode.next, l.lastNode) // For a single node, root.next should be lastNode
		assert.Nil(t, l.rootNode.next.next)
		assert.Equal(t, l.rootNode, l.rootNode.next.prev)
	})

	t.Run("Push multiple values to the front", func(t *testing.T) {
		l := newLinkedList[int]()
		l.pushFront(1) // List: 1
		l.pushFront(2) // List: 2, 1
		l.pushFront(3) // List: 3, 2, 1

		assert.Equal(t, 3, l.size())
		assert.Equal(t, 3, l.rootNode.next.Value) // First node should be 3
		assert.Equal(t, 1, l.lastNode.Value)      // Last node should still be 1

		expected := []int{3, 2, 1}
		assertLinkedListContents(t, l, expected)

		// Verify prev/next pointers for all nodes
		node3 := l.rootNode.next
		node2 := node3.next
		node1 := node2.next

		assert.Equal(t, l.rootNode, node3.prev)
		assert.Equal(t, node2, node3.next)

		assert.Equal(t, node3, node2.prev)
		assert.Equal(t, node1, node2.next)

		assert.Equal(t, node2, node1.prev)
		assert.Nil(t, node1.next)
	})

	t.Run("pushFront after pushBack", func(t *testing.T) {
		l := newLinkedList[int]()
		l.pushBack(1)  // List: 1
		l.pushBack(2)  // List: 1, 2
		l.pushFront(0) // List: 0, 1, 2

		assert.Equal(t, 3, l.size())
		assert.Equal(t, 0, l.rootNode.next.Value) // First node should be 0
		assert.Equal(t, 2, l.lastNode.Value)      // Last node should still be 2

		expected := []int{0, 1, 2}
		assertLinkedListContents(t, l, expected)
	})
}

//nolint:funlen
func TestLinkedListIterator(t *testing.T) {
	t.Run("Check Iterator for the empty linked list", func(t *testing.T) {
		l := newLinkedList[int]()
		iterator := l.iterator()
		assert.False(t, iterator.hasNext())
		value, err := iterator.next()
		assert.ErrorIs(t, err, EOI)
		assert.Equal(t, 0, value)
	})

	t.Run("Check Iterator run after pushing back to linked list", func(t *testing.T) {
		l := newLinkedList[int]()
		l.pushBack(1)
		l.pushBack(2)
		l.pushBack(3)
		iterator := l.iterator()

		assert.True(t, iterator.hasNext())
		value, err := iterator.next()
		assert.NoError(t, err)
		assert.Equal(t, 1, value)

		value, err = iterator.nextValue()
		assert.NoError(t, err)
		assert.Equal(t, 2, value)

		assert.True(t, iterator.hasNext())
		value, err = iterator.next()
		assert.NoError(t, err)
		assert.Equal(t, 2, value)

		value, err = iterator.nextValue()
		assert.NoError(t, err)
		assert.Equal(t, 3, value)

		assert.True(t, iterator.hasNext())
		value, err = iterator.next()
		assert.NoError(t, err)
		assert.Equal(t, 3, value)

		value, err = iterator.nextValue()
		assert.ErrorIs(t, err, EOI)
		assert.Equal(t, 0, value)

		assert.False(t, iterator.hasNext())
		value, err = iterator.next()
		assert.ErrorIs(t, err, EOI)
		assert.Equal(t, 0, value)
	})

	t.Run("Remove last node updates lastNode", func(t *testing.T) {
		l := newLinkedList[int]()
		values := []int{1, 2, 3}
		for _, v := range values {
			l.pushBack(v)
		}

		iterator := l.iterator()
		_, err := iterator.next() // Move to 1
		assert.NoError(t, err)
		_, err = iterator.next() // Move to 2
		assert.NoError(t, err)

		// Remove 3 (last node)
		err = iterator.removeNext()
		assert.NoError(t, err)

		// Verify lastNode is updated
		assert.Equal(t, 2, l.lastNode.Value)
		assert.Equal(t, 2, l.size())

		// Verify backward traversal
		bottomIterator := l.iteratorFromBottom()
		assert.Equal(t, 2, bottomIterator.value())
		value, err := bottomIterator.prev()
		assert.NoError(t, err)
		assert.Equal(t, 1, value)
	})

	t.Run("Remove only node resets to sentinel", func(t *testing.T) {
		l := newLinkedList[int]()
		l.pushBack(42)

		iterator := l.iterator()
		assert.Equal(t, 1, l.size())

		err := iterator.removeNext()
		assert.NoError(t, err)

		// Verify list state
		assert.Equal(t, 0, l.size())
		assert.Equal(t, l.rootNode, l.lastNode)
		assert.False(t, iterator.hasNext())
	})
}

//nolint:funlen
func TestLinkedListRemoveCurrent(t *testing.T) {
	t.Run("Remove node in the middle", func(t *testing.T) {
		l := newLinkedList[int]()
		values := []int{1, 2, 3, 4}
		for _, v := range values {
			l.pushBack(v)
		}
		iterator := l.iterator()
		_, err := iterator.next() // Move to 1
		assert.NoError(t, err)
		_, err = iterator.next() // Move to 2
		assert.NoError(t, err)

		// Remove node 2
		err = iterator.removeCurrent()
		assert.NoError(t, err)
		assert.Equal(t, 3, l.size())
		assert.Equal(t, 1, iterator.value()) // Iterator moved back to 1

		// Check remaining values
		expected := []int{1, 3, 4}
		assertLinkedListContents(t, l, expected)
		assert.Equal(t, 4, l.lastNode.Value) // lastNode should still be 4
	})

	t.Run("Remove first node", func(t *testing.T) {
		l := newLinkedList[int]()
		l.pushBack(1)
		l.pushBack(2)
		iterator := l.iterator()
		_, err := iterator.next() // Move to 1
		assert.NoError(t, err)

		// Remove node 1
		err = iterator.removeCurrent()
		assert.NoError(t, err)
		assert.Equal(t, 1, l.size())
		assert.Equal(t, l.rootNode, iterator.runNode) // Iterator moved back to root

		// Check remaining value
		val, err := l.iterator().next()
		assert.NoError(t, err)
		assert.Equal(t, 2, val)
		assert.Equal(t, 2, l.lastNode.Value)
	})

	t.Run("Remove last node", func(t *testing.T) {
		l := newLinkedList[int]()
		l.pushBack(1)
		l.pushBack(2)
		l.pushBack(3)
		iterator := l.iterator()
		_, _ = iterator.next() // -> 1
		_, _ = iterator.next() // -> 2
		_, _ = iterator.next() // -> 3

		// Remove node 3
		err := iterator.removeCurrent()
		assert.NoError(t, err)
		assert.Equal(t, 2, l.size())
		assert.Equal(t, 2, iterator.value()) // Iterator moved back to 2
		assert.Equal(t, 2, l.lastNode.Value) // lastNode updated to 2

		// Check remaining values
		expected := []int{1, 2}
		assertLinkedListContents(t, l, expected)
	})

	t.Run("Attempt to remove sentinel node", func(t *testing.T) {
		l := newLinkedList[int]()
		l.pushBack(1)
		iterator := l.iterator() // Iterator at sentinel

		err := iterator.removeCurrent()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot remove root node")
		assert.Equal(t, 1, l.size()) // List unchanged
	})

	t.Run("Attempt to remove from empty list", func(t *testing.T) {
		l := newLinkedList[int]()
		iterator := l.iterator() // Iterator at sentinel

		err := iterator.removeCurrent()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot remove root node")
		assert.Equal(t, 0, l.size()) // List unchanged
	})

	t.Run("Attempt to remove after iterating past end (invalid state)", func(t *testing.T) {
		// Note: This scenario shouldn't ideally happen with correct HasNext usage,
		// but tests robustness if the iterator somehow ends up nil.
		l := newLinkedList[int]()
		l.pushBack(1)
		iterator := l.iterator()
		_, _ = iterator.next()    // -> 1
		_, err := iterator.next() // -> EOI, iterator.runNode might become nil depending on impl.
		assert.ErrorIs(t, err, EOI)

		// Force runNode to nil to simulate an invalid state post-iteration
		iterator.runNode = nil

		err = iterator.removeCurrent()
		assert.ErrorIs(t, err, EOI)  // Expect EOI or similar error indicating invalid state
		assert.Equal(t, 1, l.size()) // List unchanged
	})

	t.Run("Remove and check backward traversal", func(t *testing.T) {
		l := newLinkedList[int]()
		values := []int{1, 2, 3, 4}
		for _, v := range values {
			l.pushBack(v)
		}
		iterator := l.iterator()
		_, _ = iterator.next() // -> 1
		_, _ = iterator.next() // -> 2
		_, _ = iterator.next() // -> 3

		// Remove node 3
		err := iterator.removeCurrent()
		assert.NoError(t, err)
		assert.Equal(t, 2, iterator.value()) // Iterator is now at node 2

		// Traverse backward from node 2
		val, err := iterator.prev()
		assert.NoError(t, err)
		assert.Equal(t, 1, val)

		// Traverse forward from node 1
		val, err = iterator.next()
		assert.NoError(t, err)
		assert.Equal(t, 2, val)
		val, err = iterator.next()
		assert.NoError(t, err)
		assert.Equal(t, 4, val) // Should skip 3 and go to 4
	})
}

func TestLinkedListConcurrentModification(t *testing.T) {
	t.Run("pushBack during iteration", func(t *testing.T) {
		l := newLinkedList[int]()
		l.pushBack(1)
		l.pushBack(2)

		iterator := l.iterator()
		_, err := iterator.next() // Move to 1
		assert.NoError(t, err)

		// Modify list during iteration
		l.pushBack(3)

		// Iterator should still work with original nodes
		value, err := iterator.next()
		assert.NoError(t, err)
		assert.Equal(t, 2, value)

		// Can reach new node
		value, err = iterator.next()
		assert.NoError(t, err)
		assert.Equal(t, 3, value)
	})
}

func TestSentinelNodeValue(t *testing.T) {
	t.Run("Sentinel node value on empty list", func(t *testing.T) {
		l := newLinkedList[int]()
		iterator := l.iterator()

		// Verify sentinel behavior
		assert.Equal(t, 0, iterator.value()) // Zero value for int
		assert.False(t, iterator.hasNext())
	})
}

func TestBackwardTraversalAfterRemoval(t *testing.T) {
	t.Run("Backward traversal after removal", func(t *testing.T) {
		l := newLinkedList[int]()
		values := []int{1, 2, 3, 4}
		for _, v := range values {
			l.pushBack(v)
		}

		// Remove node 3
		iterator := l.iterator()
		_, err := iterator.next() // Move to 1
		assert.NoError(t, err)
		_, err = iterator.next() // Move to 2
		assert.NoError(t, err)
		err = iterator.removeNext() // Remove 3
		assert.NoError(t, err)

		// Verify backward traversal
		bottomIterator := l.iteratorFromBottom()
		assert.Equal(t, 4, bottomIterator.value())

		value, err := bottomIterator.prev()
		assert.NoError(t, err)
		assert.Equal(t, 2, value)

		value, err = bottomIterator.prev()
		assert.NoError(t, err)
		assert.Equal(t, 1, value)
	})
}

func TestIteratorFromBottomAfterModification(t *testing.T) {
	t.Run("iteratorFromBottom after last node removal", func(t *testing.T) {
		l := newLinkedList[int]()
		values := []int{1, 2, 3}
		for _, v := range values {
			l.pushBack(v)
		}

		// Remove last node using forward iterator
		iterator := l.iterator()
		_, err := iterator.next() // Move to 1
		assert.NoError(t, err)
		_, err = iterator.next() // Move to 2
		assert.NoError(t, err)
		err = iterator.removeNext() // Remove 3
		assert.NoError(t, err)

		// Verify iteratorFromBottom starts at new last node
		bottomIterator := l.iteratorFromBottom()
		assert.Equal(t, 2, bottomIterator.value())

		value, err := bottomIterator.prev()
		assert.NoError(t, err)
		assert.Equal(t, 1, value)
	})
}

func TestLinkedListBackwardTraversal(t *testing.T) {
	t.Run("Backward traversal after pushing values", func(t *testing.T) {
		l := newLinkedList[int]()
		values := []int{1, 2, 3, 4, 5}
		for _, v := range values {
			l.pushBack(v)
		}

		// Move iterator to the end
		iterator := l.iterator()
		for iterator.hasNext() {
			_, err := iterator.next()
			assert.NoError(t, err)
		}

		// Check last value is 5
		assert.Equal(t, values[len(values)-1], iterator.value())

		// Traverse backward
		// 4, 3, 2, 1
		for i := len(values) - 2; i >= 0; i-- {
			assert.True(t, iterator.hasPrev())
			value, err := iterator.prev()
			assert.NoError(t, err)
			assert.Equal(t, values[i], value)
		}

		// Should not be able to go back further
		assert.False(t, iterator.hasPrev())
		_, err := iterator.prev()
		assert.ErrorIs(t, err, EOI)
	})
}

func TestIteratorFromBottom(t *testing.T) {
	t.Run("Empty list", func(t *testing.T) {
		l := newLinkedList[int]()
		iterator := l.iteratorFromBottom()
		assert.False(t, iterator.hasNext())
		assert.False(t, iterator.hasPrev())
		value, err := iterator.next()
		assert.ErrorIs(t, err, EOI)
		assert.Equal(t, 0, value)
		value, err = iterator.prev()
		assert.ErrorIs(t, err, EOI)
		assert.Equal(t, 0, value)
	})

	t.Run("Single node", func(t *testing.T) {
		l := newLinkedList[int]()
		l.pushBack(42)
		iterator := l.iteratorFromBottom()
		assert.Equal(t, 42, iterator.value())
		assert.False(t, iterator.hasNext())
		assert.False(t, iterator.hasPrev())
		value, err := iterator.prev()
		assert.ErrorIs(t, err, EOI)
		assert.Equal(t, 0, value)
	})

	t.Run("Full backward traversal", func(t *testing.T) {
		l := newLinkedList[int]()
		values := []int{1, 2, 3}
		for _, v := range values {
			l.pushBack(v)
		}
		iterator := l.iteratorFromBottom()
		assert.Equal(t, 3, iterator.value())
		for i := len(values) - 2; i >= 0; i-- {
			assert.True(t, iterator.hasPrev())
			value, err := iterator.prev()
			assert.NoError(t, err)
			assert.Equal(t, values[i], value)
		}
		assert.False(t, iterator.hasPrev())
	})

	t.Run("Mixed traversal", func(t *testing.T) {
		l := newLinkedList[int]()
		l.pushBack(1)
		l.pushBack(2)
		l.pushBack(3)
		iterator := l.iteratorFromBottom()
		assert.Equal(t, 3, iterator.value())
		value, err := iterator.prev()
		assert.NoError(t, err)
		assert.Equal(t, 2, value)
		value, err = iterator.next()
		assert.NoError(t, err)
		assert.Equal(t, 3, value)
	})

	t.Run("Value before traversal on empty list", func(t *testing.T) {
		l := newLinkedList[int]()
		iterator := l.iteratorFromBottom()
		assert.Equal(t, 0, iterator.value()) // Note: uninitialized value
	})
}
