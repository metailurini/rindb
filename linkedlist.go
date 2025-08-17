package rindb

import (
	"errors"
)

type llNode[V any] struct {
	next  *llNode[V]
	prev  *llNode[V]
	Value V
}

type LinkedList[V any] struct {
	rootNode *llNode[V]
	lastNode *llNode[V]
	len      int
}

func InitLinkedList[V any]() *LinkedList[V] {
	l := new(LinkedList[V])
	rootNode := &llNode[V]{next: nil, prev: nil}
	l.rootNode = rootNode
	l.lastNode = rootNode
	return l
}

func (l *LinkedList[V]) PushBack(value V) {
	node := &llNode[V]{
		Value: value,
		prev:  l.lastNode,
		next:  nil,
	}
	l.lastNode.next = node
	l.lastNode = node
	l.len += 1
}

// PushFront adds a new node with the given value to the front of the list.
func (l *LinkedList[V]) PushFront(value V) {
	newNode := &llNode[V]{
		Value: value,
		prev:  l.rootNode,
		next:  l.rootNode.next,
	}

	if l.len == 0 {
		l.lastNode = newNode
	} else {
		l.rootNode.next.prev = newNode
	}
	l.rootNode.next = newNode
	l.len += 1
}

func (l *LinkedList[V]) Len() int {
	return l.len
}

func (l *LinkedList[V]) Iterator() *LLIterator[V] {
	return &LLIterator[V]{
		runNode: l.rootNode,
		list:    l,
	}
}

// IteratorFromBottom returns an iterator starting from the last node
func (l *LinkedList[V]) IteratorFromBottom() *LLIterator[V] {
	return &LLIterator[V]{
		runNode: l.lastNode,
		list:    l,
	}
}

type LLIterator[V any] struct {
	runNode *llNode[V]
	list    *LinkedList[V]
}

var _ Iterator[any] = (*LLIterator[any])(nil)

func (l *LLIterator[V]) Value() V {
	return l.runNode.Value
}

// HasNext implements Iterator.
func (l *LLIterator[V]) HasNext() bool {
	return l.runNode != nil && l.runNode.next != nil
}

// Next implements Iterator.
func (l *LLIterator[V]) Next() (V, error) {
	if !l.HasNext() {
		var emptyValue V
		return emptyValue, EOI
	}
	l.runNode = l.runNode.next
	return l.runNode.Value, nil
}

// NextValue get next value without jumping to next node
func (l *LLIterator[V]) NextValue() (V, error) {
	if !l.HasNext() {
		var emptyValue V
		return emptyValue, EOI
	}
	return l.runNode.next.Value, nil
}

// Add this method to type LLIterator[V] struct
func (l *LLIterator[V]) RemoveCurrent() error {
	if l.runNode == l.list.rootNode {
		return errors.New("cannot remove root node")
	}
	if l.runNode == nil {
		return EOI
	}
	// Connect previous to next
	if l.runNode.prev != nil {
		l.runNode.prev.next = l.runNode.next
	}
	if l.runNode.next != nil {
		l.runNode.next.prev = l.runNode.prev
	}
	// If removing the last node, update lastNode
	if l.runNode == l.list.lastNode {
		l.list.lastNode = l.runNode.prev
	}
	// Move iterator back to previous node to continue iteration
	oldNode := l.runNode
	l.runNode = l.runNode.prev
	l.list.len--
	// Prevent double-free by nullifying pointers
	oldNode.next = nil
	oldNode.prev = nil
	return nil
}

func (l *LLIterator[V]) RemoveNext() error {
	if !l.HasNext() {
		return EOI
	}
	removedNode := l.runNode.next
	l.runNode.next = removedNode.next

	// Update prev pointer of the next node
	if removedNode.next != nil {
		removedNode.next.prev = l.runNode
	}

	// Update lastNode if we're removing the last node
	if removedNode == l.list.lastNode {
		l.list.lastNode = l.runNode
	}

	l.list.len -= 1
	return nil
}

func (l *LLIterator[V]) PickNext() (V, error) {
	var emptyValue V

	value, err := l.NextValue()
	if err != nil {
		return emptyValue, err
	}

	if err := l.RemoveNext(); err != nil {
		return emptyValue, err
	}
	return value, nil
}

// HasPrev checks if there is a previous node to traverse to
func (l *LLIterator[V]) HasPrev() bool {
	return l.runNode != nil && l.runNode.prev != nil && l.runNode.prev != l.list.rootNode
}

// Prev moves to the previous node and returns its value
func (l *LLIterator[V]) Prev() (V, error) {
	if !l.HasPrev() {
		var emptyValue V
		return emptyValue, EOI
	}

	l.runNode = l.runNode.prev
	return l.runNode.Value, nil
}
