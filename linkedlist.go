package rindb

import (
	"errors"
)

type llNode[V any] struct {
	next  *llNode[V]
	prev  *llNode[V]
	Value V
}

type linkedList[V any] struct {
	rootNode *llNode[V]
	lastNode *llNode[V]
	length   int
}

func newLinkedList[V any]() *linkedList[V] {
	l := new(linkedList[V])
	rootNode := &llNode[V]{next: nil, prev: nil}
	l.rootNode = rootNode
	l.lastNode = rootNode
	return l
}

func (l *linkedList[V]) pushBack(value V) {
	node := &llNode[V]{
		Value: value,
		prev:  l.lastNode,
		next:  nil,
	}
	l.lastNode.next = node
	l.lastNode = node
	l.length++
}

// pushFront adds a new node with the given value to the front of the list.
func (l *linkedList[V]) pushFront(value V) {
	newNode := &llNode[V]{
		Value: value,
		prev:  l.rootNode,
		next:  l.rootNode.next,
	}

	if l.length == 0 {
		l.lastNode = newNode
	} else {
		l.rootNode.next.prev = newNode
	}
	l.rootNode.next = newNode
	l.length++
}

func (l *linkedList[V]) size() int {
	return l.length
}

func (l *linkedList[V]) iterator() *llIterator[V] {
	return &llIterator[V]{
		runNode: l.rootNode,
		list:    l,
	}
}

// IteratorFromBottom returns an iterator starting from the last node
func (l *linkedList[V]) iteratorFromBottom() *llIterator[V] {
	return &llIterator[V]{
		runNode: l.lastNode,
		list:    l,
	}
}

type llIterator[V any] struct {
	runNode *llNode[V]
	list    *linkedList[V]
}

func (l *llIterator[V]) value() V {
	return l.runNode.Value
}

// hasNext implements Iterator.
func (l *llIterator[V]) hasNext() bool {
	return l.runNode != nil && l.runNode.next != nil
}

// next implements Iterator.
func (l *llIterator[V]) next() (V, error) {
	if !l.hasNext() {
		var emptyValue V
		return emptyValue, EOI
	}
	l.runNode = l.runNode.next
	return l.runNode.Value, nil
}

// nextValue get next value without jumping to next node
func (l *llIterator[V]) nextValue() (V, error) {
	if !l.hasNext() {
		var emptyValue V
		return emptyValue, EOI
	}
	return l.runNode.next.Value, nil
}

// removeCurrent removes the current node from the list and moves the iterator
// to the previous node. It returns an error if the current node cannot be
// removed or the iterator is exhausted.
func (l *llIterator[V]) removeCurrent() error {
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
	l.list.length--
	// Prevent double-free by nullifying pointers
	oldNode.next = nil
	oldNode.prev = nil
	return nil
}

func (l *llIterator[V]) removeNext() error {
	if !l.hasNext() {
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

	l.list.length--
	return nil
}

// HasPrev checks if there is a previous node to traverse to
func (l *llIterator[V]) hasPrev() bool {
	return l.runNode != nil && l.runNode.prev != nil && l.runNode.prev != l.list.rootNode
}

// Prev moves to the previous node and returns its value
func (l *llIterator[V]) prev() (V, error) {
	if !l.hasPrev() {
		var emptyValue V
		return emptyValue, EOI
	}

	l.runNode = l.runNode.prev
	return l.runNode.Value, nil
}
