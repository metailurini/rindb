package rindb

import "errors"

type llNode[V any] struct {
	next  *llNode[V]
	Value V
}

type LinkedList[V any] struct {
	rootNode *llNode[V]
	lastNode *llNode[V]
	len      int
}

func InitLinkedList[V any]() *LinkedList[V] {
	l := new(LinkedList[V])
	rootNode := new(llNode[V])
	l.rootNode = rootNode
	l.lastNode = rootNode
	return l
}

func (l *LinkedList[V]) PushBack(value V) {
	node := new(llNode[V])
	node.Value = value
	l.lastNode.next = node
	l.lastNode = node
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

type LLIterator[V any] struct {
	runNode *llNode[V]
	list    *LinkedList[V]
}

// EOI is end of iteration
var EOI = errors.New("EOI")

// Iterator TODO: move it to somewhere
type Iterator[T any] interface {
	HasNext() bool
	Next() (T, error)
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

func (l *LLIterator[V]) RemoveNext() error {
	if !l.HasNext() {
		ERROR("WTF, what are you thinking about this action?")
		return EOI
	}
	removedNode := l.runNode.next
	l.runNode.next = removedNode.next
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
