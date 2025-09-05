package rindb

import "container/heap"

// PriorityQueue is a generic min-heap based priority queue. It is exported so
// advanced users can leverage the implementation when building their own
// schedulers or data-structure helpers.
type PriorityQueue[T any] struct {
	items []T
	less  func(a, b T) bool
}

// NewPriorityQueue creates a new priority queue with the given less function.
func NewPriorityQueue[T any](less func(a, b T) bool) *PriorityQueue[T] {
	pq := &PriorityQueue[T]{less: less}
	heap.Init(pq)
	return pq
}

// Len implements heap.Interface.
func (pq PriorityQueue[T]) Len() int { return len(pq.items) }

// Less implements heap.Interface.
func (pq PriorityQueue[T]) Less(i, j int) bool { return pq.less(pq.items[i], pq.items[j]) }

// Swap implements heap.Interface.
func (pq PriorityQueue[T]) Swap(i, j int) { pq.items[i], pq.items[j] = pq.items[j], pq.items[i] }

// Push implements heap.Interface.
func (pq *PriorityQueue[T]) Push(x any) { pq.items = append(pq.items, x.(T)) }

// Pop implements heap.Interface.
func (pq *PriorityQueue[T]) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]
	pq.items = old[0 : n-1]
	return item
}

// PushItem pushes an item onto the priority queue.
func (pq *PriorityQueue[T]) PushItem(item T) { heap.Push(pq, item) }

// PopItem pops the highest priority item from the queue.
func (pq *PriorityQueue[T]) PopItem() T { return heap.Pop(pq).(T) }
