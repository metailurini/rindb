package rindb

import "sync"

// FileNumberAllocator issues sequential file numbers.
type FileNumberAllocator struct {
	mu   sync.Mutex
	next uint64
}

// NewFileNumberAllocator returns an allocator starting at start.
// The first call to Next will return start.
func NewFileNumberAllocator(start uint64) *FileNumberAllocator {
	return &FileNumberAllocator{next: start}
}

// Next returns the current file number and increments the allocator.
func (a *FileNumberAllocator) Next() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := a.next
	a.next++
	return n
}

// Set updates the next file number to v.
func (a *FileNumberAllocator) Set(v uint64) {
	a.mu.Lock()
	a.next = v
	a.mu.Unlock()
}

// Peek returns the next number without incrementing.
func (a *FileNumberAllocator) Peek() uint64 {
	a.mu.Lock()
	n := a.next
	a.mu.Unlock()
	return n
}
