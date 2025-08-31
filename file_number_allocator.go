package rindb

import "sync/atomic"

// FileNumberAllocator issues sequential file numbers.
type FileNumberAllocator struct {
	// next is the next file number to be allocated.
	// It must be 64-bit aligned for atomic operations on 32-bit platforms.
	// Go guarantees this for struct fields.
	next uint64
}

// NewFileNumberAllocator returns an allocator starting at start.
// The first call to Next will return start.
func NewFileNumberAllocator(start uint64) *FileNumberAllocator {
	return &FileNumberAllocator{next: start}
}

// Next returns the current file number and increments the allocator.
func (a *FileNumberAllocator) Next() uint64 {
	return atomic.AddUint64(&a.next, 1) - 1
}

// Set updates the next file number to v.
func (a *FileNumberAllocator) Set(v uint64) {
	atomic.StoreUint64(&a.next, v)
}

// Peek returns the next number without incrementing.
func (a *FileNumberAllocator) Peek() uint64 {
	return atomic.LoadUint64(&a.next)
}
