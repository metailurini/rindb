package rindb

import "sync/atomic"

// fileNumberAllocator issues sequential file numbers.
type fileNumberAllocator struct {
	// next is the next file number to be allocated.
	// It must be 64-bit aligned for atomic operations on 32-bit platforms.
	// Go guarantees this for struct fields.
	next uint64
}

// newFileNumberAllocator returns an allocator starting at start.
// The first call to next will return start.
func newFileNumberAllocator(start uint64) *fileNumberAllocator {
	return &fileNumberAllocator{next: start}
}

// nextNumber returns the current file number and increments the allocator.
func (a *fileNumberAllocator) nextNumber() uint64 {
	return atomic.AddUint64(&a.next, 1) - 1
}

// set updates the next file number to v.
func (a *fileNumberAllocator) set(v uint64) {
	atomic.StoreUint64(&a.next, v)
}

// peek returns the next number without incrementing.
func (a *fileNumberAllocator) peek() uint64 {
	return atomic.LoadUint64(&a.next)
}

// apply updates the allocator using the nextFileNumber from edit if present.
func (a *fileNumberAllocator) apply(edit versionEdit) {
	if edit.NextFileNumber != 0 {
		a.set(edit.NextFileNumber)
	}
}
