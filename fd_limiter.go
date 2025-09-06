package rindb

import "context"

// SemaphoreFDLimiter limits concurrent file descriptor usage using a semaphore.
// It implements the FDLimiter interface for table cache operations.
type SemaphoreFDLimiter struct {
	sem chan struct{}
}

// NewSemaphoreFDLimiter creates a limiter that permits up to n concurrent
// acquisitions. n must be >0.
func NewSemaphoreFDLimiter(n int) *SemaphoreFDLimiter {
	if n <= 0 {
		n = 1
	}
	return &SemaphoreFDLimiter{sem: make(chan struct{}, n)}
}

// Acquire blocks until a slot is available or the context is done.
func (l *SemaphoreFDLimiter) Acquire(ctx context.Context) error {
	select {
	case l.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release frees a slot previously acquired.
func (l *SemaphoreFDLimiter) Release() {
	select {
	case <-l.sem:
	default:
		// Release should only be called after a successful Acquire, but avoid
		// blocking here if misused.
	}
}
