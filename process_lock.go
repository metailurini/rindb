package rindb

import (
	"context"
	"os"
)

// processLock defines the interface for acquiring and releasing process locks.
type processLock interface {
	Acquire(ctx context.Context) error
	Release() error
}

// fileProcessLock implements processLock using file-based locking mechanism.
type fileProcessLock struct {
	path string
	log  scopedLogger
	fd   *os.File
}

var (
	_ processLock = (*fileProcessLock)(nil)
	_             = newProcessLock
)

// newProcessLock creates a new fileProcessLock instance.
func newProcessLock(path string, log scopedLogger) *fileProcessLock {
	return &fileProcessLock{path: path, log: log}
}
