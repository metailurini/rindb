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

func (l *fileProcessLock) Acquire(ctx context.Context) error {
	if l == nil {
		return nil
	}
	_ = l.fd
	l.log.debug(ctx, "process lock not yet implemented: %s", l.path)
	return nil
}

func (l *fileProcessLock) Release() error {
	if l == nil {
		return nil
	}
	return nil
}
