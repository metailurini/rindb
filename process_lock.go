package rindb

import (
	"context"
	"os"
)

type processLock interface {
	Acquire(ctx context.Context) error
	Release() error
}

type fileProcessLock struct {
	path string
	log  scopedLogger
	fd   *os.File
}

var (
	_ processLock = (*fileProcessLock)(nil)
	_             = newProcessLock
)

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
