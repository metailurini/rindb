//go:build !unix && !windows

package rindb

import "context"

func (l *fileProcessLock) Acquire(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.log.warn(ctx, "process locking is not supported on this platform; continuing without %s", l.path)
	return nil
}

func (l *fileProcessLock) Release() error {
	return nil
}
