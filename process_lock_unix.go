//go:build unix

package rindb

import (
	"context"
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func (l *fileProcessLock) Acquire(ctx context.Context) error {
	if l == nil {
		return nil
	}
	if l.fd != nil {
		return nil
	}

	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return fmt.Errorf("open lock file %s: %w", l.path, err)
	}

	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return fmt.Errorf("database is already open (lock %s busy)", l.path)
		}
		return fmt.Errorf("flock %s: %w", l.path, err)
	}

	l.fd = f
	l.log.info(ctx, "acquired process lock %s", l.path)
	return nil
}

func (l *fileProcessLock) Release() error {
	if l == nil || l.fd == nil {
		return nil
	}

	fd := l.fd
	l.fd = nil

	unlockErr := unix.Flock(int(fd.Fd()), unix.LOCK_UN)
	closeErr := fd.Close()

	if unlockErr != nil {
		return fmt.Errorf("unlock %s: %w", l.path, unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close %s: %w", l.path, closeErr)
	}
	return nil
}
