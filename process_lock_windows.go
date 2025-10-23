//go:build windows

package rindb

import (
	"context"
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

const lockFileAllBytes = ^uint32(0)

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

	handle := windows.Handle(f.Fd())
	var overlapped windows.Overlapped
	err = windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, lockFileAllBytes, lockFileAllBytes, &overlapped)
	if err != nil {
		_ = f.Close()
		return translateLockFileError(l.path, err)
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

	handle := windows.Handle(fd.Fd())
	var overlapped windows.Overlapped
	unlockErr := windows.UnlockFileEx(handle, 0, lockFileAllBytes, lockFileAllBytes, &overlapped)
	closeErr := fd.Close()

	if unlockErr != nil {
		return fmt.Errorf("UnlockFileEx %s: %w", l.path, unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close %s: %w", l.path, closeErr)
	}
	return nil
}

func translateLockFileError(path string, err error) error {
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return fmt.Errorf("database is already open (lock %s busy): %w", path, err)
	}
	return fmt.Errorf("LockFileEx %s: %w", path, err)
}
