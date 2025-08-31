package rindb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const fileSystemPermission = 0o600

var (
	_ io.ReadWriteCloser = (*FileSystem)(nil)

	ErrFileNotOpened = errors.New("file is not opened")
)

type FileSystem struct {
	mu       sync.RWMutex
	filePath string
	file     *os.File
}

func OpenFS(ctx context.Context, filePath string) (*FileSystem, error) {
	fs := &FileSystem{filePath: filePath}
	if err := fs.Open(ctx); err != nil {
		return nil, err
	}
	return fs, nil
}

func NewFS(file *os.File) *FileSystem {
	return &FileSystem{filePath: file.Name(), file: file}
}

func (fs *FileSystem) IsOpened() bool {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.file != nil
}

func (fs *FileSystem) Open(ctx context.Context) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.file != nil {
		WARN(ctx, "File %s is already opened. Consider close and re-open again", fs.Path())
		return nil
	}

	file, err := os.OpenFile(filepath.Clean(fs.filePath), os.O_RDWR|os.O_CREATE, fileSystemPermission)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", fs.filePath, err)
	}
	fs.file = file
	return nil
}

func (fs *FileSystem) Path() string {
	return fs.filePath
}

func (fs *FileSystem) Sync() error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.file == nil {
		return ErrFileNotOpened
	}
	return fs.file.Sync()
}

func (fs *FileSystem) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.file == nil {
		return nil
	}

	if err := fs.file.Close(); err != nil {
		return err
	}
	fs.file = nil
	return nil
}

// Rename moves the underlying file to newPath.
// It closes the file if it's open and updates the internal path.
func (fs *FileSystem) Rename(newPath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.file != nil {
		if err := fs.file.Close(); err != nil {
			return err
		}
		fs.file = nil
	}
	if err := os.Rename(fs.filePath, newPath); err != nil {
		return err
	}
	fs.filePath = newPath
	return nil
}

func (fs *FileSystem) Clean() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.file == nil {
		return ErrFileNotOpened
	}

	if err := fs.file.Close(); err != nil {
		return fmt.Errorf("failed to close file %s before cleaning: %w", fs.Path(), err)
	}

	cleanFile, err := os.OpenFile(fs.Path(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, fileSystemPermission)
	if err != nil {
		return fmt.Errorf("failed to open/truncate file %s for cleaning: %w", fs.Path(), err)
	}
	fs.file = cleanFile

	if err := fs.file.Sync(); err != nil {
		_ = fs.file.Close()
		fs.file = nil
		return fmt.Errorf("failed to sync file %s after cleaning: %w", fs.Path(), err)
	}

	return nil
}

// CursorPos get current cursor position in file system
func (fs *FileSystem) CursorPos() (int64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.file == nil {
		return 0, ErrFileNotOpened
	}

	return fs.file.Seek(0, io.SeekCurrent)
}

func (fs *FileSystem) Write(p []byte) (int, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.file == nil {
		return 0, ErrFileNotOpened
	}

	return fs.file.Write(p)
}

func (fs *FileSystem) Read(p []byte) (int, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.file == nil {
		return 0, ErrFileNotOpened
	}

	return fs.file.Read(p)
}

func (fs *FileSystem) Seek(offset int64, whence int) (int64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.file == nil {
		return 0, ErrFileNotOpened
	}

	return fs.file.Seek(offset, whence)
}

func (fs *FileSystem) ReadAt(p []byte, off int64) (int, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.file == nil {
		return 0, ErrFileNotOpened
	}

	return fs.file.ReadAt(p, off)
}

func (fs *FileSystem) WriteAt(p []byte, off int64) (int, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.file == nil {
		return 0, ErrFileNotOpened
	}

	return fs.file.WriteAt(p, off)
}
