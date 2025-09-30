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
	mu          sync.RWMutex
	filePath    string
	file        *os.File
	mmap        *mmapHandle
	mmapEnabled bool
	log         scopedLogger
}

func newFileSystem(filePath string) *FileSystem {
	return &FileSystem{filePath: filePath, log: newScopedLogger(nopLogger{}, LogLevelWarn)}
}

func OpenFS(ctx context.Context, filePath string) (*FileSystem, error) {
	fs := newFileSystem(filePath)
	if err := fs.Open(ctx); err != nil {
		return nil, err
	}
	return fs, nil
}

// OpenExistingFS opens a file system for an existing file without creating it if missing.
func OpenExistingFS(ctx context.Context, filePath string) (*FileSystem, error) {
	fs := newFileSystem(filePath)
	if err := fs.OpenExisting(ctx); err != nil {
		return nil, err
	}
	return fs, nil
}

func NewFS(file *os.File) *FileSystem {
	fs := newFileSystem(file.Name())
	fs.file = file
	return fs
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
		fs.maybeMmapLocked(ctx)
		return nil
	}

	file, err := os.OpenFile(filepath.Clean(fs.filePath), os.O_RDWR|os.O_CREATE, fileSystemPermission)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", fs.filePath, err)
	}
	fs.file = file
	fs.maybeMmapLocked(ctx)
	return nil
}

// OpenExisting opens the file system assuming the file already exists.
// It returns an error if the file does not exist.
func (fs *FileSystem) OpenExisting(ctx context.Context) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.file != nil {
		fs.maybeMmapLocked(ctx)
		return nil
	}

	file, err := os.OpenFile(filepath.Clean(fs.filePath), os.O_RDWR, fileSystemPermission)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", fs.filePath, err)
	}
	fs.file = file
	fs.maybeMmapLocked(ctx)
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

	fs.closeMmapLocked(context.Background())
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
		fs.closeMmapLocked(context.Background())
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

	fs.closeMmapLocked(context.Background())
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

func (fs *FileSystem) configureMmap(ctx context.Context, enabled bool, log scopedLogger) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if log.Logger == nil {
		log = newScopedLogger(nopLogger{}, LogLevelWarn)
	}
	fs.log = log
	fs.mmapEnabled = enabled
	if !enabled {
		fs.closeMmapLocked(ctx)
		return
	}
	fs.maybeMmapLocked(ctx)
}

func (fs *FileSystem) maybeMmapLocked(ctx context.Context) {
	if !fs.mmapEnabled || fs.file == nil || fs.mmap != nil {
		return
	}
	handle, err := callMapFile(fs.file)
	if err != nil {
		fs.ensureLogger()
		if isMmapUnsupported(err) {
			fs.log.debug(ctx, "sstable mmap unsupported for %s: %v", fs.filePath, err)
			return
		}
		fs.log.warn(ctx, "failed to mmap %s: %v", fs.filePath, err)
		return
	}
	if handle == nil || handle.Bytes() == nil {
		return
	}
	fs.mmap = handle
}

func (fs *FileSystem) closeMmapLocked(ctx context.Context) {
	if fs.mmap == nil {
		return
	}
	fs.ensureLogger()
	if err := fs.mmap.Close(); err != nil {
		fs.log.warn(ctx, "failed to close mmap for %s: %v", fs.filePath, err)
	}
	fs.mmap = nil
}

func (fs *FileSystem) ensureLogger() {
	if fs.log.Logger == nil {
		fs.log = newScopedLogger(nopLogger{}, LogLevelWarn)
	}
}

func (fs *FileSystem) mmapBytes(off int64, length int) []byte {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if fs.mmap == nil || off < 0 {
		return nil
	}
	data := fs.mmap.Bytes()
	if data == nil || off >= int64(len(data)) {
		return nil
	}
	start := int(off)
	if length <= 0 {
		return data[start:start]
	}
	if end := start + length; end > start && end <= len(data) {
		return data[start:end]
	}
	return data[start:]
}
