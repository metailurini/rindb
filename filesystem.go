package rindb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const fileSystemPermission = 0o600

var (
	_ io.ReadWriteCloser = (*FileSystem)(nil)

	ErrFileNotOpened = errors.New("file is not opened")
)

type FileSystem struct {
	filePath string
	file     *os.File
}

func OpenFS(filePath string) (*FileSystem, error) {
	fs := &FileSystem{filePath: filePath}
	if err := fs.Open(); err != nil {
		return nil, err
	}
	return fs, nil
}

func NewFS(file *os.File) *FileSystem {
	return &FileSystem{filePath: file.Name(), file: file}
}

func (fs *FileSystem) IsOpened() bool {
	return fs.file != nil
}

func (fs *FileSystem) Open() error {
	if fs.IsOpened() {
		WARN("File %s is already opened. Consider close and re-open again", fs.Path())
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
	if !fs.IsOpened() {
		return ErrFileNotOpened
	}
	return fs.file.Sync()
}

func (fs *FileSystem) Close() error {
	if !fs.IsOpened() {
		return ErrFileNotOpened
	}

	err := fs.file.Close()
	if err != nil {
		return err
	}
	fs.file = nil
	return nil
}

func (fs *FileSystem) Clean() error {
	if err := fs.Close(); err != nil {
		return fmt.Errorf("failed to close file %s before cleaning: %w", fs.Path(), err)
	}

	// Open with truncation
	cleanFile, err := os.OpenFile(fs.Path(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, fileSystemPermission)
	if err != nil {
		return fmt.Errorf("failed to open/truncate file %s for cleaning: %w", fs.Path(), err)
	}
	fs.file = cleanFile // Assign the new file handle

	if err := fs.Sync(); err != nil {
		// Close the newly opened file before returning error
		_ = fs.Close() // Ignore close error here as we're returning the sync error
		return fmt.Errorf("failed to sync file %s after cleaning: %w", fs.Path(), err)
	}

	return nil
}

// CursorPos get current cursor position in file system
func (fs *FileSystem) CursorPos() (int64, error) {
	if !fs.IsOpened() {
		return 0, ErrFileNotOpened
	}

	return fs.file.Seek(0, io.SeekCurrent)
}

// Rename to rename file system to a new name but
// keep access connection to that file during runtime
//
// Deprecated: no more purpose to use this function
func (fs *FileSystem) Rename(newPath string) error {
	if !fs.IsOpened() {
		return ErrFileNotOpened
	}

	// TODO: add lock
	if err := fs.file.Close(); err != nil {
		// If close fails, we probably shouldn't proceed with rename.
		return fmt.Errorf("failed to close file %s before renaming: %w", fs.Path(), err)
	}
	fs.file = nil // Mark as closed

	if err := os.Rename(fs.Path(), newPath); err != nil {
		return fmt.Errorf("failed to rename file from %s to %s: %w", fs.Path(), newPath, err)
	}

	// Open the newly named file
	newFile, err := os.OpenFile(filepath.Clean(newPath), os.O_RDWR, fileSystemPermission)
	if err != nil {
		// Rename succeeded, but opening the new path failed.
		return fmt.Errorf("failed to open renamed file %s: %w", newPath, err)
	}

	fs.file = newFile
	fs.filePath = newPath
	return nil
}

func (fs *FileSystem) Write(p []byte) (int, error) {
	if !fs.IsOpened() {
		return 0, ErrFileNotOpened
	}

	// TODO: add lock
	return fs.file.Write(p)
}

func (fs *FileSystem) Read(p []byte) (int, error) {
	if !fs.IsOpened() {
		return 0, ErrFileNotOpened
	}

	// TODO: add lock
	return fs.file.Read(p)
}
