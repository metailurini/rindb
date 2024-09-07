package rindb

import (
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
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
		return errors.Wrap(err, "failed to open file system: %w")
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
		return errors.Wrap(err, "failed to close file system: %w")
	}

	cleanFile, err := os.OpenFile(fs.Path(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, fileSystemPermission)
	if err != nil {
		return errors.Wrap(err, "failed to clean file system: %w")
	}

	fs.file = cleanFile

	if err := fs.Sync(); err != nil {
		return errors.Wrap(err, "failed to sync file system")
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
		return errors.Wrap(err, "failed to close file system: %w")
	}

	if err := os.Rename(fs.Path(), newPath); err != nil {
		return errors.Wrap(err, "failed to rename file system: %w")
	}

	newFile, err := os.OpenFile(filepath.Clean(newPath), os.O_RDWR, fileSystemPermission)
	if err != nil {
		return errors.Wrap(err, "failed to open file system: %w")
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
