package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type wal struct {
	file *os.File
}

const walPermission = 0o600

func openWAL(path string) (*wal, error) {
	file, err := os.OpenFile(filepath.Clean(path), os.O_RDWR|os.O_CREATE, walPermission)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	return &wal{file: file}, nil
}

func newWAL(file *os.File) *wal {
	return &wal{file: file}
}

func (w *wal) path() string {
	return w.file.Name()
}

func (w *wal) sync() error {
	return w.file.Sync()
}

func (w *wal) close() error {
	return w.file.Close()
}

func (w *wal) clean() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	cleanWal, err := os.OpenFile(w.path(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, walPermission)
	if err != nil {
		return fmt.Errorf("failed to clean WAL file: %w", err)
	}

	w.file = cleanWal

	err = w.sync()
	if err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}

	return nil
}

func (w *wal) append(key, value []byte) error {
	_, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end of file: %w", err)
	}

	err = write(w.file, key, value)
	if err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	err = w.sync()
	if err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}
