package rindb

import (
	"errors"
	"fmt"
	"io"
)

type WAL struct {
	*FileSystem
	tm     *TransactionManager
	config Config
}

func NewWAL(config Config, fs *FileSystem) WAL {
	return WAL{
		FileSystem: fs,
		tm:         NewTransactionManager(),
		config:     config,
	}
}

func (w *WAL) Load() (Memtable, error) {
	_, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		return Memtable{}, fmt.Errorf("failed to seek to start of WAL file %s: %w", w.Path(), err)
	}
	mem := InitMemtable(w.config)
	for {
		record, err := ReadRecord(w.file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // Normal end of file
			}
			return Memtable{}, fmt.Errorf("failed to read record from WAL %s: %w", w.Path(), err)
		}

		mem.Put(record)
	}
	return mem, nil
}

func (w *WAL) Append(record Record) error {
	tx := w.tm.Begin()
	defer tx.Rollback()

	_, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end of WAL file %s: %w", w.Path(), err)
	}

	if err = WriteRecord(tx, record); err != nil {
		return fmt.Errorf("failed to write record to WAL transaction: %w", err)
	}

	if err = tx.Commit(w.file); err != nil {
		return fmt.Errorf("failed to commit WAL transaction to %s: %w", w.Path(), err)
	}

	if err = w.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file %s: %w", w.Path(), err)
	}

	return nil
}

func (w *WAL) AppendMany(records []Record) error {
	tx := w.tm.Begin()
	defer tx.Rollback()

	_, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end of WAL file %s: %w", w.Path(), err)
	}

	for i, record := range records {
		if err := WriteRecord(tx, record); err != nil {
			return fmt.Errorf("failed to write record %d to WAL transaction: %w", i, err)
		}
	}

	if err = tx.Commit(w.file); err != nil {
		return fmt.Errorf("failed to commit multi-record WAL transaction to %s: %w", w.Path(), err)
	}

	if err = w.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file %s after multi-record append: %w", w.Path(), err)
	}

	return nil
}
