package rindb

import (
	"io"

	"github.com/pkg/errors"
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
		return Memtable{}, errors.Wrap(err, "failed to seek to start of file: %w")
	}
	mem := InitMemtable(w.config)
	ce := 0
	for {
		record, err := ReadRecord(w.file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return Memtable{}, err
		}

		ce += CalOnDiskSize(record)
		mem.Put(record.GetKey(), record.GetValue())
	}
	return mem, nil
}

func (w *WAL) Append(record Record) error {
	tx := w.tm.Begin()
	defer tx.Rollback()

	_, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.Wrap(err, "failed to seek to end of file")
	}

	err = WriteRecord(tx, record)
	if err != nil {
		return errors.Wrap(err, "failed to write record")
	}

	err = tx.Commit(w.file)
	if err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	err = w.Sync()
	if err != nil {
		return errors.Wrap(err, "failed to sync file")
	}

	return nil
}

func (w *WAL) AppendMany(records []Record) error {
	tx := w.tm.Begin()
	defer tx.Rollback()

	_, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.Wrap(err, "failed to seek to end of file")
	}

	for _, record := range records {
		err := WriteRecord(tx, record)
		if err != nil {
			return errors.Wrap(err, "failed to write record")
		}
	}

	err = tx.Commit(w.file)
	if err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	err = w.Sync()
	if err != nil {
		return errors.Wrap(err, "failed to sync file")
	}

	return nil
}
