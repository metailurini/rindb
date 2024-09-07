package rindb

import (
	"bytes"
	"io"

	"github.com/pkg/errors"
)

type WAL struct{ *FileSystem }

func NewWAL(fs *FileSystem) WAL {
	return WAL{fs}
}

func (w *WAL) Load() (Memtable, error) {
	_, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		return Memtable{}, errors.Wrap(err, "failed to seek to start of file: %w")
	}
	mem := InitMemtable()
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
	_, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.Wrap(err, "failed to seek to end of file: %w")
	}

	err = WriteRecord(w, record)
	if err != nil {
		return errors.Wrap(err, "failed to write to file: %w")
	}

	err = w.Sync()
	if err != nil {
		return errors.Wrap(err, "failed to sync file: %w")
	}

	return nil
}

func (w *WAL) AppendMany(records []Record) error {
	_, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.Wrap(err, "failed to seek to end of file: %w")
	}

	// write to string buffer and write back to file
	// to make sure that all data must be persistent
	txBuf := bytes.NewBufferString("")
	for _, record := range records {
		err := WriteRecord(txBuf, record)
		if err != nil {
			return errors.Wrap(err, "failed to write to buffer: %w")
		}
	}

	_, err = w.Write(txBuf.Bytes())
	if err != nil {
		return errors.Wrap(err, "failed to write to file: %w")
	}

	err = w.Sync()
	if err != nil {
		return errors.Wrap(err, "failed to sync file: %w")
	}

	return nil
}
