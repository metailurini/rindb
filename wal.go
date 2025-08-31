package rindb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"sync/atomic"
)

type WAL struct {
	*FileSystem
	tm      *TransactionManager
	config  Config
	records atomic.Uint64
	bytes   atomic.Uint64
}

// DefaultNewWALFunc provides the default WAL initialization logic.
func DefaultNewWALFunc(ctx context.Context, cfg Config) (*WAL, error) {
	walPath := path.Join(cfg.databaseDir, "WAL")
	fs, err := OpenFS(ctx, walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file %s: %w", walPath, err)
	}
	return NewWAL(cfg, fs), nil
}

func NewWAL(config Config, fs *FileSystem) *WAL {
	return &WAL{
		FileSystem: fs,
		tm:         NewTransactionManager(),
		config:     config,
	}
}

func (w *WAL) Load(ctx context.Context) (Memtable, error) {
	ctx, span := walTracer.Start(ctx, "WAL.Load")
	start := time.Now()
	defer func() {
		walLoadDuration.Record(ctx, float64(time.Since(start).Milliseconds()))
		span.End()
	}()

	reader := newOffsetReader(w.FileSystem, 0)
	mem := InitMemtable(w.config)
	for {
		record, err := ReadRecord(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // Normal end of file
			}
			if errors.Is(err, ErrChecksumMismatch) {
				return Memtable{}, fmt.Errorf("checksum mismatch in WAL %s: %w", w.Path(), err)
			}
			return Memtable{}, fmt.Errorf("failed to read record from WAL %s: %w", w.Path(), err)
		}

		mem.Put(record)
		w.records.Add(1)
		w.bytes.Add(uint64(CalOnDiskSize(record)))
	}
	walRecordsCounter.Add(ctx, int64(w.records.Load()))
	walBytesCounter.Add(ctx, int64(w.bytes.Load()))
	return mem, nil
}

func (w *WAL) Append(ctx context.Context, record Record) error {
	ctx, span := walTracer.Start(ctx, "WAL.Append")
	start := time.Now()
	defer func() {
		walAppendDuration.Record(ctx, float64(time.Since(start).Milliseconds()))
		span.End()
	}()

	tx := w.tm.Begin()
	defer tx.Rollback(ctx)

	if err := func() error {
		w.FileSystem.mu.Lock()
		defer w.FileSystem.mu.Unlock()

		if w.FileSystem.file == nil {
			return ErrFileNotOpened
		}
		if _, err := w.FileSystem.file.Seek(0, io.SeekEnd); err != nil {
			return fmt.Errorf("failed to seek to end of WAL file %s: %w", w.Path(), err)
		}
		if err := WriteRecord(tx, record); err != nil {
			return fmt.Errorf("failed to write record to WAL transaction: %w", err)
		}
		if err := tx.Commit(ctx, w.FileSystem.file); err != nil {
			return fmt.Errorf("failed to commit WAL transaction to %s: %w", w.Path(), err)
		}
		return nil
	}(); err != nil {
		return err
	}

	if err := w.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file %s: %w", w.Path(), err)
	}

	walRecordsCounter.Add(ctx, 1)
	walBytesCounter.Add(ctx, int64(CalOnDiskSize(record)))
	w.records.Add(1)
	w.bytes.Add(uint64(CalOnDiskSize(record)))

	return nil
}

func (w *WAL) AppendMany(ctx context.Context, records []Record) error {
	ctx, span := walTracer.Start(ctx, "WAL.AppendMany")
	start := time.Now()
	defer func() {
		walAppendManyDuration.Record(ctx, float64(time.Since(start).Milliseconds()))
		span.End()
	}()

	tx := w.tm.Begin()
	defer tx.Rollback(ctx)

	var totalBytes int
	if err := func() error {
		w.FileSystem.mu.Lock()
		defer w.FileSystem.mu.Unlock()

		if w.FileSystem.file == nil {
			return ErrFileNotOpened
		}
		if _, err := w.FileSystem.file.Seek(0, io.SeekEnd); err != nil {
			return fmt.Errorf("failed to seek to end of WAL file %s: %w", w.Path(), err)
		}

		for i, record := range records {
			if err := WriteRecord(tx, record); err != nil {
				return fmt.Errorf("failed to write record %d to WAL transaction: %w", i, err)
			}
			totalBytes += CalOnDiskSize(record)
		}

		if err := tx.Commit(ctx, w.FileSystem.file); err != nil {
			return fmt.Errorf("failed to commit multi-record WAL transaction to %s: %w", w.Path(), err)
		}
		return nil
	}(); err != nil {
		return err
	}

	if err := w.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file %s after multi-record append: %w", w.Path(), err)
	}

	walRecordsCounter.Add(ctx, int64(len(records)))
	walBytesCounter.Add(ctx, int64(totalBytes))
	w.records.Add(uint64(len(records)))
	w.bytes.Add(uint64(totalBytes))

	return nil
}

// Clean removes WAL records with sequence numbers lower than minSeq.
//
// It rewrites the WAL preserving only the records with sequence numbers >= minSeq.
// The method resets internal counters based on the remaining records.
func (w *WAL) Clean(ctx context.Context, minSeq uint64) error {
	ctx, span := walTracer.Start(ctx, "WAL.Clean")
	defer span.End()

	reader := newOffsetReader(w.FileSystem, 0)

	dir := filepath.Dir(w.Path())
	tmp, err := os.CreateTemp(dir, "wal_clean_*")
	if err != nil {
		return fmt.Errorf("failed to create temp WAL file: %w", err)
	}
	tmpFS := NewFS(tmp)
	tmpPath := tmpFS.Path()

	var keptRecords uint64
	var keptBytes int64

	for {
		rec, err := ReadRecord(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			_ = tmpFS.Close()
			_ = os.Remove(tmpPath)
			if errors.Is(err, ErrChecksumMismatch) {
				return fmt.Errorf("checksum mismatch while reading WAL: %w", err)
			}
			return fmt.Errorf("failed to read WAL record: %w", err)
		}
		if rec.GetSequenceNumber() < minSeq {
			continue
		}
		tx := w.tm.Begin()
		if err := WriteRecord(tx, rec); err != nil {
			_ = tmpFS.Close()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("failed to write record to WAL transaction: %w", err)
		}
		if err := tx.Commit(ctx, tmpFS); err != nil {
			_ = tmpFS.Close()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("failed to commit WAL transaction: %w", err)
		}
		keptRecords++
		keptBytes += int64(CalOnDiskSize(rec))
	}

	if err := tmpFS.Sync(); err != nil {
		_ = tmpFS.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	if err := w.Close(); err != nil {
		_ = tmpFS.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to close WAL: %w", err)
	}
	if err := tmpFS.Rename(w.Path()); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to replace WAL: %w", err)
	}
	if err := w.Open(ctx); err != nil {
		return fmt.Errorf("failed to reopen WAL: %w", err)
	}

	prevRecords := w.records.Swap(keptRecords)
	prevBytes := w.bytes.Swap(uint64(keptBytes))

	walRecordsCounter.Add(ctx, int64(keptRecords)-int64(prevRecords))
	walBytesCounter.Add(ctx, keptBytes-int64(prevBytes))

	return nil
}
