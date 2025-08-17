package rindb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"time"
)

type WAL struct {
	*FileSystem
	tm     *TransactionManager
	config Config
}

// DefaultNewWALFunc provides the default WAL initialization logic.
func DefaultNewWALFunc(ctx context.Context, cfg Config) (WAL, error) {
	walPath := path.Join(cfg.databaseDir, "WAL")
	fs, err := OpenFS(ctx, walPath)
	if err != nil {
		return WAL{}, fmt.Errorf("failed to open WAL file %s: %w", walPath, err)
	}
	return NewWAL(cfg, fs), nil
}

func NewWAL(config Config, fs *FileSystem) WAL {
	return WAL{
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

func (w *WAL) Append(ctx context.Context, record Record) error {
	ctx, span := walTracer.Start(ctx, "WAL.Append")
	start := time.Now()
	defer func() {
		walAppendDuration.Record(ctx, float64(time.Since(start).Milliseconds()))
		span.End()
	}()

	tx := w.tm.Begin()
	defer tx.Rollback(ctx)

	_, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end of WAL file %s: %w", w.Path(), err)
	}

	if err = WriteRecord(tx, record); err != nil {
		return fmt.Errorf("failed to write record to WAL transaction: %w", err)
	}

	if err = tx.Commit(ctx, w.file); err != nil {
		return fmt.Errorf("failed to commit WAL transaction to %s: %w", w.Path(), err)
	}

	if err = w.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file %s: %w", w.Path(), err)
	}

	walRecordsCounter.Add(ctx, 1)
	walBytesCounter.Add(ctx, int64(CalOnDiskSize(record)))

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

	_, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end of WAL file %s: %w", w.Path(), err)
	}

	var totalBytes int
	for i, record := range records {
		if err := WriteRecord(tx, record); err != nil {
			return fmt.Errorf("failed to write record %d to WAL transaction: %w", i, err)
		}
		totalBytes += CalOnDiskSize(record)
	}

	if err = tx.Commit(ctx, w.file); err != nil {
		return fmt.Errorf("failed to commit multi-record WAL transaction to %s: %w", w.Path(), err)
	}

	if err = w.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file %s after multi-record append: %w", w.Path(), err)
	}

	walRecordsCounter.Add(ctx, int64(len(records)))
	walBytesCounter.Add(ctx, int64(totalBytes))

	return nil
}
