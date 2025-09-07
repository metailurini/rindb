package rindb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"sync/atomic"
)

func cleanupTemp(fs *FileSystem, p string) {
	_ = fs.Close()
	_ = os.Remove(p)
}

type wal struct {
	*FileSystem
	tm      *transactionManager
	config  Config
	records atomic.Uint64
	bytes   atomic.Uint64

	// injected for testing
	writeRecord func(tx *transaction, rec Record) error
	txCommit    func(tx *transaction, ctx context.Context) error
}

// DefaultNewWALFunc provides the default WAL initialization logic.
func DefaultNewWALFunc(ctx context.Context, cfg Config) (*wal, error) {
	// Attempt to reuse the highest-numbered WAL if it exists.
	entries, err := os.ReadDir(cfg.databaseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read database directory %s: %w", cfg.databaseDir, err)
	}

	var maxID uint64
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, walExt) {
			if n, nerr := fileNum(name); nerr == nil && n > maxID {
				maxID = n
			}
		}
	}
	id := maxID
	if id == 0 {
		id = cfg.fileNumberAllocator.nextNumber()
	}
	wp := path.Join(cfg.databaseDir, walPath(id))
	fs, err := OpenFS(ctx, wp)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file %s: %w", wp, err)
	}
	return NewWAL(cfg, fs), nil
}

func NewWAL(config Config, fs *FileSystem) *wal {
	return &wal{
		FileSystem:  fs,
		tm:          newTransactionManager(),
		config:      config,
		writeRecord: writeRecord,
		txCommit: func(tx *transaction, ctx context.Context) error {
			return tx.commit(ctx)
		},
	}
}

func (w *wal) Load(ctx context.Context) (memtable, error) {
	ctx, span := walTracer.Start(ctx, "wal.Load")
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
				return memtable{}, fmt.Errorf("checksum mismatch in WAL %s: %w", w.Path(), err)
			}
			return memtable{}, fmt.Errorf("failed to read record from WAL %s: %w", w.Path(), err)
		}

		mem.Put(record)
		w.records.Add(1)
		w.bytes.Add(uint64(CalOnDiskSize(record)))
	}
	walRecordsCounter.Add(ctx, int64(w.records.Load()))
	walBytesCounter.Add(ctx, int64(w.bytes.Load()))
	return mem, nil
}

func (w *wal) Append(ctx context.Context, record Record) error {
	ctx, span := walTracer.Start(ctx, "wal.Append")
	start := time.Now()
	defer func() {
		walAppendDuration.Record(ctx, float64(time.Since(start).Milliseconds()))
		span.End()
	}()

	tx, err := w.tm.begin(w.FileSystem)
	if err != nil {
		return err
	}
	defer tx.rollback(ctx)

	if err := w.writeRecord(tx, record); err != nil {
		return fmt.Errorf("failed to write record to WAL transaction: %w", err)
	}
	if err := w.txCommit(tx, ctx); err != nil {
		return fmt.Errorf("failed to commit WAL transaction to %s: %w", w.Path(), err)
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

func (w *wal) AppendMany(ctx context.Context, records []Record) error {
	ctx, span := walTracer.Start(ctx, "wal.AppendMany")
	start := time.Now()
	defer func() {
		walAppendManyDuration.Record(ctx, float64(time.Since(start).Milliseconds()))
		span.End()
	}()

	tx, err := w.tm.begin(w.FileSystem)
	if err != nil {
		return err
	}
	defer tx.rollback(ctx)

	var totalBytes int
	for i, record := range records {
		if err := w.writeRecord(tx, record); err != nil {
			return fmt.Errorf("failed to write record %d to WAL transaction: %w", i, err)
		}
		totalBytes += CalOnDiskSize(record)
	}

	if err := w.txCommit(tx, ctx); err != nil {
		return fmt.Errorf("failed to commit multi-record WAL transaction to %s: %w", w.Path(), err)
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
func (w *wal) Clean(ctx context.Context, minSeq uint64) error {
	ctx, span := walTracer.Start(ctx, "wal.Clean")
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
			cleanupTemp(tmpFS, tmpPath)
			if errors.Is(err, ErrChecksumMismatch) {
				return fmt.Errorf("checksum mismatch while reading WAL: %w", err)
			}
			return fmt.Errorf("failed to read WAL record: %w", err)
		}
		if rec.GetSequenceNumber() < minSeq {
			continue
		}
		tx, err := w.tm.begin(tmpFS)
		if err != nil {
			cleanupTemp(tmpFS, tmpPath)
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		if err := w.writeRecord(tx, rec); err != nil {
			cleanupTemp(tmpFS, tmpPath)
			return fmt.Errorf("failed to write record to WAL transaction: %w", err)
		}
		if err := w.txCommit(tx, ctx); err != nil {
			cleanupTemp(tmpFS, tmpPath)
			return fmt.Errorf("failed to commit WAL transaction: %w", err)
		}
		keptRecords++
		keptBytes += int64(CalOnDiskSize(rec))
	}

	if err := tmpFS.Sync(); err != nil {
		cleanupTemp(tmpFS, tmpPath)
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	if err := w.Close(); err != nil {
		cleanupTemp(tmpFS, tmpPath)
		return fmt.Errorf("failed to close WAL: %w", err)
	}
	if err := tmpFS.Rename(w.Path()); err != nil {
		cleanupTemp(tmpFS, tmpPath)
		return fmt.Errorf("failed to replace WAL: %w", err)
	}
	if err := tmpFS.Close(); err != nil {
		return fmt.Errorf("failed to close temp WAL: %w", err)
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
