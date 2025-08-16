package rindb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
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

var (
	walTracer             = otel.Tracer("rindb/wal")
	walMeter              = otel.Meter("rindb/wal")
	walLoadDuration       metric.Float64Histogram
	walAppendDuration     metric.Float64Histogram
	walAppendManyDuration metric.Float64Histogram
	walRecordsCounter     metric.Int64Counter
	walBytesCounter       metric.Int64Counter
)

func init() {
	walLoadDuration, _ = walMeter.Float64Histogram("rindb.wal.load.duration", metric.WithUnit("s"))
	walAppendDuration, _ = walMeter.Float64Histogram("rindb.wal.append.duration", metric.WithUnit("s"))
	walAppendManyDuration, _ = walMeter.Float64Histogram("rindb.wal.append_many.duration", metric.WithUnit("s"))
	walRecordsCounter, _ = walMeter.Int64Counter("rindb.wal.records")
	walBytesCounter, _ = walMeter.Int64Counter("rindb.wal.bytes")
}

func (w *WAL) Load(ctx context.Context) (Memtable, error) {
	ctx, span := walTracer.Start(ctx, "WAL.Load")
	start := time.Now()
	defer func() {
		walLoadDuration.Record(ctx, time.Since(start).Seconds())
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
		walAppendDuration.Record(ctx, time.Since(start).Seconds())
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
		walAppendManyDuration.Record(ctx, time.Since(start).Seconds())
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
