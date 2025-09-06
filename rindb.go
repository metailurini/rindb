// Package rindb is key-value database
package rindb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ErrDatabaseClosed is returned when an operation is attempted on a closed database.
var ErrDatabaseClosed = errors.New("database is closed")

// Rindb is the main database structure
type Rindb struct {
	WAL               *wal
	Memtable          memtable
	SSTableManager    *ssTableManager
	versionSet        *versionSet
	manifest          manifestWriter
	config            Config
	shutdownTelemetry func(context.Context) error
	mu                sync.RWMutex   // Mutex for thread-safe access
	wg                sync.WaitGroup // WaitGroup to track background goroutines
	closed            bool           // Flag to indicate if the database is closed
	sequenceNumber    uint64
	activeSnapshots   []uint64 // Sorted list of active snapshot sequences

	getCalls    atomic.Uint64
	putCalls    atomic.Uint64
	removeCalls atomic.Uint64
	iRangeCalls atomic.Uint64
	flushCount  atomic.Uint64
}

// Stats represents runtime statistics of the database.
//
// It includes information about memtable size, sequence number,
// active snapshot count, per-level SSTable counts, WAL usage and operation counters.
type Stats struct {
	MemtableBytes    int
	SequenceNumber   uint64
	ActiveSnapshots  int
	SSTablesPerLevel []int
	WALBytes         uint64
	WALRecords       uint64
	GetCalls         uint64
	PutCalls         uint64
	RemoveCalls      uint64
	IRangeCalls      uint64
	Flushes          uint64
}

// InitRinDB initializes a new RinDB instance with provided configuration options.
// Parameters:
//
//	opts... - Configuration options to customize database behavior
//
// Returns:
//
//	*Rindb - Initialized database instance
//	error  - Any initialization error encountered
//
// Errors:
//   - Filesystem errors during directory creation
//   - WAL initialization failures
//   - SSTable manager startup failures
//
// Initialization sequence:
// 1. Create database directory structure
// 2. Initialize Write-Ahead Log (WAL)
// 3. Load existing memtable from WAL
// 4. Initialize SSTable storage manager
func InitRinDB(ctx context.Context, opts ...Option) (_ *Rindb, err error) {
	cfg := NewConfig(opts...)

	shutdownTelemetry, err := OtelInit(ctx, cfg.enableTelemetry, cfg.exporterEndpoint, cfg.exporterInsecure, cfg.telemetrySamplingRate)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize telemetry: %w", err)
	}

	// Create database directory with secure permissions (0750 = owner RWX, group RX, others none)
	if err := os.MkdirAll(cfg.databaseDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create database directory %s: %w", cfg.databaseDir, err)
	}

	vs, manifestPath, err := recoverVersionSet(ctx, cfg.databaseDir, cfg.fileNumberAllocator)
	if err != nil {
		return nil, err
	}
	if manifestPath == "" {
		manifestFile := DefaultManifestFile
		manifestPath = path.Join(cfg.databaseDir, manifestFile)
		if err := writeCurrent(ctx, cfg.databaseDir, manifestFile); err != nil {
			return nil, err
		}
	}
	mw, err := cfg.newManifestWriterFunc(ctx, manifestPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = mw.Close()
		}
	}()
	if vs.NextFileNumber == 0 {
		vs.NextFileNumber = 1
	}

	wal, err := cfg.newWALFunc(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			// Close WAL if there's an error after this point while opening database
			_ = wal.Close()
		}
	}()

	// Determine the maximum sequence number by comparing the manifest's
	// LastSequence with the WAL's highest sequence.
	maxSeqNum, memtable, err := getMaxSequenceNumber(ctx, vs, wal)
	if err != nil {
		return nil, err
	}

	ssTableManager, err := cfg.newSSTableManagerFunc(ctx, cfg, vs, mw)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize SSTable manager: %w", err)
	}

	info(ctx, "Initialized RinDB with database directory %s", cfg.databaseDir)
	rin := &Rindb{
		WAL:               wal,
		Memtable:          memtable,
		SSTableManager:    ssTableManager,
		versionSet:        vs,
		manifest:          mw,
		config:            cfg,
		shutdownTelemetry: shutdownTelemetry,
		sequenceNumber:    maxSeqNum,
	}
	if err := rin.maybeRotateManifest(ctx); err != nil {
		return nil, err
	}
	return rin, nil
}

// Stats returns current statistics of the database.
func (r *Rindb) Stats() Stats {
	// Read atomic stats without locking first.
	stats := Stats{
		WALBytes:    r.WAL.bytes.Load(),
		WALRecords:  r.WAL.records.Load(),
		GetCalls:    r.getCalls.Load(),
		PutCalls:    r.putCalls.Load(),
		RemoveCalls: r.removeCalls.Load(),
		IRangeCalls: r.iRangeCalls.Load(),
		Flushes:     r.flushCount.Load(),
	}

	// Lock to get memtable stats, sequence number and snapshot count.
	r.mu.RLock()
	stats.MemtableBytes = r.Memtable.ByteSize()
	stats.SequenceNumber = r.sequenceNumber
	stats.ActiveSnapshots = len(r.activeSnapshots)
	r.mu.RUnlock()

	// Lock separately for SSTable manager stats.
	r.SSTableManager.mu.RLock()
	vs := r.SSTableManager.versionSet
	if vs != nil {
		stats.SSTablesPerLevel = make([]int, len(vs.Levels))
		for i, files := range vs.Levels {
			stats.SSTablesPerLevel[i] = len(files)
		}
	}
	r.SSTableManager.mu.RUnlock()

	return stats
}

func (r *Rindb) TableCacheStats() tableCacheStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.SSTableManager == nil {
		return tableCacheStats{}
	}
	return r.SSTableManager.TableCacheStats()
}

// Get retrieves the value associated with the given key from the database.
// It first checks the memtable and then the SSTables if the key is not found in the memtable.
// Parameters:
//
//	key - The key to search for.
//
// Returns:
//
//	Bytes - The value associated with the key, or nil if the key is not found.
//	error - An error if the database is closed, or if an error occurs during lookup in memtable or SSTables.
func (r *Rindb) Get(ctx context.Context, key Bytes, seq ...uint64) (Bytes, error) {
	ctx, span := tracer.Start(ctx, "Rindb.Get")
	defer span.End()
	getCalls.Add(ctx, 1)
	r.getCalls.Add(1)
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("key_size", len(key)))
	}

	maxSeq := getMaxSeq(seq...)

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, ErrDatabaseClosed
	}

	value, err := r.Memtable.GetAt(key, maxSeq)
	if err == nil {
		return value, nil
	}
	if !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}
	return r.SSTableManager.SearchKey(ctx, key, maxSeq)
}

// IRange returns an iterator over records with keys in [start, end],
// merged across the memtable and relevant SSTables.
//
// The returned iterator must be closed when no longer needed to release
// any associated resources.
func (r *Rindb) IRange(ctx context.Context, start, end Bytes, seq ...uint64) (*RangeIterator, error) {
	ctx, span := tracer.Start(ctx, "Rindb.IRange")
	defer span.End()
	iRangeCalls.Add(ctx, 1)
	r.iRangeCalls.Add(1)
	if span.IsRecording() {
		span.SetAttributes(
			attribute.Int("start_key_size", len(start)),
			attribute.Int("end_key_size", len(end)),
		)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, ErrDatabaseClosed
	}

	maxSeq := getMaxSeq(seq...)

	iterators := []Iterator[Record]{r.Memtable.IRange(start, end, maxSeq)}

	ssts, err := r.SSTableManager.GetRelevantSSTables(ctx, start, end)
	if err != nil {
		return nil, err
	}
	opened := ssts

	cleanupOpened := func() {
		for _, o := range opened {
			o.Release()
		}
	}

	for _, hnd := range ssts {
		rangeIter, err := hnd.Table.IRange(start, end, maxSeq)
		if err != nil {
			cleanupOpened()
			return nil, err
		}
		iterators = append(iterators, rangeIter)
	}

	mergeIter, err := NewMergingIterator(iterators, cleanupOpened)
	if err != nil {
		cleanupOpened()
		return nil, err
	}

	return NewRangeIterator(mergeIter), nil
}

// Put inserts or updates a key-value pair in the database.
// The operation is first written to the Write-Ahead Log (WAL) and then applied to the memtable.
// If the memtable size exceeds the configured threshold, it triggers a flush to an SSTable and WAL cleaning.
// Parameters:
//
//	key - The key to insert or update.
//	value - The value to associate with the key.
//
// Returns:
//
//	error - An error if the database is closed, or if an error occurs during WAL append, memtable update,
//	        flushing, SSTable registration, or WAL cleaning.
func (r *Rindb) Put(ctx context.Context, key, value Bytes) error {
	ctx, span := tracer.Start(ctx, "Rindb.Put")
	defer span.End()
	putCalls.Add(ctx, 1)
	r.putCalls.Add(1)
	if span.IsRecording() {
		span.SetAttributes(
			attribute.Int("key_size", len(key)),
			attribute.Int("value_size", len(value)),
		)
	}

	if err := func() error {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.closed {
			return ErrDatabaseClosed
		}
		r.sequenceNumber++
		record := RecordImpl{Key: key, Value: value, SequenceNumber: r.sequenceNumber, Type: TypeValue}
		if err := r.WAL.Append(ctx, record); err != nil {
			return err
		}
		r.Memtable.Put(record)
		memSize := r.Memtable.ByteSize()
		if uint(memSize) >= r.config.maxMemtableSize {
			flushCount.Add(ctx, 1)
			r.flushCount.Add(1)
			info(ctx, "Memtable estimated size %d reached threshold %d, flushing.", memSize, r.config.maxMemtableSize)

			fs, err := r.SSTableManager.newSSTableFS(ctx)
			if err != nil {
				errorf(ctx, "Failed to create new SSTable file system: %v", err)
				return fmt.Errorf("failed to create new SSTable file system: %w", err)
			}

			_, meta, err := flush(ctx, r.config, r.Memtable, fs)
			if err != nil {
				_ = fs.Close()
				errorf(ctx, "Failed to flush memtable: %v", err)
				return fmt.Errorf("failed to flush memtable: %w", err)
			}

			if err := r.SSTableManager.addSSTable(ctx, meta, r.sequenceNumber); err != nil {
				_ = fs.Close()
				errorf(ctx, "Failed to register new SSTable %s: %v", fs.Path(), err)
				return fmt.Errorf("failed to register new SSTable %s: %w", fs.Path(), err)
			}
			if err := fs.Close(); err != nil {
				warn(ctx, "Failed to close FileSystem %s: %v", fs.Path(), err)
			}

			r.Memtable.Clear()
			snapMin := r.minSnapshotSeq()
			walThreshold := snapMin
			if len(r.activeSnapshots) == 0 {
				walThreshold = r.sequenceNumber + 1
			}

			if err := r.WAL.Clean(ctx, walThreshold); err != nil {
				errorf(ctx, "Failed to clean WAL after memtable flush: %v", err)
				return fmt.Errorf("failed to clean WAL: %w", err)
			}

			r.SSTableManager.setMinSnapshotSeq(snapMin)
			flushSeq := r.sequenceNumber
			info(ctx, "Triggering background compaction check.")
			r.wg.Add(1)
			compactionCtx := trace.ContextWithSpan(context.Background(), trace.SpanFromContext(ctx))

			go func(ctx context.Context, flushSeq uint64) {
				defer r.wg.Done()
				info(ctx, "Background compaction goroutine started.")
				if err := r.SSTableManager.Compact(ctx); err != nil {
					errorf(ctx, "Background compaction failed: %v", err)
				} else {
					info(ctx, "Background compaction goroutine finished.")
				}

				r.mu.Lock()
				defer r.mu.Unlock()

				if err := r.maybeRotateManifest(ctx); err != nil {
					errorf(ctx, "Manifest rotation failed: %v", err)
				}

				if err := r.cleanupObsoleteLocked(ctx, flushSeq); err != nil {
					errorf(ctx, "Post-compaction cleanup failed: %v", err)
				}
			}(compactionCtx, flushSeq)
		}
		return nil
	}(); err != nil {
		return err
	}
	r.SSTableManager.recordWrite()
	return nil
}

// Remove deletes a key-value pair from the database by writing a tombstone record.
// The deletion is first written to the Write-Ahead Log (WAL) and then applied to the memtable.
// Parameters:
//
//	key - The key to remove.
//
// Returns:
//
//	error - An error if the database is closed, or if an error occurs during WAL append or memtable update.
func (r *Rindb) Remove(ctx context.Context, key Bytes) error {
	ctx, span := tracer.Start(ctx, "Rindb.Remove")
	defer span.End()
	removeCalls.Add(ctx, 1)
	r.removeCalls.Add(1)
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("key_size", len(key)))
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrDatabaseClosed
	}

	r.sequenceNumber++
	record := RecordImpl{Key: key, Value: nil, SequenceNumber: r.sequenceNumber, Type: TypeDeletion}
	if err := r.WAL.Append(ctx, record); err != nil {
		return err
	}
	r.Memtable.Put(record)
	return nil
}

// Close gracefully shuts down the database instance with proper resource cleanup.
// Sequence:
// 1. Prevent new operations by marking as closed
// 2. Wait for background compaction to complete
// 3. Close WAL and SSTableManager resources
// 4. Log final shutdown status
// Safety: Idempotent - multiple calls will return ErrDatabaseClosed
func (r *Rindb) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ctx, span := tracer.Start(ctx, "Rindb.Close")
	defer span.End()

	r.mu.Lock()
	// Check if already closed
	if r.closed {
		r.mu.Unlock()
		return ErrDatabaseClosed // Return specific error if already closed
	}

	// Mark as closing immediately to prevent new operations
	r.closed = true
	r.mu.Unlock() // Unlock while waiting for goroutines

	// Wait for any background operations (like compaction) to complete
	info(ctx, "Waiting for background operations to finish...")
	waitStart := time.Now()
	r.wg.Wait()
	closeBackgroundWaitDuration.Record(ctx, float64(time.Since(waitStart).Milliseconds()))
	info(ctx, "Background operations finished.")

	// Re-acquire lock to safely close resources
	r.mu.Lock()
	defer r.mu.Unlock()

	// Close the WAL
	if err := r.WAL.Close(); err != nil {
		// Log the error but attempt to close SSTableManager anyway
		errorf(ctx, "Error closing WAL: %v", err)
		// Optionally return the WAL error immediately, or collect errors
		// return fmt.Errorf("error closing WAL: %w", err)
	} else {
		info(ctx, "WAL closed successfully.")
	}

	// Close the SSTableManager
	// Assuming SSTableManager.Close() handles potential errors internally or returns them
	r.SSTableManager.Close(ctx) // SSTableManager.Close currently doesn't return an error
	info(ctx, "SSTableManager closed.")

	if r.manifest != nil {
		if err := r.manifest.Close(); err != nil {
			errorf(ctx, "Error closing manifest: %v", err)
		}
	}

	if err := r.shutdownTelemetry(ctx); err != nil {
		errorf(ctx, "Error shutting down telemetry: %v", err)
	} else {
		info(ctx, "Telemetry shutdown completed.")
	}

	info(ctx, "RinDB closed successfully")
	return nil
}
