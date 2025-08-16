// Package rindb is key-value database
package rindb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// ErrDatabaseClosed is returned when an operation is attempted on a closed database.
var ErrDatabaseClosed = errors.New("database is closed")

var tracer = otel.Tracer("rindb")

// Rindb is the main database structure
type Rindb struct {
	wal            WAL
	memtable       Memtable
	ssTableManager *SSTableManager
	config         Config
	mu             sync.RWMutex   // Mutex for thread-safe access
	wg             sync.WaitGroup // WaitGroup to track background goroutines
	closed         bool           // Flag to indicate if the database is closed
	sequenceNumber uint64
}

// InitRinDB initializes a new RinDB instance with provided configuration options.
// Parameters:
//
//	opts... - Configuration options to customize database behavior
//
// Returns:
//
//	Rindb - Initialized database instance
//	error - Any initialization error encountered
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
func InitRinDB(ctx context.Context, opts ...Option) (Rindb, error) {
	cfg := NewConfig(opts...)

	// Create database directory with secure permissions (0750 = owner RWX, group RX, others none)
	if err := os.MkdirAll(cfg.databaseDir, 0750); err != nil {
		return Rindb{}, fmt.Errorf("failed to create database directory %s: %w", cfg.databaseDir, err)
	}

	walPath := path.Join(cfg.databaseDir, "WAL")
	fs, err := OpenFS(ctx, walPath)
	if err != nil {
		return Rindb{}, fmt.Errorf("failed to open WAL file %s: %w", walPath, err)
	}
	wal := NewWAL(cfg, fs)
	memtable, err := wal.Load()
	if err != nil {
		return Rindb{}, err
	}

	// Set default is 0, while inserting new record, it will automatically increase
	// So first record's sequence number is always 1 if database is empty
	var maxSeqNum uint64 = 0

	memMaxSeqNum, err := getMaxSequenceNumberFromMemtable(memtable)
	if err != nil {
		return Rindb{}, err
	}
	maxSeqNum = max(maxSeqNum, memMaxSeqNum)

	ssTableManager, err := InitSSTableManager(ctx, cfg)
	if err != nil {
		// Consider closing the WAL file system if manager init fails
		_ = fs.Close()
		return Rindb{}, fmt.Errorf("failed to initialize SSTable manager: %w", err)
	}

	// Only scan L0 SSTables for max sequence number during initialization.
	// L0 SSTables contain the most recent data after the memtable.
	sstMaxSeqNum, err := getMaxSequenceNumberFromSSTables(ctx, ssTableManager)
	if err != nil {
		return Rindb{}, err
	}
	maxSeqNum = max(maxSeqNum, sstMaxSeqNum)

	INFO(ctx, "Initialized RinDB with database directory %s", cfg.databaseDir)
	return Rindb{
		wal:            wal,
		memtable:       memtable,
		ssTableManager: ssTableManager,
		config:         cfg,
		sequenceNumber: maxSeqNum,
	}, nil
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
func (r *Rindb) Get(ctx context.Context, key Bytes) (Bytes, error) {
	ctx, span := tracer.Start(ctx, "db.get")
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("key_size", len(key)))
	}
	defer span.End()

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, ErrDatabaseClosed
	}

	value, err := r.memtable.Get(key)
	if err == nil {
		return value, nil
	}
	if !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}
	return r.ssTableManager.searchKey(ctx, key)
}

// IRange returns an iterator over records with keys in [start, end],
// merged across the memtable and relevant SSTables.
func (r *Rindb) IRange(ctx context.Context, start, end Bytes) (Iterator[Record], error) {
	ctx, span := tracer.Start(ctx, "db.irange")
	if span.IsRecording() {
		span.SetAttributes(
			attribute.Int("start_key_size", len(start)),
			attribute.Int("end_key_size", len(end)),
		)
	}
	defer span.End()

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, ErrDatabaseClosed
	}

	iterators := []Iterator[Record]{r.memtable.IRange(start, end)}

	sstables, err := r.ssTableManager.GetRelevantSSTables(ctx, start, end)
	if err != nil {
		return nil, err
	}

	cleanup := func() {
		it := sstables.Iterator()
		for it.HasNext() {
			sst, _ := it.Next()
			_ = sst.Close()
		}
	}

	it := sstables.Iterator()
	for it.HasNext() {
		sst, _ := it.Next()
		rangeIter, err := sst.IRange(start, end)
		if err != nil {
			cleanup()
			return nil, err
		}
		iterators = append(iterators, rangeIter)
	}

	pq := buildRangePQ(iterators)

	return &mergedIRange{pq: pq, cleanup: cleanup}, nil
}

type pqItem struct {
	rec  Record
	iter Iterator[Record]
}

func buildRangePQ(iterators []Iterator[Record]) *PriorityQueue[pqItem] {
	less := func(a, b pqItem) bool {
		cmp := a.rec.GetKey().Compare(b.rec.GetKey())
		if cmp == CmpEqual {
			return a.rec.GetSequenceNumber() > b.rec.GetSequenceNumber()
		}
		return cmp == CmpLess
	}

	pq := NewPriorityQueue[pqItem](less)
	for _, it := range iterators {
		if it.HasNext() {
			rec, err := it.Next()
			if err == nil {
				pq.PushItem(pqItem{rec: rec, iter: it})
			}
		}
	}
	return pq
}

type mergedIRange struct {
	pq         *PriorityQueue[pqItem]
	lastKey    Bytes
	lastKeySet bool
	next       Record
	prepared   bool
	cleanup    func()
	err        error
}

func (m *mergedIRange) prepare() {
	for !m.prepared && m.err == nil && m.pq.Len() > 0 {
		item := m.pq.PopItem()
		key := item.rec.GetKey()

		if !m.lastKeySet || key.Compare(m.lastKey) != CmpEqual {
			if len(item.rec.GetValue()) > 0 {
				m.next = item.rec
				m.prepared = true
			}
			m.lastKey = key
			m.lastKeySet = true
		}

		if item.iter.HasNext() {
			rec, err := item.iter.Next()
			if err != nil {
				if !errors.Is(err, EOI) {
					m.err = err
				}
			} else {
				m.pq.PushItem(pqItem{rec: rec, iter: item.iter})
			}
		}
	}
	if m.err != nil || (!m.prepared && m.pq.Len() == 0) {
		_ = m.Close()
	}
}

// HasNext implements Iterator[Record].
func (m *mergedIRange) HasNext() bool {
	m.prepare()
	return m.prepared
}

// Next implements Iterator[Record].
func (m *mergedIRange) Next() (Record, error) {
	if !m.HasNext() {
		var empty Record
		if m.err != nil {
			return empty, m.err
		}
		return empty, EOI
	}
	m.prepared = false
	return m.next, nil
}

// Close releases any resources held by the iterator. It is safe to call multiple times.
func (m *mergedIRange) Close() error {
	if m.cleanup != nil {
		m.cleanup()
		m.cleanup = nil
	}
	return m.err
}

// CloseIterator calls Close on iter if it implements it.
func CloseIterator[T any](iter Iterator[T]) error {
	if c, ok := any(iter).(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
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
	ctx, span := tracer.Start(ctx, "db.put")
	if span.IsRecording() {
		span.SetAttributes(
			attribute.Int("key_size", len(key)),
			attribute.Int("value_size", len(value)),
		)
	}
	defer span.End()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrDatabaseClosed
	}

	r.sequenceNumber++
	record := RecordImpl{Key: key, Value: value, SequenceNumber: r.sequenceNumber}
	if err := r.wal.Append(ctx, record); err != nil {
		return err
	}
	r.memtable.Put(record) // This now updates the internal size estimate

	memSize := r.memtable.ByteSize()
	flushCount := 0
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("memtable_size", int(memSize)))
	}

	// Check estimated byte size and flush if needed
	// Cast ByteSize() to uint to match maxMemtableSize type
	// Check estimated byte size and flush if needed
	if uint(memSize) >= r.config.maxMemtableSize {
		flushCount = 1
		INFO(ctx, "Memtable estimated size %d reached threshold %d, flushing.", memSize, r.config.maxMemtableSize)

		// Create new SSTable file system for level 0
		fs, err := r.ssTableManager.NewSSTableFS(ctx, 0)
		if err != nil {
			ERROR(ctx, "Failed to create new SSTable file system: %v", err)
			return fmt.Errorf("failed to create new SSTable file system: %w", err)
		}

		_, err = Flush(ctx, r.config, r.memtable, fs)
		if err != nil {
			_ = fs.Close() // Attempt to close FS on flush error
			ERROR(ctx, "Failed to flush memtable: %v", err)
			return fmt.Errorf("failed to flush memtable: %w", err)
		}

		// Register the new SSTable with ssTableManager
		if err := r.ssTableManager.AddSSTable(ctx, 0, fs); err != nil {
			ERROR(ctx, "Failed to register new SSTable %s: %v", fs.Path(), err)
			return fmt.Errorf("failed to register new SSTable %s: %w", fs.Path(), err)
		}

		// Clear the memtable and clean the WAL *after* successful flush and registration
		r.memtable.Clear()
		if err := r.wal.Clean(); err != nil {
			ERROR(ctx, "Failed to clean WAL after memtable flush: %v", err)
			return fmt.Errorf("failed to clean WAL: %w", err)
		}

		// Trigger compaction in a goroutine *after* flushing
		INFO(ctx, "Triggering background compaction check.")
		r.wg.Add(1)
		go func(ctx context.Context) {
			defer r.wg.Done()
			INFO(ctx, "Background compaction goroutine started.")
			if err := r.ssTableManager.Compact(ctx); err != nil {
				ERROR(ctx, "Background compaction failed: %v", err)
			} else {
				INFO(ctx, "Background compaction goroutine finished.")
			}
		}(ctx)
	}

	if span.IsRecording() {
		span.SetAttributes(attribute.Int("flush_count", flushCount))
	}

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
	ctx, span := tracer.Start(ctx, "db.remove")
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("key_size", len(key)))
	}
	defer span.End()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrDatabaseClosed
	}

	r.sequenceNumber++
	record := RecordImpl{Key: key, Value: nil, SequenceNumber: r.sequenceNumber}
	if err := r.wal.Append(ctx, record); err != nil {
		return err
	}
	r.memtable.Put(record)
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
	ctx, span := tracer.Start(ctx, "db.close")
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
	INFO(ctx, "Waiting for background operations to finish...")
	waitStart := time.Now()
	r.wg.Wait()
	if span.IsRecording() {
		span.SetAttributes(attribute.Int64("background_wait_ms", time.Since(waitStart).Milliseconds()))
	}
	INFO(ctx, "Background operations finished.")

	// Re-acquire lock to safely close resources
	r.mu.Lock()
	defer r.mu.Unlock()

	// Close the WAL
	if err := r.wal.Close(); err != nil {
		// Log the error but attempt to close SSTableManager anyway
		ERROR(ctx, "Error closing WAL: %v", err)
		// Optionally return the WAL error immediately, or collect errors
		// return fmt.Errorf("error closing WAL: %w", err)
	} else {
		INFO(ctx, "WAL closed successfully.")
	}

	// Close the SSTableManager
	// Assuming SSTableManager.Close() handles potential errors internally or returns them
	r.ssTableManager.Close(ctx) // SSTableManager.Close currently doesn't return an error
	INFO(ctx, "SSTableManager closed.")

	INFO(ctx, "RinDB closed successfully")
	return nil
}
