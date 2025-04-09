// Package rindb is key-value database
package rindb

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/oklog/ulid/v2"
)

// ErrDatabaseClosed is returned when an operation is attempted on a closed database.
var ErrDatabaseClosed = errors.New("database is closed")

// https://github.com/google/leveldb/blob/main/doc/impl.md

// Rindb is the main database structure
type Rindb struct {
	wal            WAL
	memtable       Memtable
	ssTableManager *SSTableManager
	config         Config
	mu             sync.RWMutex   // Mutex for thread-safe access
	wg             sync.WaitGroup // WaitGroup to track background goroutines
	closed         bool           // Flag to indicate if the database is closed
}

// SSTableManager is storage for SSTables
type SSTableManager struct {
	openedFs *list.List
	levels   []*LinkedList[*FileSystem]
	config   Config
	mu       sync.RWMutex
}

func InitSSTableManager(config Config) (*SSTableManager, error) {
	h := &SSTableManager{openedFs: list.New(), config: config}
	err := h.LoadLevels(config.databaseDir)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// AddSSTable registers a new SSTable file system with the manager at the specified level.
func (h *SSTableManager) AddSSTable(levelNumb int, fs *FileSystem) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Ensure the level exists
	for len(h.levels) <= levelNumb {
		h.levels = append(h.levels, InitLinkedList[*FileSystem]())
	}

	// Add the new SSTable to the end of the level list
	h.levels[levelNumb].PushBack(fs)
	INFO("Registered new SSTable %s at level %d", fs.Path(), levelNumb)
	return nil
}

func (h *SSTableManager) LoadLevels(dir string) error {
	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	sort.Slice(dirEntries, func(i, j int) bool {
		return dirEntries[i].Name() < dirEntries[j].Name()
	})

	levels := make([]*LinkedList[*FileSystem], 0)
	for _, dirEntry := range dirEntries {
		fileName := dirEntry.Name()
		filePath := path.Join(dir, fileName)
		isSSTable := strings.HasSuffix(filePath, ".sst")
		if !isSSTable {
			continue
		}

		idx := strings.Index(fileName, "_")
		if idx == -1 {
			ERROR("Invalid sstable name: %s", fileName)
			continue
		}

		levelNumb, err := strconv.ParseInt(fileName[1:idx], 32, 32)
		if err != nil {
			return err
		}

		extLevelNumb := int(levelNumb) + 1
		if extLevelNumb > len(levels) {
			levels = append(levels, make([]*LinkedList[*FileSystem], extLevelNumb-len(levels))...)
		}

		if levels[levelNumb] == nil {
			levels[levelNumb] = InitLinkedList[*FileSystem]()
		}
		levels[levelNumb].PushBack(&FileSystem{filePath: filePath})
	}
	h.levels = levels
	return nil
}

func (h *SSTableManager) NewSSTableFS(levelNumb int) (*FileSystem, error) {
	uid := ulid.Make()
	sstableFileName := path.Join(h.config.databaseDir, fmt.Sprintf("l%02d_%s.sst", levelNumb, uid.String()))
	fs, err := OpenFS(sstableFileName)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func (h *SSTableManager) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	element := h.openedFs.Front()
	for element != nil {
		fs, ok := element.Value.(*FileSystem)
		if !ok {
			ERROR("can not cast element to file system")
			break
		}

		if err := fs.Close(); err != nil {
			ERROR("Error closing file %s: %v", fs.Path(), err)
		}
		INFO("Closed %s successfully", fs.Path())
		element = element.Next()
	}

	for _, level := range h.levels {
		// Usually, the levels are fully populated
		// but it's possible that some levels are nil when testing
		if level == nil {
			continue
		}
		levelIterator := level.Iterator()
		for levelIterator.HasNext() {
			fs, err := levelIterator.Next()
			if err != nil {
				ERROR("Error iterating through level: %v", err)
				continue
			}

			if !fs.IsOpened() {
				WARN("File %s is already closed", fs.Path())
				continue
			}

			if err := fs.Close(); err != nil {
				ERROR("Error closing file %s: %v", fs.Path(), err)
				continue
			}
			INFO("Closed %s successfully", fs.Path())
		}
	}
}

func (h *SSTableManager) shouldCompact(levelNumb int, level *LinkedList[*FileSystem]) bool {
	// No longer need internal constants

	if level == nil || level.Len() == 0 {
		return false // Cannot compact an empty or non-existent level
	}

	if levelNumb == 0 {
		// Use the config value for L0 threshold
		return level.Len() >= h.config.level0CompactionThreshold
	}

	// Calculate total size in Bytes for higher levels
	var totalSizeBytes int64 // Use int64 to avoid overflow
	iter := level.Iterator()
	for iter.HasNext() {
		fs, err := iter.Next() // Use Next, no need to PickNext here
		if err != nil {
			ERROR("Error iterating level %d for size check: %v", levelNumb, err)
			continue // Skip problematic entries
		}
		info, err := os.Stat(fs.filePath)
		if err != nil {
			// Log error if file cannot be stated, might indicate an issue
			ERROR("Error stating file %s for size check: %v", fs.filePath, err)
			continue // Skip files we can't stat
		}
		totalSizeBytes += info.Size()
	}

	// Calculate the threshold for this level using config values
	// Ensure multiplier is at least 1 to avoid issues with Pow(0) or negative powers
	multiplier := h.config.levelSizeMultiplier
	if multiplier < 1 {
		WARN("levelSizeMultiplier is %d, using 1 instead for threshold calculation.", multiplier)
		multiplier = 1 // Prevent multiplier < 1
	}
	// Use float64 for Pow, then convert threshold to int64 bytes for comparison
	levelThresholdBytes := int64(h.config.baseCompactionSizeMB) * int64(math.Pow(float64(multiplier), float64(levelNumb))) * 1024 * 1024

	// Compare total bytes with threshold bytes
	return totalSizeBytes >= levelThresholdBytes
}

func (h *SSTableManager) Compact() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	INFO("Starting compaction check across %d levels", len(h.levels))

	var err error
	compactionOccurred := false // Track if any compaction actually happened
	for levelNumb := 0; levelNumb < len(h.levels); levelNumb++ {
		if h.levels[levelNumb] == nil {
			continue
		}
		level := h.levels[levelNumb]

		if !h.shouldCompact(levelNumb, level) {
			INFO("Level %d (size/count: %d) does not meet compaction threshold, skipping.", levelNumb, level.Len())
			continue
		}

		INFO("Level %d (size/count: %d) requires compaction.", levelNumb, level.Len())
		newLevelNumb := levelNumb + 1

		if levelNumb == 0 {
			// Level 0: Merge all SSTables into L1
			err = h.compactLevel0(level, newLevelNumb)
			if err != nil {
				ERROR("Error compacting Level 0: %v", err)
				return err
			}
		} else {
			// Higher levels: Pick one SSTable and merge with overlapping L1+ SSTables
			err = h.compactHigherLevel(level, newLevelNumb)
			if err != nil {
				ERROR("Error compacting Level %d: %v", levelNumb, err)
				return err
			}
		}
		// If compaction happened for this level, set the flag
		if err == nil { // Assuming err is nil if compaction was successful or skipped appropriately
			compactionOccurred = true // Or set based on actual merge/compact calls succeeding
		}
	}

	if compactionOccurred {
		INFO("Compaction process completed.")
	} else {
		INFO("No levels required compaction.")
	}
	return nil
}

func (h *SSTableManager) compactLevel0(level *LinkedList[*FileSystem], newLevelNumb int) error {
	var sstablesToMerge []SStable
	iter := level.Iterator()
	for iter.HasNext() {
		fs, err := iter.PickNext()
		if err != nil {
			return err
		}
		if err := fs.Open(); err != nil {
			return err
		}
		sstable, err := NewSSTable(h.config, fs)
		if err != nil {
			_ = fs.Close()
			return err
		}
		sstablesToMerge = append(sstablesToMerge, sstable)
	}

	// Find overlapping SSTables in the next level
	overlappingSSTables, err := h.findOverlappingSSTables(newLevelNumb, sstablesToMerge)
	if err != nil {
		return err
	}

	sstablesToMerge = append(overlappingSSTables, sstablesToMerge...)
	return h.mergeSSTables(newLevelNumb, sstablesToMerge)
}

func (h *SSTableManager) compactHigherLevel(level *LinkedList[*FileSystem], newLevelNumb int) error {
	// Pick *all* SSTables from the source level to compact
	var sstablesToMerge []SStable
	iter := level.Iterator()
	for iter.HasNext() {
		fs, err := iter.PickNext() // Removes from the source level list
		if err != nil {
			// Attempt to close any already opened SSTables before returning error
			for _, sst := range sstablesToMerge {
				_ = sst.Close()
			}
			return fmt.Errorf("error picking next SSTable from level: %w", err)
		}
		if err := fs.Open(); err != nil {
			// Attempt to close any already opened SSTables before returning error
			for _, sst := range sstablesToMerge {
				_ = sst.Close()
			}
			return fmt.Errorf("error opening picked SSTable %s: %w", fs.Path(), err)
		}
		sstable, err := NewSSTable(h.config, fs)
		if err != nil {
			_ = fs.Close()
			// Attempt to close any already opened SSTables before returning error
			for _, sst := range sstablesToMerge {
				_ = sst.Close()
			}
			return fmt.Errorf("error creating SStable object for %s: %w", fs.Path(), err)
		}
		sstablesToMerge = append(sstablesToMerge, sstable)
	}

	if len(sstablesToMerge) == 0 {
		WARN("compactHigherLevel called on an empty or already processed level.")
		return nil // Nothing to merge
	}

	// Find overlapping SSTables in the next level based on the combined range of source SSTables
	overlappingSSTables, err := h.findOverlappingSSTables(newLevelNumb, sstablesToMerge)
	if err != nil {
		return err
	}

	sstablesToMerge = append(overlappingSSTables, sstablesToMerge...)
	err = h.mergeSSTables(newLevelNumb, sstablesToMerge)
	if err != nil {
		return err
	}

	// Remove overlapping SSTables from the level after successful merge
	iter = h.levels[newLevelNumb].Iterator()
	for iter.HasNext() {
		fs, err := iter.Next()
		if err != nil {
			return err
		}
		for _, sst := range overlappingSSTables {
			if fs.Path() == sst.Path() {
				iter.RemoveCurrent()
				break
			}
		}
	}
	return nil
}

func (h *SSTableManager) findOverlappingSSTables(levelNumb int, sources []SStable) ([]SStable, error) {
	if levelNumb >= len(h.levels) || h.levels[levelNumb] == nil {
		return nil, nil // No overlapping SSTables if the level doesn’t exist
	}

	// Get key range of sources
	minKey, maxKey := getKeyRange(sources)
	var overlapping []SStable
	iter := h.levels[levelNumb].Iterator()
	for iter.HasNext() {
		fs, err := iter.Next()
		if err != nil {
			return nil, err
		}
		if err := fs.Open(); err != nil {
			return nil, err
		}
		sstable, err := NewSSTable(h.config, fs)
		if err != nil {
			_ = fs.Close()
			return nil, err
		}
		if sstable.Overlaps(minKey, maxKey) {
			overlapping = append(overlapping, sstable)
		} else {
			_ = fs.Close() // Close if not overlapping
		}
	}
	return overlapping, nil
}

func getKeyRange(sstables []SStable) (Bytes, Bytes) {
	var minKey, maxKey Bytes
	for i, sst := range sstables {
		sstMin, sstMax := sst.GetKeyRange() // Assume SStable has GetKeyRange
		if i == 0 {
			minKey, maxKey = sstMin, sstMax
		} else {
			if minKey.Compare(sstMin) > 0 {
				minKey = sstMin
			}
			if maxKey.Compare(sstMax) < 0 {
				maxKey = sstMax
			}
		}
	}
	return minKey, maxKey
}

// mergeSSTables merges a list of SSTables into a new SSTable at the specified level.
// Assumes the caller holds the necessary lock (e.g., h.mu.Lock()).
func (h *SSTableManager) mergeSSTables(newLevelNumb int, pickedUpSSTable []SStable) error {
	newLevelSSTable, err := h.NewSSTableFS(newLevelNumb)
	if err != nil {
		return err
	}

	if _, err := mergeSSTables(h.config, newLevelSSTable, pickedUpSSTable); err != nil {
		return err
	}

	if len(h.levels) == newLevelNumb {
		h.levels = append(h.levels, InitLinkedList[*FileSystem]())
	}
	h.levels[newLevelNumb].PushBack(newLevelSSTable)

	// remove merged sstable
	for _, sstable := range pickedUpSSTable {
		if err := os.Remove(sstable.Path()); err != nil {
			log.Printf("Error removing file %s: %v", sstable.Path(), err)
		}
	}
	return nil
}

func (h *SSTableManager) searchKey(key Bytes) (Bytes, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var latestValue Bytes
	var found bool

	// In an LSM-tree, data is organized into levels, where lower levels (e.g., level 0) contain newer data,
	// and higher levels (e.g., level 1, 2, etc.) contain older, compacted data. When searching for a key,
	// we must prioritize the most recent data, as it reflects the latest updates or deletions (e.g., tombstones).
	// Within each level, SSTables are also ordered by recency, especially in level 0 where SSTables may overlap.
	// Iterating from the bottom (most recent SSTable) to the top (oldest SSTable) ensures we find the latest
	// version of the key first. Once a key is found in a level, we can stop searching that level, as newer
	// levels take precedence, and within a level, the most recent SSTable's value is authoritative.
	// This bottom-up traversal aligns with LSM's principle of prioritizing recency in a write-heavy system.
	for levelNumb := range h.levels {
		if h.levels[levelNumb] == nil {
			// No more levels to search
			break
		}

		iterator := h.levels[levelNumb].IteratorFromBottom()
		fs := iterator.Value()
		for fs != nil {
			if err := fs.Open(); err != nil {
				return nil, err
			}
			sstable, err := NewSSTable(h.config, fs)
			if err != nil {
				_ = fs.Close()
				return nil, err
			}

			value, err := sstable.GetValue(key)
			if err == nil {
				// Found a value or tombstone; this is the latest so far
				latestValue = value
				found = true
				_ = fs.Close()
				break
			}
			if !errors.Is(err, ErrKeyNotFound) {
				_ = fs.Close()
				return nil, err
			}
			if err := fs.Close(); err != nil {
				return nil, err
			}

			if !iterator.HasPrev() {
				break
			}

			fs, err = iterator.Prev()
			if err != nil {
				return nil, err
			}
		}
		if found {
			break
		}
	}

	if found {
		return latestValue, nil
	}
	return nil, ErrKeyNotFound
}

func mergeSSTables(config Config, target *FileSystem, sources []SStable) (SStable, error) {
	memtable := InitMemtable(config)
	for _, sstable := range sources {
		iterator, err := sstable.Iterator()
		if err != nil {
			return SStable{}, err
		}
		for iterator.HasNext() {
			record, err := iterator.Next()
			if err != nil {
				return SStable{}, err
			}
			memtable.Put(record.GetKey(), record.GetValue())
		}
	}
	sstable, err := Flush(config, memtable, target)
	if err != nil {
		return SStable{}, err
	}
	return sstable, nil
}

func InitRinDB(opts ...Option) (Rindb, error) {
	cfg := NewConfig(opts...)

	// Ensure the database directory exists
	if err := os.MkdirAll(cfg.databaseDir, 0750); err != nil {
		return Rindb{}, fmt.Errorf("failed to create database directory %s: %w", cfg.databaseDir, err)
	}

	walPath := path.Join(cfg.databaseDir, "WAL")
	fs, err := OpenFS(walPath)
	if err != nil {
		return Rindb{}, fmt.Errorf("failed to open WAL file %s: %w", walPath, err)
	}
	wal := NewWAL(cfg, fs)
	memtable, err := wal.Load()
	if err != nil {
		return Rindb{}, err
	}
	ssTableManager, err := InitSSTableManager(cfg)
	if err != nil {
		// Consider closing the WAL file system if manager init fails
		_ = fs.Close()
		return Rindb{}, fmt.Errorf("failed to initialize SSTable manager: %w", err)
	}
	INFO("Initialized RinDB with database directory %s", cfg.databaseDir)
	return Rindb{
		wal:            wal,
		memtable:       memtable,
		ssTableManager: ssTableManager,
		config:         cfg,
	}, nil
}

func (r *Rindb) Get(key Bytes) (Bytes, error) {
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
	return r.ssTableManager.searchKey(key)
}

func (r *Rindb) Put(key, value Bytes) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrDatabaseClosed
	}

	record := RecordImpl{Key: key, Value: value}
	if err := r.wal.Append(record); err != nil {
		return err
	}
	r.memtable.Put(key, value) // This now updates the internal size estimate

	// Check estimated byte size and flush if needed
	// Cast ByteSize() to uint to match maxMemtableSize type
	// Check estimated byte size and flush if needed
	if uint(r.memtable.ByteSize()) >= r.config.maxMemtableSize {
		INFO("Memtable estimated size %d reached threshold %d, flushing.", r.memtable.ByteSize(), r.config.maxMemtableSize)

		// Create new SSTable file system for level 0
		fs, err := r.ssTableManager.NewSSTableFS(0)
		if err != nil {
			ERROR("Failed to create new SSTable file system: %v", err)
			return fmt.Errorf("failed to create new SSTable file system: %w", err)
		}

		_, err = Flush(r.config, r.memtable, fs)
		if err != nil {
			_ = fs.Close() // Attempt to close FS on flush error
			ERROR("Failed to flush memtable: %v", err)
			return fmt.Errorf("failed to flush memtable: %w", err)
		}

		// Wont close the file system here, as it will be managed by ssTableManager
		r.ssTableManager.openedFs.PushBack(fs) // Add to opened file systems

		// Register the new SSTable with ssTableManager
		if err := r.ssTableManager.AddSSTable(0, fs); err != nil {
			ERROR("Failed to register new SSTable %s: %v", fs.Path(), err)
			return fmt.Errorf("failed to register new SSTable %s: %w", fs.Path(), err)
		}

		// Clear the memtable and clean the WAL *after* successful flush and registration
		r.memtable.Clear()
		if err := r.wal.Clean(); err != nil {
			ERROR("Failed to clean WAL after memtable flush: %v", err)
			return fmt.Errorf("failed to clean WAL: %w", err)
		}

		// Trigger compaction in a goroutine *after* flushing
		INFO("Triggering background compaction check.")
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			INFO("Background compaction goroutine started.")
			if err := r.ssTableManager.Compact(); err != nil {
				ERROR("Background compaction failed: %v", err)
			} else {
				INFO("Background compaction goroutine finished.")
			}
		}()
	}
	return nil
}

func (r *Rindb) Remove(key Bytes) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrDatabaseClosed
	}

	record := RecordImpl{Key: key, Value: nil}
	if err := r.wal.Append(record); err != nil {
		return err
	}
	r.memtable.Put(record.GetKey(), record.GetValue())
	return nil
}

// Close closes the Rindb instance, ensuring all resources are released.
func (r *Rindb) Close() error {
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
	INFO("Waiting for background operations to finish...")
	r.wg.Wait()
	INFO("Background operations finished.")

	// Re-acquire lock to safely close resources
	r.mu.Lock()
	defer r.mu.Unlock()

	// Close the WAL
	if err := r.wal.Close(); err != nil {
		// Log the error but attempt to close SSTableManager anyway
		ERROR("Error closing WAL: %v", err)
		// Optionally return the WAL error immediately, or collect errors
		// return fmt.Errorf("error closing WAL: %w", err)
	} else {
		INFO("WAL closed successfully.")
	}

	// Close the SSTableManager
	// Assuming SSTableManager.Close() handles potential errors internally or returns them
	r.ssTableManager.Close() // SSTableManager.Close currently doesn't return an error
	INFO("SSTableManager closed.")

	INFO("RinDB closed successfully")
	return nil
}
