package rindb

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// SSTableManager manages SSTable storage and compaction in a leveled structure.
// Responsibilities:
// - Maintains multiple levels of SSTables (L0, L1, etc.)
// - Handles compaction across levels based on size/count thresholds
// - Manages file handles for SSTables
// - Coordinates concurrent access with read/write locks
type SSTableManager struct {
	openedFs *list.List // List of *FileSystem that are currently opened
	levels   []*LinkedList[*FileSystem]
	config   Config
	mu       sync.RWMutex
}

// openAndLoadSSTable opens a FileSystem, creates an SSTable object from it,
// and adds the FileSystem to the manager's list of opened file systems.
// It returns the created *SSTable or an error.
func (h *SSTableManager) openAndLoadSSTable(ctx context.Context, fs *FileSystem) (*SStable, error) {
	if err := fs.Open(ctx); err != nil {
		return nil, fmt.Errorf("failed to open sstable file %s: %w", fs.Path(), err)
	}
	sstable, err := NewSSTable(ctx, h.config, fs)
	if err != nil {
		_ = fs.Close() // Ensure file is closed on SSTable creation error
		return nil, fmt.Errorf("failed to create sstable object for %s: %w", fs.Path(), err)
	}
	h.openedFs.PushBack(fs) // Track opened file system
	return &sstable, nil
}

func InitSSTableManager(ctx context.Context, config Config) (*SSTableManager, error) {
	h := &SSTableManager{openedFs: list.New(), config: config}
	err := h.LoadLevels(config.databaseDir)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// AddSSTable registers a new SSTable file system with the manager at the specified level.
func (h *SSTableManager) AddSSTable(ctx context.Context, levelNumb int, fs *FileSystem) error {
	ctx, span := sstableMgmtTracer.Start(ctx, "SSTableManager.AddSSTable")
	start := time.Now()
	defer func() {
		span.End()
		addSSTableLatency.Record(ctx, float64(time.Since(start).Milliseconds()))
		addSSTableCalls.Add(ctx, 1)
	}()

	h.mu.Lock()
	defer h.mu.Unlock()

	// Ensure the level exists
	for len(h.levels) <= levelNumb {
		h.levels = append(h.levels, InitLinkedList[*FileSystem]())
	}

	// Add the new SSTable to the end of the level list
	h.levels[levelNumb].PushBack(fs)
	INFO(ctx, "Registered new SSTable %s at level %d", fs.Path(), levelNumb)
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
			return fmt.Errorf("invalid sstable name: %s", fileName)
		}

		levelNumb, err := strconv.ParseInt(fileName[1:idx], 10, 32)
		if err != nil {
			return fmt.Errorf("invalid level in sstable name %s: %w", fileName, err)
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

func (h *SSTableManager) NewSSTableFS(ctx context.Context, levelNumb int) (*FileSystem, error) {
	uid := ulid.Make()
	sstableFileName := path.Join(h.config.databaseDir, fmt.Sprintf("l%02d_%s.sst", levelNumb, uid.String()))
	fs, err := OpenFS(ctx, sstableFileName)
	if err != nil {
		return nil, err
	}

	h.openedFs.PushBack(fs)
	return fs, nil
}

func (h *SSTableManager) Close(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()

	element := h.openedFs.Front()
	for element != nil {
		fs, ok := element.Value.(*FileSystem)
		if !ok {
			ERROR(ctx, "can not cast element to file system")
			break
		}

		if err := fs.Close(); err != nil {
			ERROR(ctx, "Error closing file %s: %v", fs.Path(), err)
		}
		INFO(ctx, "Closed %s successfully", fs.Path())
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
				ERROR(ctx, "Error iterating through level: %v", err)
				continue
			}

			if !fs.IsOpened() {
				WARN(ctx, "File %s is already closed", fs.Path())
				continue
			}

			if err := fs.Close(); err != nil {
				ERROR(ctx, "Error closing file %s: %v", fs.Path(), err)
				continue
			}
			INFO(ctx, "Closed %s successfully", fs.Path())
		}
	}
}

func (h *SSTableManager) shouldCompact(ctx context.Context, levelNumb int, level *LinkedList[*FileSystem]) bool {
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
			ERROR(ctx, "Error iterating level %d for size check: %v", levelNumb, err)
			continue // Skip problematic entries
		}
		info, err := os.Stat(fs.filePath)
		if err != nil {
			// Log error if file cannot be stated, might indicate an issue
			ERROR(ctx, "Error stating file %s for size check: %v", fs.filePath, err)
			continue // Skip files we can't stat
		}
		totalSizeBytes += info.Size()
	}

	// Calculate the threshold for this level using config values
	// Ensure multiplier is at least 1 to avoid issues with Pow(0) or negative powers
	multiplier := h.config.levelSizeMultiplier
	if multiplier < 1 {
		WARN(ctx, "levelSizeMultiplier is %d, using 1 instead for threshold calculation.", multiplier)
		multiplier = 1 // Prevent multiplier < 1
	}
	// Use float64 for Pow, then convert threshold to int64 bytes for comparison
	levelThresholdBytes := int64(h.config.baseCompactionSizeMB) * int64(math.Pow(float64(multiplier), float64(levelNumb))) * 1024 * 1024

	// Compare total bytes with threshold bytes
	return totalSizeBytes >= levelThresholdBytes
}

func (h *SSTableManager) Compact(ctx context.Context) error {
	ctx, span := sstableMgmtTracer.Start(ctx, "SSTableManager.Compact")
	start := time.Now()
	defer func() {
		span.End()
		compactLatency.Record(ctx, float64(time.Since(start).Milliseconds()))
		compactCalls.Add(ctx, 1)
	}()

	h.mu.Lock()
	defer h.mu.Unlock()
	INFO(ctx, "Starting compaction check across %d levels", len(h.levels))

	var err error
	compactionOccurred := false // Track if any compaction actually happened
	for levelNumb := 0; levelNumb < len(h.levels); levelNumb++ {
		if h.levels[levelNumb] == nil {
			continue
		}
		level := h.levels[levelNumb]

		if !h.shouldCompact(ctx, levelNumb, level) {
			INFO(ctx, "Level %d (size/count: %d) does not meet compaction threshold, skipping.", levelNumb, level.Len())
			continue
		}

		INFO(ctx, "Level %d (size/count: %d) requires compaction.", levelNumb, level.Len())
		newLevelNumb := levelNumb + 1

		if levelNumb == 0 {
			// Level 0: Merge all SSTables into L1
			err = h.compactLevel0(ctx, level, newLevelNumb)
			if err != nil {
				ERROR(ctx, "Error compacting Level 0: %v", err)
				return err
			}
		} else {
			// Higher levels: Pick one SSTable and merge with overlapping L1+ SSTables
			err = h.compactHigherLevel(ctx, level, newLevelNumb)
			if err != nil {
				ERROR(ctx, "Error compacting Level %d: %v", levelNumb, err)
				return err
			}
		}
		// If compaction happened for this level, set the flag
		if err == nil { // Assuming err is nil if compaction was successful or skipped appropriately
			compactionOccurred = true // Or set based on actual merge/compact calls succeeding
		}
	}

	if compactionOccurred {
		INFO(ctx, "Compaction process completed.")
	} else {
		INFO(ctx, "No levels required compaction.")
	}
	return nil
}

func (h *SSTableManager) compactLevel0(ctx context.Context, level *LinkedList[*FileSystem], newLevelNumb int) error {
	var sstablesToMerge []SStable
	iter := level.Iterator()
	for iter.HasNext() {
		fs, err := iter.PickNext()
		if err != nil {
			return err
		}
		sstable, err := h.openAndLoadSSTable(ctx, fs)
		if err != nil {
			return err
		}
		sstablesToMerge = append(sstablesToMerge, *sstable)
	}

	// Find overlapping SSTables in the next level
	overlappingSSTables, err := h.findOverlappingSSTables(ctx, newLevelNumb, sstablesToMerge)
	if err != nil {
		return err
	}

	sstablesToMerge = append(overlappingSSTables, sstablesToMerge...)
	return h.mergeSSTables(ctx, newLevelNumb, sstablesToMerge)
}

func (h *SSTableManager) compactHigherLevel(ctx context.Context, level *LinkedList[*FileSystem], newLevelNumb int) error {
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
		sstable, err := h.openAndLoadSSTable(ctx, fs)
		if err != nil {
			// Attempt to close any already opened SSTables before returning error
			for _, sst := range sstablesToMerge {
				_ = sst.Close()
			}
			return fmt.Errorf("error creating SStable object for %s: %w", fs.Path(), err)
		}
		sstablesToMerge = append(sstablesToMerge, *sstable)
	}

	if len(sstablesToMerge) == 0 {
		WARN(ctx, "compactHigherLevel called on an empty or already processed level.")
		return nil // Nothing to merge
	}

	// Find overlapping SSTables in the next level based on the combined range of source SSTables
	overlappingSSTables, err := h.findOverlappingSSTables(ctx, newLevelNumb, sstablesToMerge)
	if err != nil {
		return err
	}

	sstablesToMerge = append(overlappingSSTables, sstablesToMerge...)
	err = h.mergeSSTables(ctx, newLevelNumb, sstablesToMerge)
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

func (h *SSTableManager) findOverlappingSSTables(ctx context.Context, levelNumb int, sources []SStable) ([]SStable, error) {
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
		sstable, err := h.openAndLoadSSTable(ctx, fs)
		if err != nil {
			return nil, err
		}
		if sstable.Overlaps(minKey, maxKey) {
			overlapping = append(overlapping, *sstable)
		} else {
			_ = fs.Close() // Close if not overlapping
		}
	}
	return overlapping, nil
}

// mergeSSTables merges a list of SSTables into a new SSTable at the specified level.
// Assumes the caller holds the necessary lock (e.g., h.mu.Lock()).
func (h *SSTableManager) mergeSSTables(ctx context.Context, newLevelNumb int, pickedUpSSTable []SStable) error {
	newLevelSSTable, err := h.NewSSTableFS(ctx, newLevelNumb)
	if err != nil {
		return err
	}

	if _, err := mergeSSTables(ctx, h.config, newLevelSSTable, pickedUpSSTable); err != nil {
		return err
	}

	if len(h.levels) == newLevelNumb {
		h.levels = append(h.levels, InitLinkedList[*FileSystem]())
	}
	h.levels[newLevelNumb].PushBack(newLevelSSTable)

	// remove merged sstable
	for _, sstable := range pickedUpSSTable {
		if err := os.Remove(sstable.Path()); err != nil {
			ERROR(ctx, "Error removing file %s: %v", sstable.Path(), err)
		}
	}
	return nil
}

// GetRelevantSSTables finds SSTables that might contain keys within the given range [startKey, endKey].
// For Level 0, all SSTables are considered relevant.
// For Level 1 and higher, SSTables are checked for overlap with the given key range.
// The returned LinkedList contains *SSTable objects, which are opened and ready for use.
// The SSTables in Level 0 are appended to the list, maintaining newest-first order.
// while SSTables from higher levels are added to the back.
func (h *SSTableManager) GetRelevantSSTables(ctx context.Context, startKey, endKey Bytes) (*LinkedList[*SStable], error) {
	ctx, span := sstableMgmtTracer.Start(ctx, "SSTableManager.GetRelevantSSTables")
	start := time.Now()
	defer func() {
		span.End()
		getRelevantLatency.Record(ctx, float64(time.Since(start).Milliseconds()))
		getRelevantCalls.Add(ctx, 1)
	}()

	h.mu.RLock()
	defer h.mu.RUnlock()

	relevantSSTables := InitLinkedList[*SStable]()

	for levelNumb, level := range h.levels {
		if level == nil || level.Len() == 0 {
			continue
		}

		// Level 0: All SSTables are considered relevant.
		// Iterate from newest to oldest (bottom to top) to prioritize newer data.
		if levelNumb == 0 {
			iterator := level.IteratorFromBottom()
			fs := iterator.Value()
			for fs != nil {
				sstable, err := h.openAndLoadSSTable(ctx, fs)
				if err != nil {
					return nil, fmt.Errorf("failed to open and load sstable %s: %w", fs.Path(), err)
				}

				relevantSSTables.PushBack(sstable) // Add to back to maintain newest-first order for L0

				if !iterator.HasPrev() {
					break
				}
				fs, err = iterator.Prev()
				if err != nil {
					return nil, fmt.Errorf("failed to get previous sstable in level %d: %w", levelNumb, err)
				}
			}
		} else {
			// Levels 1+: Check for overlap with the given key range.
			// Iterate from oldest to newest (top to bottom) for higher levels.
			iterator := level.Iterator()
			for iterator.HasNext() {
				fs, err := iterator.Next()
				if err != nil {
					return nil, fmt.Errorf("failed to get next sstable in level %d: %w", levelNumb, err)
				}
				sstable, err := h.openAndLoadSSTable(ctx, fs)
				if err != nil {
					return nil, fmt.Errorf("failed to open and load sstable %s: %w", fs.Path(), err)
				}

				if sstable.Overlaps(startKey, endKey) {
					relevantSSTables.PushBack(sstable) // Add to back for higher levels
				} else {
					_ = fs.Close() // Close if not relevant
				}
			}
		}
	}

	getRelevantSSTables.Add(ctx, int64(relevantSSTables.Len()))
	return relevantSSTables, nil
}

func (h *SSTableManager) searchKey(ctx context.Context, key Bytes) (Bytes, error) {
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
			sstable, err := h.openAndLoadSSTable(ctx, fs)
			if err != nil {
				return nil, err
			}

			value, err := sstable.GetValue(ctx, key)
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

// TODO: This is a temporary approach that only scans L0 SSTables.
// We should scan *all* levels to find the true max sequence number.
// The correct long-term solution is to maintain a MANIFEST file
// that tracks global sequence number metadata across all levels.
func getMaxSequenceNumberFromSSTables(ctx context.Context, ssTableManager *SSTableManager) (uint64, error) {
	var maxSeqNum uint64
	if len(ssTableManager.levels) > 0 && ssTableManager.levels[0] != nil {
		level0 := ssTableManager.levels[0]
		levelIterator := level0.Iterator()
		for levelIterator.HasNext() {
			fs, err := levelIterator.Next()
			if err != nil {
				return 0, err
			}
			sstable, err := ssTableManager.openAndLoadSSTable(ctx, fs)
			if err != nil {
				return 0, err
			}
			sstSeqNum, err := sstable.MaxSequenceNumber()
			if err != nil {
				return 0, err
			}

			maxSeqNum = max(maxSeqNum, sstSeqNum)

			if err := fs.Close(); err != nil {
				return 0, err
			}
		}
	}
	return maxSeqNum, nil
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

func mergeSSTables(ctx context.Context, config Config, target *FileSystem, sources []SStable) (SStable, error) {
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
			memtable.Put(record)
		}
	}
	sstable, err := Flush(ctx, config, memtable, target)
	if err != nil {
		return SStable{}, err
	}
	return sstable, nil
}
