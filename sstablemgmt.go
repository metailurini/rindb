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
	"github.com/shirou/gopsutil/v3/disk"
)

// writeRateAlpha is the smoothing factor for write-rate exponential moving
// average. A higher value weights recent samples more heavily.
const writeRateAlpha = 0.2

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

	// minSnapshotSeq is the smallest sequence number of any active
	// snapshot. When no snapshots are active it is set to
	// math.MaxUint64, allowing tombstone GC at the bottommost level.
	minSnapshotSeq uint64

	// writeRate is the moving average of writes per second.
	writeRate float64

	// ioLoad represents the fraction of time the disk was busy with I/O
	// operations in the last sampling interval (0-1).
	ioLoad float64

	// internal counters for sampling
	writeCounter    uint64
	lastWriteSample time.Time
	lastIOTotal     uint64
	lastIOSample    time.Time

	// goroutine management for I/O load sampler
	stopIOLoadSampler chan struct{}
	ioSamplerWG       sync.WaitGroup

	// dependency injection for testing
	now         func() time.Time
	diskSampler func() (uint64, error)
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
	h := &SSTableManager{
		openedFs:          list.New(),
		config:            config,
		stopIOLoadSampler: make(chan struct{}),
		now:               time.Now,
		minSnapshotSeq:    math.MaxUint64,
		// diskSampler aggregates the IoTime from all available disk
		// counters. IoTime is the number of milliseconds the disk has
		// been busy since boot.
		diskSampler: func() (uint64, error) {
			counters, err := disk.IOCounters()
			if err != nil {
				return 0, err
			}
			var total uint64
			for _, c := range counters {
				total += c.IoTime
			}
			return total, nil
		},
	}

	if err := h.LoadLevels(config.databaseDir); err != nil {
		return nil, err
	}

	h.ioSamplerWG.Add(1)
	go h.startIOLoadSampler()

	return h, nil
}

// setMinSnapshotSeq updates the minimum snapshot sequence number
// in a thread-safe manner.
func (h *SSTableManager) setMinSnapshotSeq(seq uint64) {
	h.mu.Lock()
	h.minSnapshotSeq = seq
	h.mu.Unlock()
}

// recordWrite increments the write counter and, once a second has elapsed,
// updates the moving average of writes per second using an exponential moving
// average. It is called for every `Put`.
func (h *SSTableManager) recordWrite() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.writeCounter++
	now := h.now()
	if h.lastWriteSample.IsZero() {
		h.lastWriteSample = now
		return
	}
	if now.Sub(h.lastWriteSample) >= time.Second {
		duration := now.Sub(h.lastWriteSample).Seconds()
		rate := float64(h.writeCounter) / duration
		// Exponential moving average with smoothing factor writeRateAlpha
		// to smooth out short-term spikes in throughput.
		h.writeRate = (1-writeRateAlpha)*h.writeRate + writeRateAlpha*rate
		h.writeCounter = 0
		h.lastWriteSample = now
	}
}

// sampleIOLoad reads the cumulative IoTime counter and derives the fraction of
// time the disk was busy since the last sample. If the counter decreases it is
// assumed to have reset and the sample is skipped.
func (h *SSTableManager) sampleIOLoad() {
	total, err := h.diskSampler()
	if err != nil {
		return
	}
	now := h.now()
	if !h.lastIOSample.IsZero() {
		if total < h.lastIOTotal {
			// Counter reset detected; reset baseline.
			h.lastIOTotal = total
			h.lastIOSample = now
			return
		}
		deltaIO := total - h.lastIOTotal
		deltaTime := now.Sub(h.lastIOSample).Milliseconds()
		if deltaTime > 0 {
			load := float64(deltaIO) / float64(deltaTime)
			h.mu.Lock()
			h.ioLoad = load
			h.mu.Unlock()
		}
	}
	h.lastIOTotal = total
	h.lastIOSample = now
}

// startIOLoadSampler periodically records disk utilization until signalled to
// stop. It is launched in a background goroutine by `InitSSTableManager` and
// terminates when `stopIOLoadSampler` is closed.
func (h *SSTableManager) startIOLoadSampler() {
	defer h.ioSamplerWG.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-h.stopIOLoadSampler:
			return
		case <-ticker.C:
			h.sampleIOLoad()
		}
	}
}

// dynamicTriggerHit evaluates whether the dynamic compaction conditions are
// met: the write rate exceeds the configured trigger while disk utilization is
// below the allowed maximum.
func (h *SSTableManager) dynamicTriggerHit() bool {
	if h.config.writeRateTrigger <= 0 || h.config.ioLoadMax <= 0 {
		return false
	}
	return h.writeRate > h.config.writeRateTrigger && h.ioLoad < h.config.ioLoadMax
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
		h.levels = append(h.levels, nil)
	}
	if h.levels[levelNumb] == nil {
		h.levels[levelNumb] = InitLinkedList[*FileSystem]()
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
	if h.stopIOLoadSampler != nil {
		close(h.stopIOLoadSampler)
		h.ioSamplerWG.Wait()
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for e := h.openedFs.Front(); e != nil; {
		next := e.Next()
		fs, ok := e.Value.(*FileSystem)
		if !ok {
			ERROR(ctx, "can not cast element to file system")
			break
		}

		if err := fs.Close(); err != nil {
			ERROR(ctx, "Error closing file %s: %v", fs.Path(), err)
		} else {
			INFO(ctx, "Closed %s successfully", fs.Path())
		}
		h.openedFs.Remove(e)
		e = next
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
				h.removeOpenedFS(fs)
				continue
			}
			INFO(ctx, "Closed %s successfully", fs.Path())
			h.removeOpenedFS(fs)
		}
	}
}

func (h *SSTableManager) shouldCompact(ctx context.Context, levelNumb int, level *LinkedList[*FileSystem]) bool {
	if level == nil || level.Len() == 0 {
		return false // Cannot compact an empty or non-existent level
	}

	if h.dynamicTriggerHit() {
		return true
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
	if err := h.mergeSSTables(ctx, newLevelNumb, sstablesToMerge); err != nil {
		return err
	}

	return h.removeOverlappingFromLevel(h.levels[newLevelNumb], overlappingSSTables)
}

func (h *SSTableManager) compactHigherLevel(ctx context.Context, level *LinkedList[*FileSystem], newLevelNumb int) error {
	// Pick *all* SSTables from the source level to compact
	var sstablesToMerge []SStable
	iter := level.Iterator()
	for iter.HasNext() {
		fs, err := iter.PickNext() // Removes from the source level list
		if err != nil {
			// Attempt to close any already opened SSTables before returning error
			h.closeSSTables(sstablesToMerge)
			return fmt.Errorf("error picking next SSTable from level: %w", err)
		}
		sstable, err := h.openAndLoadSSTable(ctx, fs)
		if err != nil {
			// Attempt to close any already opened SSTables before returning error
			h.closeSSTables(sstablesToMerge)
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

	return h.removeOverlappingFromLevel(h.levels[newLevelNumb], overlappingSSTables)
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
			h.closeSSTables(overlapping)
			return nil, err
		}
		sstable, err := h.openAndLoadSSTable(ctx, fs)
		if err != nil {
			h.closeSSTables(overlapping)
			return nil, err
		}
		if sstable.Overlaps(minKey, maxKey) {
			overlapping = append(overlapping, *sstable)
		} else {
			_ = fs.Close() // Close if not overlapping
			h.removeOpenedFS(fs)
		}
	}
	return overlapping, nil
}

// removeOverlappingFromLevel deletes the given overlapping SSTables from the level's linked list.
// It constructs a set of paths for O(1) lookups and iterates the level once, yielding O(N+M)
// complexity instead of O(N*M) with nested loops.
func (h *SSTableManager) removeOverlappingFromLevel(level *LinkedList[*FileSystem], overlapping []SStable) error {
	if len(overlapping) == 0 || level == nil {
		return nil
	}

	pathsToRemove := make(map[string]struct{}, len(overlapping))
	for _, sst := range overlapping {
		pathsToRemove[sst.Path()] = struct{}{}
	}

	iter := level.Iterator()
	for iter.HasNext() {
		fs, err := iter.Next()
		if err != nil {
			return err
		}
		if _, ok := pathsToRemove[fs.Path()]; ok {
			if err := iter.RemoveCurrent(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (h *SSTableManager) closeSSTables(sstables []SStable) {
	for i := range sstables {
		fs := sstables[i].FileSystem
		_ = sstables[i].Close()
		h.removeOpenedFS(fs)
	}
}

func (h *SSTableManager) removeOpenedFS(target *FileSystem) {
	for e := h.openedFs.Front(); e != nil; e = e.Next() {
		fs, ok := e.Value.(*FileSystem)
		if ok && fs == target {
			h.openedFs.Remove(e)
			return
		}
	}
}

// mergeSSTables merges a list of SSTables into a new SSTable at the specified level.
// Assumes the caller holds the necessary lock (e.g., h.mu.Lock()).
func (h *SSTableManager) mergeSSTables(ctx context.Context, newLevelNumb int, pickedUpSSTable []SStable) error {
	newLevelSSTable, err := h.NewSSTableFS(ctx, newLevelNumb)
	if err != nil {
		return err
	}

	// determine if the target level is the bottommost non-empty level
	bottommost := true
	for i := newLevelNumb + 1; i < len(h.levels); i++ {
		if h.levels[i] != nil && h.levels[i].Len() > 0 {
			bottommost = false
			break
		}
	}

	merged, err := mergeSSTablesV2(ctx, h.config, newLevelSSTable, pickedUpSSTable, bottommost, h.minSnapshotSeq)
	// close and remove merged sstables even if there is an error
	h.closeSSTables(pickedUpSSTable)
	if err != nil || merged == nil {
		_ = newLevelSSTable.Close()
		h.removeOpenedFS(newLevelSSTable)
		if rmErr := os.Remove(newLevelSSTable.Path()); rmErr != nil {
			if err == nil {
				ERROR(ctx, "Error removing empty file %s: %v", newLevelSSTable.Path(), rmErr)
			} else {
				WARN(ctx, "Failed to remove target file %s after merge error: %v", newLevelSSTable.Path(), rmErr)
			}
		}
		if err != nil {
			return err
		}
		return nil
	}

	if len(h.levels) == newLevelNumb {
		h.levels = append(h.levels, InitLinkedList[*FileSystem]())
	}
	h.levels[newLevelNumb].PushBack(newLevelSSTable)

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
					h.removeOpenedFS(fs)
				}
			}
		}
	}

	getRelevantSSTables.Add(ctx, int64(relevantSSTables.Len()))
	return relevantSSTables, nil
}

func (h *SSTableManager) searchKey(ctx context.Context, key Bytes, seq ...uint64) (Bytes, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	maxSeq := getMaxSeq(seq...)

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
			if _, getOffsetErr := sstable.SparseIndex.GetOffset(key); getOffsetErr != nil {
				closeErr := fs.Close()
				h.removeOpenedFS(fs)
				if errors.Is(getOffsetErr, ErrKeyNotFound) {
					if closeErr != nil {
						return nil, fmt.Errorf("key not found in sparse index, but failed to close sstable: %w", closeErr)
					}
				} else {
					return nil, errors.Join(getOffsetErr, closeErr)
				}
			} else {
				value, getErr := sstable.GetValue(ctx, key, maxSeq)
				closeErr := fs.Close()
				h.removeOpenedFS(fs)

				if getErr == nil {
					latestValue = value
					found = true
					if closeErr != nil {
						return nil, fmt.Errorf("value found but failed to close sstable: %w", closeErr)
					}
					break // Exit the loop for this level as we've found the key.
				}

				// If a tombstone is found, it's a definitive "not found" for this key.
				// Combine with closeErr if it occurred.
				if errors.Is(getErr, ErrTombstoneFound) {
					return nil, errors.Join(ErrKeyNotFound, closeErr)
				}

				// For other "key not found" errors, continue searching.
				// However, if a closeErr occurred, we must return it.
				if errors.Is(getErr, ErrKeyNotFound) {
					if closeErr != nil {
						return nil, fmt.Errorf("key not found in sstable, but failed to close: %w", closeErr)
					}
					// Otherwise, continue to the next sstable.
				} else {
					// A different error occurred during GetValue. Return it, combined with any closeErr.
					return nil, errors.Join(getErr, closeErr)
				}
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

			closeErr := fs.Close()
			ssTableManager.removeOpenedFS(fs)
			if closeErr != nil {
				return 0, closeErr
			}
		}
	}
	return maxSeqNum, nil
}

func getKeyRange(sstables []SStable) (Bytes, Bytes) {
	var (
		minKey, maxKey Bytes
		initialized    bool
	)
	for _, sst := range sstables {
		sstMin, sstMax := sst.GetKeyRange()
		if sstMin == nil || sstMax == nil {
			continue
		}
		if !initialized {
			minKey, maxKey = sstMin, sstMax
			initialized = true
			continue
		}
		if minKey.Compare(sstMin) > 0 {
			minKey = sstMin
		}
		if maxKey.Compare(sstMax) < 0 {
			maxKey = sstMax
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
	sstable, err := flush(ctx, config, memtable, target)
	if err != nil {
		return SStable{}, err
	}
	return sstable, nil
}

func mergeSSTablesV2(ctx context.Context, config Config, target *FileSystem, sources []SStable, bottommost bool, minSeq uint64) (_ *SStable, err error) {
	if len(sources) == 0 {
		return nil, nil
	}

	iterators := make([]Iterator[Record], 0, len(sources))
	for _, sstable := range sources {
		iter, err := sstable.Iterator()
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, iter)
	}

	mergeIter, err := NewMergingIterator(iterators, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := mergeIter.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	var (
		wrote      int
		lastKey    Bytes
		lastKeySet bool
		lastSeq    uint64
		memtable   = InitMemtable(config)
	)
	for mergeIter.HasNext() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rec, err := mergeIter.Next()
		if err != nil {
			return nil, err
		}

		key := rec.GetKey()
		if !lastKeySet || key.Compare(lastKey) != CmpEqual {
			lastKey = key.Clone()
			lastKeySet = true
		} else if lastSeq <= minSeq {
			continue
		}
		lastSeq = rec.GetSequenceNumber()

		if rec.GetType() == TypeDeletion && bottommost && rec.GetSequenceNumber() < minSeq {
			continue // GC tombstone only at bottommost when older than snapshots
		}

		memtable.Put(rec)
		wrote++
	}

	if wrote == 0 {
		// Nothing to write → no SST produced.
		return nil, nil
	}

	sstable, err := flush(ctx, config, memtable, target)
	if err != nil {
		return nil, err
	}
	return &sstable, nil
}
