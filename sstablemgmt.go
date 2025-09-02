package rindb

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

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
	openedFsMu  sync.Mutex
	openedFs    map[uint64]*FileSystem     // legacy tracking for compaction paths
	openedByNum map[uint64]*SStable        // open SSTables keyed by file number
	levels      []*LinkedList[*FileSystem] // temporary shim; metadata lives in versionSet
	versionSet  *VersionSet
	manifest    ManifestWriter
	config      Config
	mu          sync.RWMutex

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
	num, err := fileNum(fs.Path())
	if err != nil {
		_ = fs.Close()
		return nil, fmt.Errorf("invalid sstable path %s: %w", fs.Path(), err)
	}
	h.openedFsMu.Lock()
	h.openedFs[num] = fs // Track opened file system
	h.openedFsMu.Unlock()
	return &sstable, nil
}

// buildVersionSetFromDisk scans the database directory for existing SSTable
// files and constructs a VersionSet based on their metadata. Only files with
// the `.sst` extension and valid numeric file numbers are considered. Each
// discovered SSTable is opened to determine its key range and maximum sequence
// number. The returned VersionSet contains a single level populated with the
// collected metadata.
func buildVersionSetFromDisk(ctx context.Context, cfg Config) (*VersionSet, error) {
	var metas []FileMeta
	err := filepath.WalkDir(cfg.databaseDir, func(p string, d iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(p) != ".sst" {
			return nil
		}
		num, err := fileNum(p)
		if err != nil {
			return err
		}
		fs, err := OpenExistingFS(ctx, p)
		if err != nil {
			return err
		}
		sst, err := NewSSTable(ctx, cfg, fs)
		if err != nil {
			_ = fs.Close()
			return err
		}
		lo, hi := sst.GetKeyRange()
		seqHi, err := sst.MaxSequenceNumber()
		closeErr := sst.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		metas = append(metas, FileMeta{
			Number:   num,
			Level:    0,
			Smallest: InternalKey{UserKey: lo},
			Largest:  InternalKey{UserKey: hi},
			SeqHi:    seqHi,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &VersionSet{Levels: [][]FileMeta{metas}}, nil
}

func InitSSTableManager(ctx context.Context, config Config, vs *VersionSet, mw ManifestWriter) (*SSTableManager, error) {
	if vs == nil && !config.repairMode {
		return nil, errors.New("rindb: version set cannot be nil in non-repair mode")
	}

	if config.repairMode {
		var err error
		vs, err = buildVersionSetFromDisk(ctx, config)
		if err != nil {
			return nil, err
		}
	}

	h := &SSTableManager{
		openedFs:          make(map[uint64]*FileSystem),
		openedByNum:       make(map[uint64]*SStable),
		versionSet:        vs,
		manifest:          mw,
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

	if vs != nil {
		levels := make([]*LinkedList[*FileSystem], len(vs.Levels))
		for lvl, files := range vs.Levels {
			if len(files) == 0 {
				continue
			}
			ll := InitLinkedList[*FileSystem]()
			for _, f := range files {
				fs := &FileSystem{filePath: path.Join(config.databaseDir, sstPath(f.Number))}
				ll.PushBack(fs)
			}
			levels[lvl] = ll
		}
		h.levels = levels
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

// AddSSTable registers a new SSTable's metadata, persists it to the manifest,
// and updates the in-memory VersionSet. It keeps the legacy `levels` list in
// sync until all call sites migrate to VersionSet usage only.
func (h *SSTableManager) AddSSTable(ctx context.Context, meta FileMeta) error {
	ctx, span := sstableMgmtTracer.Start(ctx, "SSTableManager.AddSSTable")
	start := time.Now()
	defer func() {
		span.End()
		addSSTableLatency.Record(ctx, float64(time.Since(start).Milliseconds()))
		addSSTableCalls.Add(ctx, 1)
	}()

	edit := VersionEdit{
		AddFiles:       []FileMeta{meta},
		NextFileNumber: h.config.fileNumberAllocator.Peek(),
	}
	if h.manifest != nil {
		if err := h.manifest.Append(edit); err != nil {
			return err
		}
		if err := h.manifest.Sync(); err != nil {
			return err
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if err := edit.Apply(h.versionSet); err != nil {
		return err
	}
	h.config.fileNumberAllocator.Apply(edit)

	// Maintain legacy levels list for existing compaction code paths.
	for len(h.levels) <= meta.Level {
		h.levels = append(h.levels, nil)
	}
	if h.levels[meta.Level] == nil {
		h.levels[meta.Level] = InitLinkedList[*FileSystem]()
	}
	fs := &FileSystem{filePath: path.Join(h.config.databaseDir, sstPath(meta.Number))}
	h.levels[meta.Level].PushBack(fs)
	INFO(ctx, "Registered new SSTable %s at level %d", fs.Path(), meta.Level)
	return nil
}

func (h *SSTableManager) NewSSTableFS(ctx context.Context, levelNumb int) (*FileSystem, error) {
	id := h.config.fileNumberAllocator.Next()
	sstableFileName := path.Join(h.config.databaseDir, sstPath(id))
	fs, err := OpenFS(ctx, sstableFileName)
	if err != nil {
		return nil, err
	}

	h.openedFsMu.Lock()
	h.openedFs[id] = fs
	h.openedFsMu.Unlock()
	return fs, nil
}

func (h *SSTableManager) Close(ctx context.Context) {
	if h.stopIOLoadSampler != nil {
		close(h.stopIOLoadSampler)
		h.ioSamplerWG.Wait()
	}

	h.mu.Lock()
	h.openedFsMu.Lock()
	sstablesToClose := make([]*SStable, 0, len(h.openedByNum))
	for _, s := range h.openedByNum {
		sstablesToClose = append(sstablesToClose, s)
	}
	h.openedByNum = make(map[uint64]*SStable)
	filesToClose := make([]*FileSystem, 0, len(h.openedFs))
	for _, fs := range h.openedFs {
		filesToClose = append(filesToClose, fs)
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
			num, nerr := fileNum(fs.Path())
			alreadyOpened := false
			if nerr == nil {
				_, alreadyOpened = h.openedFs[num]
			}
			if alreadyOpened {
				continue
			}
			filesToClose = append(filesToClose, fs)
		}
	}
	h.openedFs = make(map[uint64]*FileSystem)
	h.openedFsMu.Unlock()
	h.mu.Unlock()

	for _, s := range sstablesToClose {
		if err := s.Close(); err != nil {
			WARN(ctx, "Error closing sstable %s: %v", s.Path(), err)
		}
	}
	for _, fs := range filesToClose {
		if !fs.IsOpened() {
			WARN(ctx, "File %s is already closed", fs.Path())
			continue
		}
		if err := fs.Close(); err != nil {
			ERROR(ctx, "Error closing file %s: %v", fs.Path(), err)
		} else {
			INFO(ctx, "Closed %s successfully", fs.Path())
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
			h.closeSSTables(ctx, sstablesToMerge)
			return fmt.Errorf("error picking next SSTable from level: %w", err)
		}
		sstable, err := h.openAndLoadSSTable(ctx, fs)
		if err != nil {
			// Attempt to close any already opened SSTables before returning error
			h.closeSSTables(ctx, sstablesToMerge)
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
			h.closeSSTables(ctx, overlapping)
			return nil, err
		}
		sstable, err := h.openAndLoadSSTable(ctx, fs)
		if err != nil {
			h.closeSSTables(ctx, overlapping)
			return nil, err
		}
		if sstable.Overlaps(minKey, maxKey) {
			overlapping = append(overlapping, *sstable)
		} else {
			_ = fs.Close() // Close if not overlapping
			if err := h.removeOpenedFS(fs); err != nil {
				WARN(ctx, "Failed to remove opened file %s: %v", fs.Path(), err)
			}
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

func (h *SSTableManager) closeSSTables(ctx context.Context, sstables []SStable) {
	for i := range sstables {
		fs := sstables[i].FileSystem
		_ = sstables[i].Close()
		if err := h.removeOpenedFS(fs); err != nil {
			WARN(ctx, "Failed to remove opened file %s: %v", fs.Path(), err)
		}
	}
}

func (h *SSTableManager) removeOpenedFS(target *FileSystem) error {
	num, err := fileNum(target.Path())
	if err != nil {
		return err
	}
	h.openedFsMu.Lock()
	delete(h.openedFs, num)
	h.openedFsMu.Unlock()
	return nil
}

// openByNumber returns an opened SSTable for the given file number. The
// SSTable is cached so repeated lookups reuse the same handle.
func (h *SSTableManager) openByNumber(ctx context.Context, num uint64) (*SStable, error) {
	h.mu.RLock()
	if sst, ok := h.openedByNum[num]; ok {
		h.mu.RUnlock()
		return sst, nil
	}
	h.mu.RUnlock()

	h.mu.Lock()
	defer h.mu.Unlock()
	if sst, ok := h.openedByNum[num]; ok {
		return sst, nil
	}

	path := path.Join(h.config.databaseDir, sstPath(num))
	fs, err := OpenExistingFS(ctx, path)
	if err != nil {
		return nil, err
	}
	sst, err := NewSSTable(ctx, h.config, fs)
	if err != nil {
		_ = fs.Close()
		return nil, err
	}
	h.openedByNum[num] = &sst
	return &sst, nil
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

	merged, meta, err := mergeSSTablesV2(ctx, h.config, newLevelSSTable, pickedUpSSTable, bottommost, h.minSnapshotSeq)
	// close and remove merged sstables even if there is an error
	h.closeSSTables(ctx, pickedUpSSTable)
	if err != nil || merged == nil {
		_ = newLevelSSTable.Close()
		if rmErr := h.removeOpenedFS(newLevelSSTable); rmErr != nil {
			WARN(ctx, "Failed to remove opened file %s: %v", newLevelSSTable.Path(), rmErr)
		}
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

	var (
		dels     []FileMeta
		delMetas []DeletedFileMeta
	)
	for _, sstable := range pickedUpSSTable {
		num, nerr := fileNum(sstable.Path())
		if nerr != nil {
			ERROR(ctx, "Error parsing file number for %s: %v", sstable.Path(), nerr)
			continue
		}
		dels = append(dels, FileMeta{Number: num})
		level := h.findLevel(num)
		if level >= 0 {
			delMetas = append(delMetas, DeletedFileMeta{Level: level, Number: num})
		}
	}

	meta.Level = newLevelNumb
	edit := VersionEdit{
		AddFiles:       []FileMeta{meta},
		DeleteFiles:    delMetas,
		NextFileNumber: h.config.fileNumberAllocator.Peek(),
	}
	if h.manifest != nil {
		if err := h.manifest.Append(edit); err != nil {
			return err
		}
		if err := h.manifest.Sync(); err != nil {
			return err
		}
	}
	if err := edit.Apply(h.versionSet); err != nil {
		return err
	}
	h.config.fileNumberAllocator.Apply(edit)

	if err := removeFiles(h.config.databaseDir, dels); err != nil {
		ERROR(ctx, "Error removing files: %v", err)
	}

	return nil
}

// GetRelevantSSTables gathers file numbers whose ranges overlap [startKey, endKey].
// Level 0 files are returned in newest-first order while higher levels retain
// their existing ordering.
func (h *SSTableManager) GetRelevantSSTables(ctx context.Context, startKey, endKey Bytes) []uint64 {
	ctx, span := sstableMgmtTracer.Start(ctx, "SSTableManager.GetRelevantSSTables")
	start := time.Now()
	defer func() {
		span.End()
		getRelevantLatency.Record(ctx, float64(time.Since(start).Milliseconds()))
		getRelevantCalls.Add(ctx, 1)
	}()

	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.versionSet == nil {
		return nil
	}

	var out []uint64
	for lvl, files := range h.versionSet.Levels {
		if len(files) == 0 {
			continue
		}
		if lvl == 0 {
			for i := len(files) - 1; i >= 0; i-- {
				f := files[i]
				if endKey.Compare(f.Smallest.UserKey) >= 0 && startKey.Compare(f.Largest.UserKey) <= 0 {
					p := path.Join(h.config.databaseDir, sstPath(f.Number))
					info, err := os.Stat(p)
					if err == nil && info.Size() > 0 {
						out = append(out, f.Number)
					}
				}
			}
			continue
		}
		for _, f := range files {
			if endKey.Compare(f.Smallest.UserKey) >= 0 && startKey.Compare(f.Largest.UserKey) <= 0 {
				p := path.Join(h.config.databaseDir, sstPath(f.Number))
				info, err := os.Stat(p)
				if err == nil && info.Size() > 0 {
					out = append(out, f.Number)
				}
			}
		}
	}
	getRelevantSSTables.Add(ctx, int64(len(out)))
	return out
}

func (h *SSTableManager) findLevel(num uint64) int {
	if h.versionSet == nil {
		return -1
	}
	for lvl, files := range h.versionSet.Levels {
		for _, f := range files {
			if f.Number == num {
				return lvl
			}
		}
	}
	return -1
}

func (h *SSTableManager) searchKey(ctx context.Context, key Bytes, seq ...uint64) (Bytes, error) {
	maxSeq := getMaxSeq(seq...)

	// Compaction may remove SSTables while a lookup is in progress. If we
	// encounter a missing file, retry the search with a fresh view of the
	// levels to pick up the replacement SSTables. A small retry budget keeps
	// us from looping indefinitely in pathological cases.
	const maxRetries = 2

	for retries := 0; retries <= maxRetries; retries++ {
		nums := h.GetRelevantSSTables(ctx, key, key)
		missing := false
		for _, num := range nums {
			sst, err := h.openByNumber(ctx, num)
			if err != nil {
				WARN(ctx, "Failed to open SSTable %d: %v", num, err)
				if errors.Is(err, os.ErrNotExist) {
					// SSTable was removed, likely due to a concurrent compaction.
					missing = true
					break
				}
				continue
			}
			val, err := sst.GetValue(ctx, key, maxSeq)
			if err == nil {
				return val, nil
			}
			if errors.Is(err, ErrTombstoneFound) {
				return nil, ErrKeyNotFound
			}
			if !errors.Is(err, ErrKeyNotFound) {
				return nil, err
			}
		}
		if !missing {
			return nil, ErrKeyNotFound
		}
	}
	return nil, ErrKeyNotFound
}

// TODO: This is a temporary approach that only scans L0 SSTables.
// We should scan *all* levels to find the true max sequence number.
// The correct long-term solution is to maintain a MANIFEST file
// that tracks global sequence number metadata across all levels.
func getMaxSequenceNumberFromSSTables(ctx context.Context, ssTableManager *SSTableManager) (uint64, error) {
	var maxSeqNum uint64
	if ssTableManager.versionSet == nil || len(ssTableManager.versionSet.Levels) == 0 {
		return 0, nil
	}
	for _, meta := range ssTableManager.versionSet.Levels[0] {
		fs := &FileSystem{filePath: path.Join(ssTableManager.config.databaseDir, sstPath(meta.Number))}
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
		if rmErr := ssTableManager.removeOpenedFS(fs); rmErr != nil {
			WARN(ctx, "Failed to remove opened file %s: %v", fs.Path(), rmErr)
		}
		if closeErr != nil {
			return 0, closeErr
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

func mergeSSTablesV2(ctx context.Context, config Config, target *FileSystem, sources []SStable, bottommost bool, minSeq uint64) (_ *SStable, meta FileMeta, err error) {
	if len(sources) == 0 {
		return nil, FileMeta{}, nil
	}

	iterators := make([]Iterator[Record], 0, len(sources))
	for _, sstable := range sources {
		iter, err := sstable.Iterator()
		if err != nil {
			return nil, FileMeta{}, err
		}
		iterators = append(iterators, iter)
	}

	mergeIter, err := NewMergingIterator(iterators, nil)
	if err != nil {
		return nil, FileMeta{}, err
	}
	defer func() {
		if cerr := mergeIter.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	expected := 0
	for _, sstable := range sources {
		expected += len(sstable.SparseIndex)
	}

	builder, err := NewSSTableBuilder(ctx, config, target, expected)
	if err != nil {
		return nil, FileMeta{}, err
	}
	defer builder.Close(ctx)

	var (
		wrote      int
		lastKey    Bytes
		lastKeySet bool
		skipRest   bool
	)
	for mergeIter.HasNext() {
		if err := ctx.Err(); err != nil {
			return nil, FileMeta{}, err
		}
		rec, err := mergeIter.Next()
		if err != nil {
			return nil, FileMeta{}, err
		}
		key := rec.GetKey()
		if !lastKeySet || key.Compare(lastKey) != CmpEqual {
			lastKey = key.Clone()
			lastKeySet = true
			skipRest = false
		} else if skipRest {
			continue
		}

		seq := rec.GetSequenceNumber()
		if seq <= minSeq {
			if rec.GetType() == TypeDeletion && bottommost && seq < minSeq {
				skipRest = true
				continue // GC tombstone only at bottommost when older than snapshots
			}
			skipRest = true
		}

		if err := builder.Add(rec); err != nil {
			return nil, FileMeta{}, err
		}
		wrote++
	}

	if wrote == 0 {
		// Nothing to write → no SST produced.
		return nil, FileMeta{}, nil
	}

	sst, meta, _, err := builder.Build(ctx)
	if err != nil {
		return nil, FileMeta{}, err
	}
	return &sst, meta, nil
}
