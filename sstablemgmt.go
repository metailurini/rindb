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

func removeFiles(dir string, files []FileMeta) error {
	var first error
	for _, f := range files {
		p := filepath.Join(dir, sstPath(f.Number))
		if err := os.Remove(p); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if first == nil {
				first = err
			}
		}
	}
	return first
}

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
	openedByNum map[uint64]*SStable // open SSTables keyed by file number
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

func (h *SSTableManager) sstInfo(num uint64) (os.FileInfo, error) {
	return os.Stat(path.Join(h.config.databaseDir, sstPath(num)))
}

// openAndLoadSSTable opens a FileSystem and creates an SSTable object from it.
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
		if d.IsDir() || filepath.Ext(p) != sstExt {
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
		info, err := os.Stat(p)
		if err != nil {
			return err
		}
		metas = append(metas, FileMeta{
			Number:   num,
			Level:    0,
			Smallest: InternalKey{UserKey: lo},
			Largest:  InternalKey{UserKey: hi},
			Size:     uint64(info.Size()),
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
// and updates the in-memory VersionSet.
func (h *SSTableManager) AddSSTable(ctx context.Context, meta FileMeta, lastSeq uint64) error {
	ctx, span := sstableMgmtTracer.Start(ctx, "SSTableManager.AddSSTable")
	start := time.Now()
	defer func() {
		span.End()
		addSSTableLatency.Record(ctx, float64(time.Since(start).Milliseconds()))
		addSSTableCalls.Add(ctx, 1)
	}()

	if lastSeq == 0 {
		return fmt.Errorf("lastSeq must be greater than zero")
	}

	edit := VersionEdit{
		AddFiles:       []FileMeta{meta},
		NextFileNumber: h.config.fileNumberAllocator.Peek(),
		LastSequence:   lastSeq,
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
	INFO(ctx, "Registered new SSTable %s at level %d", path.Join(h.config.databaseDir, sstPath(meta.Number)), meta.Level)
	return nil
}

func (h *SSTableManager) NewSSTableFS(ctx context.Context, levelNumb int) (*FileSystem, error) {
	id := h.config.fileNumberAllocator.Next()
	sstableFileName := path.Join(h.config.databaseDir, sstPath(id))
	fs, err := OpenFS(ctx, sstableFileName)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func (h *SSTableManager) Close(ctx context.Context) {
	if h.stopIOLoadSampler != nil {
		close(h.stopIOLoadSampler)
		h.ioSamplerWG.Wait()
	}

	h.mu.Lock()
	sstablesToClose := make([]*SStable, 0, len(h.openedByNum))
	for _, s := range h.openedByNum {
		sstablesToClose = append(sstablesToClose, s)
	}
	h.openedByNum = make(map[uint64]*SStable)
	h.mu.Unlock()

	for _, s := range sstablesToClose {
		if err := s.Close(); err != nil {
			WARN(ctx, "Error closing sstable %s: %v", s.Path(), err)
		}
	}
}

func (h *SSTableManager) shouldCompact(ctx context.Context, levelNumb int, files []FileMeta) bool {
	if len(files) == 0 {
		return false
	}

	if h.dynamicTriggerHit() {
		return true
	}

	if levelNumb == 0 {
		return len(files) >= h.config.level0CompactionThreshold
	}

	var totalSize int64
	for _, f := range files {
		totalSize += int64(f.Size)
	}

	multiplier := h.config.levelSizeMultiplier
	if multiplier < 1 {
		WARN(ctx, "levelSizeMultiplier is %d, using 1 instead", multiplier)
		multiplier = 1
	}
	threshold := int64(h.config.baseCompactionSizeMB) * int64(math.Pow(float64(multiplier), float64(levelNumb))) * 1024 * 1024
	return totalSize >= threshold
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

	INFO(ctx, "Starting compaction check across %d levels", len(h.versionSet.Levels))

	for lvl, files := range h.versionSet.Levels {
		if !h.shouldCompact(ctx, lvl, files) {
			continue
		}
		picked := append([]FileMeta(nil), files...)
		if len(picked) == 0 {
			continue
		}
		overlaps, err := h.findOverlaps(ctx, lvl+1, picked)
		if err != nil {
			return err
		}
		if err := h.mergeIntoLevel(ctx, lvl+1, append(overlaps, picked...)); err != nil {
			return err
		}
	}
	return nil
}

func (h *SSTableManager) findOverlaps(ctx context.Context, level int, inputs []FileMeta) ([]FileMeta, error) {
	if level >= len(h.versionSet.Levels) {
		return nil, nil
	}
	files := h.versionSet.Levels[level]
	if len(files) == 0 || len(inputs) == 0 {
		return nil, nil
	}

	var (
		minKey Bytes
		maxKey Bytes
		first  = true
	)
	for _, f := range inputs {
		if len(f.Smallest.UserKey) == 0 && len(f.Largest.UserKey) == 0 {
			continue
		}
		if first {
			minKey = f.Smallest.UserKey
			maxKey = f.Largest.UserKey
			first = false
			continue
		}
		if f.Smallest.UserKey.Compare(minKey) < 0 {
			minKey = f.Smallest.UserKey
		}
		if f.Largest.UserKey.Compare(maxKey) > 0 {
			maxKey = f.Largest.UserKey
		}
	}
	if first {
		return nil, nil
	}
	var over []FileMeta
	for _, f := range files {
		if maxKey.Compare(f.Smallest.UserKey) >= 0 && minKey.Compare(f.Largest.UserKey) <= 0 {
			over = append(over, f)
		}
	}
	return over, nil
}

func (h *SSTableManager) mergeIntoLevel(ctx context.Context, dst int, inputs []FileMeta) error {
	if len(inputs) == 0 {
		return nil
	}

	var sources []SStable
	for _, fm := range inputs {
		fs := &FileSystem{filePath: path.Join(h.config.databaseDir, sstPath(fm.Number))}
		sst, err := h.openAndLoadSSTable(ctx, fs)
		if err != nil {
			h.closeSSTables(ctx, sources)
			return err
		}
		sources = append(sources, *sst)
	}

	newFS, err := h.NewSSTableFS(ctx, dst)
	if err != nil {
		h.closeSSTables(ctx, sources)
		return err
	}

	bottom := true
	if h.versionSet != nil {
		for i := dst + 1; i < len(h.versionSet.Levels); i++ {
			if len(h.versionSet.Levels[i]) > 0 {
				bottom = false
				break
			}
		}
	}

	merged, meta, err := mergeSSTablesV2(ctx, h.config, newFS, sources, bottom, h.minSnapshotSeq)
	h.closeSSTables(ctx, sources)
	if err != nil || merged == nil {
		_ = newFS.Close()
		if rmErr := os.Remove(newFS.Path()); rmErr != nil && err == nil {
			ERROR(ctx, "Error removing file %s: %v", newFS.Path(), rmErr)
		}
		return err
	}

	var (
		dels     []FileMeta
		delMetas []DeletedFileMeta
	)
	for _, fm := range inputs {
		dels = append(dels, FileMeta{Number: fm.Number})
		delMetas = append(delMetas, DeletedFileMeta{Level: fm.Level, Number: fm.Number})
	}

	meta.Level = dst
	edit := VersionEdit{AddFiles: []FileMeta{meta}, DeleteFiles: delMetas, NextFileNumber: h.config.fileNumberAllocator.Peek()}
	if h.manifest != nil {
		if err := h.manifest.Append(edit); err != nil {
			return err
		}
		if err := h.manifest.Sync(); err != nil {
			return err
		}
	}
	if h.versionSet != nil {
		if err := edit.Apply(h.versionSet); err != nil {
			return err
		}
	}
	h.config.fileNumberAllocator.Apply(edit)

	if err := removeFiles(h.config.databaseDir, dels); err != nil {
		ERROR(ctx, "Error removing files: %v", err)
		return err
	}
	return nil
}

func (h *SSTableManager) closeSSTables(ctx context.Context, sstables []SStable) {
	for i := range sstables {
		if err := sstables[i].Close(); err != nil {
			WARN(ctx, "Error closing sstable %s: %v", sstables[i].Path(), err)
		}
	}
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

// GetRelevantSSTables gathers SSTables whose ranges overlap [startKey, endKey].
// Level 0 files are returned in newest-first order while higher levels retain
// their existing ordering. If an SSTable referenced in the current version is
// missing on disk, the function returns the error so callers can retry with a
// fresh view.
func (h *SSTableManager) GetRelevantSSTables(ctx context.Context, startKey, endKey Bytes) ([]*SStable, error) {
	ctx, span := sstableMgmtTracer.Start(ctx, "SSTableManager.GetRelevantSSTables")
	start := time.Now()
	defer func() {
		span.End()
		getRelevantLatency.Record(ctx, float64(time.Since(start).Milliseconds()))
		getRelevantCalls.Add(ctx, 1)
	}()

	h.mu.RLock()
	nums := make([]uint64, 0)
	for lvl, files := range h.versionSet.Levels {
		if len(files) == 0 {
			continue
		}
		if lvl == 0 {
			for i := len(files) - 1; i >= 0; i-- {
				f := files[i]
				if endKey.Compare(f.Smallest.UserKey) >= 0 && startKey.Compare(f.Largest.UserKey) <= 0 {
					info, err := h.sstInfo(f.Number)
					if err != nil {
						h.mu.RUnlock()
						if errors.Is(err, os.ErrNotExist) {
							return nil, err
						}
						WARN(ctx, "Failed to stat SSTable %d: %v", f.Number, err)
						return nil, fmt.Errorf("failed to stat SSTable %d: %w", f.Number, err)
					}
					if info.Size() > 0 {
						nums = append(nums, f.Number)
					}
				}
			}
			continue
		}
		for _, f := range files {
			if endKey.Compare(f.Smallest.UserKey) >= 0 && startKey.Compare(f.Largest.UserKey) <= 0 {
				info, err := h.sstInfo(f.Number)
				if err != nil {
					h.mu.RUnlock()
					if errors.Is(err, os.ErrNotExist) {
						return nil, err
					}
					WARN(ctx, "Failed to stat SSTable %d: %v", f.Number, err)
					return nil, fmt.Errorf("failed to stat SSTable %d: %w", f.Number, err)
				}
				if info.Size() > 0 {
					nums = append(nums, f.Number)
				}
			}
		}
	}
	h.mu.RUnlock()

	out := make([]*SStable, 0, len(nums))
	for _, num := range nums {
		sst, err := h.openByNumber(ctx, num)
		if err != nil {
			for _, s := range out {
				_ = s.Close()
			}
			if errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
			WARN(ctx, "Failed to open SSTable %d: %v", num, err)
			return nil, fmt.Errorf("failed to open SSTable %d: %w", num, err)
		}
		out = append(out, sst)
	}
	getRelevantSSTables.Add(ctx, int64(len(out)))
	return out, nil
}

func (h *SSTableManager) searchKey(ctx context.Context, key Bytes, seq ...uint64) (Bytes, error) {
	maxSeq := getMaxSeq(seq...)

	// Compaction may remove SSTables while a lookup is in progress. If we
	// encounter a missing file, retry the search with a fresh view of the
	// levels to pick up the replacement SSTables. A small retry budget keeps
	// us from looping indefinitely in pathological cases.
	const maxRetries = 2

	for retries := 0; retries <= maxRetries; retries++ {
		ssts, err := h.GetRelevantSSTables(ctx, key, key)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// SSTable was removed, likely due to a concurrent compaction.
				continue
			}
			return nil, err
		}
		for _, sst := range ssts {
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
		return nil, ErrKeyNotFound
	}
	return nil, ErrKeyNotFound
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
