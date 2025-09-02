# SSTable Manager Version-Based Refactor Plan

Legacy `SSTableManager` keeps per-level linked lists of `*FileSystem` while also
tracking the same data in `VersionSet`.  This plan removes the redundant lists
so that `VersionSet` becomes the sole source of truth.

## Type and Construction
```go
// levels field deleted; versionSet drives all metadata.
type SSTableManager struct {
    openedFsMu  sync.Mutex
    openedFs    map[uint64]*FileSystem
    openedByNum map[uint64]*SStable

    versionSet *VersionSet
    manifest   ManifestWriter
    config     Config
    mu         sync.RWMutex
    // snapshot + sampler fields unchanged
}

func InitSSTableManager(ctx context.Context, cfg Config, vs *VersionSet, mw ManifestWriter) (*SSTableManager, error) {
    if vs == nil && !cfg.repairMode {
        return nil, errors.New("version set required")
    }
    if cfg.repairMode {
        vs = &VersionSet{}
        var metas []FileMeta
        err := filepath.WalkDir(cfg.databaseDir, func(path string, d fs.DirEntry, err error) error {
            if err != nil { return err }
            if filepath.Ext(path) != ".sst" { return nil }
            num, err := fileNum(path)
            if err != nil { return err }
            fs, err := OpenExistingFS(ctx, path)
            if err != nil { return err }
            defer fs.Close()
            sst, err := NewSSTable(ctx, cfg, fs)
            if err != nil { return err }
            defer sst.Close()
            lo, hi := sst.GetKeyRange()
            seqHi, err := sst.MaxSequenceNumber()
            if err != nil { return err }
            metas = append(metas, FileMeta{
                Number:   num,
                Level:    0,
                Smallest: InternalKey{UserKey: lo},
                Largest:  InternalKey{UserKey: hi},
                SeqHi:    seqHi,
            })
            return nil
        })
        if err != nil { return nil, err }
        vs.Levels = [][]FileMeta{metas}
    }
    return &SSTableManager{
        openedFs:    make(map[uint64]*FileSystem),
        openedByNum: make(map[uint64]*SStable),
        versionSet:  vs,
        manifest:    mw,
        config:      cfg,
        stopIOLoadSampler: make(chan struct{}),
        now:               time.Now,
        minSnapshotSeq:    math.MaxUint64,
        diskSampler: func() (uint64, error) { /* unchanged */ },
    }, nil
}
```

The previous repair-mode `LoadLevels` function is removed; the directory scan above populates `VersionSet` directly. Each SSTable
is opened just long enough to pull its metadata and then closed so recovery does not exhaust descriptors. The metadata is gathered
into a slice and assigned to `vs.Levels` (all files start in Level 0), mirroring the layout a healthy manifest would have produced.

## SSTable Registration
```go
func (h *SSTableManager) AddSSTable(ctx context.Context, meta FileMeta) error {
    edit := VersionEdit{
        AddFiles:       []FileMeta{meta},
        NextFileNumber: h.config.fileNumberAllocator.Peek(),
    }
    if h.manifest != nil {
        if err := h.manifest.Append(edit); err != nil { return err }
        if err := h.manifest.Sync(); err != nil { return err }
    }
    h.mu.Lock() // lock only to update in-memory versionSet
    defer h.mu.Unlock()
    if err := edit.Apply(h.versionSet); err != nil {
        return err
    }
    h.config.fileNumberAllocator.Apply(edit) // keep allocator consistent with manifest
    return nil
}
```

AddSSTable first appends a `VersionEdit` to the manifest so the on-disk log matches memory even if the process crashes. The mutex
only guards the in-memory `versionSet` mutation and allocator update; readers proceed concurrently. `NextFileNumber` keeps the allocator in sync with
files registered in the manifest.

## Compaction Flow
```go
func (h *SSTableManager) Compact(ctx context.Context) error {
    for lvl := range h.versionSet.Levels {
        h.mu.RLock()
        files := h.versionSet.Levels[lvl]
        if !h.shouldCompact(ctx, lvl, files) {
            h.mu.RUnlock()
            continue
        }
        var nums []uint64
        if lvl == 0 {
            for _, f := range files { nums = append(nums, f.Number) }
        } else {
            nums = []uint64{files[0].Number}
        }
        h.mu.RUnlock()

        picked, err := h.pickFiles(ctx, lvl, nums)
        if err != nil { return err }
        overlaps, err := h.findOverlaps(ctx, lvl+1, picked)
        if err != nil { return err }
        metas, err := h.merge(ctx, lvl+1, append(picked, overlaps...))
        if err != nil { return err }
        edit := VersionEdit{
            AddFiles:    metas,
            DeleteFiles: toDeleted(append(picked, overlaps...)),
            NextFileNumber: h.config.fileNumberAllocator.Peek(),
        }
        if h.manifest != nil {
            if err := h.manifest.Append(edit); err != nil { return err }
            if err := h.manifest.Sync(); err != nil { return err }
        }
        func() {
            h.mu.Lock()
            defer h.mu.Unlock()
            if err = edit.Apply(h.versionSet); err == nil {
                h.config.fileNumberAllocator.Apply(edit)
            }
        }()
        if err != nil { return err }
    }
    return nil
}

func (h *SSTableManager) shouldCompact(ctx context.Context, level int, files []FileMeta) bool {
    if len(files) == 0 { return false }
    if h.dynamicTriggerHit() { return true }
    if level == 0 {
        return len(files) >= h.config.level0CompactionThreshold
    }
    var total uint64
    for _, f := range files { total += f.Size }
    mult := h.config.levelSizeMultiplier
    if mult < 1 { mult = 1 }
    threshold := uint64(float64(h.config.baseCompactionSizeMB) * math.Pow(float64(mult), float64(level)) * 1024 * 1024)
    return total >= threshold
}

func (h *SSTableManager) pickFiles(ctx context.Context, level int, nums []uint64) ([]FileMeta, error) {
    h.mu.Lock()
    defer h.mu.Unlock()
    files := h.versionSet.Levels[level]
    picked := make([]FileMeta, 0, len(nums))
    remaining := files[:0]
    want := make(map[uint64]struct{}, len(nums))
    for _, n := range nums { want[n] = struct{}{} }
    for _, fm := range files {
        if _, ok := want[fm.Number]; ok {
            picked = append(picked, fm)
        } else {
            remaining = append(remaining, fm)
        }
    }
    h.versionSet.Levels[level] = remaining
    return picked, nil
}

func (h *SSTableManager) findOverlaps(ctx context.Context, level int, inputs []FileMeta) ([]FileMeta, error) {
    if level >= len(h.versionSet.Levels) { return nil, nil }
    min, max := keyRange(inputs) // compute combined key span
    var overlaps []FileMeta
    for _, fm := range h.versionSet.Levels[level] {
        if fm.Smallest.Compare(max) <= 0 && fm.Largest.Compare(min) >= 0 {
            overlaps = append(overlaps, fm)
        }
    }
    return overlaps, nil
}

func (h *SSTableManager) merge(ctx context.Context, level int, inputs []FileMeta) ([]FileMeta, error) {
    tbls := make([]SStable, 0, len(inputs))
    for _, fm := range inputs {
        sst, err := h.openByNumber(ctx, fm.Number)
        if err != nil {
            h.closeSSTables(ctx, tbls)
            return nil, err
        }
        tbls = append(tbls, *sst)
    }
    out, err := h.NewSSTableFS(ctx, level)
    if err != nil {
        h.closeSSTables(ctx, tbls)
        return nil, err
    }
    bottom := level == len(h.versionSet.Levels)-1
    _, meta, err := mergeSSTablesV2(ctx, h.config, out, tbls, bottom, h.minSnapshotSeq)
    h.closeSSTables(ctx, tbls)
    if err != nil {
        _ = out.Close()
        return nil, err
    }
    meta.Level = level
    return []FileMeta{meta}, out.Close()
}

func toDeleted(files []FileMeta) []DeletedFileMeta {
    dels := make([]DeletedFileMeta, len(files))
    for i, f := range files {
        dels[i] = DeletedFileMeta{Level: f.Level, Number: f.Number}
    }
    return dels
}
```
Helpers resolve `FileMeta.Number` to a `FileSystem` lazily; no `levels` linked lists remain. Legacy helpers `compactLevel0`, `compactHigherLevel`, `findOverlappingSSTables`, and `removeOverlappingFromLevel` are removed. `shouldCompact` mirrors current thresholds—Level 0 triggers on file count while higher levels use cumulative size. `pickFiles` removes chosen file numbers from `VersionSet` (all files at Level 0 or the first file at higher levels) so they are not compacted twice; `findOverlaps` compares key spans in the next level, `toDeleted` converts them to `DeletedFileMeta`, and `merge` opens each SSTable by file number before writing the merged output.

## Sequence Number Scan
```go
func getMaxSequenceNumber(ctx context.Context, vs *VersionSet, wal *WAL) (uint64, error) {
    maxSeq := vs.LastSequence
    mem, err := wal.Load(ctx)
    if err != nil { return 0, err }
    walSeq, err := getMaxSequenceNumberFromMemtable(mem)
    if err != nil { return 0, err }
    return max(maxSeq, walSeq), nil
}
```

`getMaxSequenceNumberFromSSTables` and its Level‑0 scan are deleted; `InitRinDB` invokes this helper instead.

```go
func InitRinDB(ctx context.Context, cfg Config) (*Rindb, error) {
    vs, err := RecoverVersionSet(ctx, cfg.databaseDir, cfg.fileNumberAllocator)
    if err != nil { return nil, err }
    wal, err := cfg.newWALFunc(ctx, cfg)
    if err != nil { return nil, err }
    seq, err := getMaxSequenceNumber(ctx, vs, wal)
    if err != nil { return nil, err }
    db := &Rindb{sequenceNumber: seq, versionSet: vs, wal: wal}
    return db, nil
}
```

During startup the database first recovers the `VersionSet` from the manifest and then opens the WAL. `getMaxSequenceNumber`
loads the WAL and compares its highest sequence with the manifest's `LastSequence`, avoiding an SSTable scan. The resulting value seeds
`Rindb.sequenceNumber` so new writes continue the sequence monotonically after recovery.

## Stats Gathering
```go
func (r *Rindb) Stats() Stats {
    stats := Stats{ /* atomics omitted */ }
    r.mu.RLock()
    stats.MemtableBytes = r.memtable.ByteSize()
    stats.SequenceNumber = r.sequenceNumber
    stats.ActiveSnapshots = len(r.activeSnapshots)
    r.mu.RUnlock()

    r.ssTableManager.mu.RLock()
    stats.SSTablesPerLevel = make([]int, len(r.ssTableManager.versionSet.Levels))
    for i, files := range r.ssTableManager.versionSet.Levels {
        stats.SSTablesPerLevel[i] = len(files)
    }
    r.ssTableManager.mu.RUnlock()
    return stats
}
```

These stats read directly from `versionSet` rather than iterating linked lists. The approach leaves SSTable internals untouched
and makes it easier to surface new metrics atop immutable metadata.

## Test Adjustments
```go
type testRindbSetup struct {
    T       *testing.T
    Config  *Config
    RinDB   *Rindb
    Manager *SSTableManager
    Version *VersionSet
    // ... other fields
}

func newTestRindbSetup(t *testing.T, ctx context.Context, cfg *Config) *testRindbSetup {
    // InitRinDB as today
    return &testRindbSetup{
        T: t, Config: &finalCfg, RinDB: db,
        Manager: db.ssTableManager, Version: db.versionSet,
    }
}

func (ts *testRindbSetup) AddSSTable(meta FileMeta) {
    require.NoError(ts.T, ts.Manager.AddSSTable(context.Background(), meta))
}

func (ts *testRindbSetup) createSSTable(level int, kv map[string]string) (FileMeta, *SStable) {
    fs, err := ts.Manager.NewSSTableFS(context.Background(), level)
    require.NoError(ts.T, err)

    pairs := make([][2]Bytes, 0, len(kv))
    for k, v := range kv {
        pairs = append(pairs, [2]Bytes{Bytes(k), Bytes(v)})
    }
    sst, meta, err := flush(context.Background(), *ts.Config, populateMemtable(*ts.Config, pairs...), fs)
    require.NoError(ts.T, err)
    meta.Level = level
    return meta, &sst
}
```
Tests in `sstablemgmt_test.go` and helpers in `utils_test.go` seed files via `AddSSTable` and assert against
`Manager.versionSet.Levels`. Other tests such as `rindb_test.go` and `range_test.go` drop `AddSSTableToLevel` and direct
`manager.levels` access. The helper carries the opened database via `RinDB`, builds SSTables through `createSSTable`, registers
them, and lets tests verify placement through the public API only.
