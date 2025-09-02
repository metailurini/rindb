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
        // scan cfg.databaseDir and populate vs.Levels with FileMeta for each *.sst
        // open each table and read its metadata (key ranges, seq nums, file size)
        // to build a proper FileMeta entry for recovery
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
    return edit.Apply(h.versionSet)
}
```

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
        picked := h.pickFiles(ctx, lvl, files)
        h.mu.RUnlock()

        overlaps, err := h.findOverlaps(ctx, lvl+1, picked)
        if err != nil { return err }
        metas, err := h.merge(ctx, lvl+1, append(picked, overlaps...))
        if err != nil { return err }
        edit := VersionEdit{
            AddFiles:    metas,
            DeleteFiles: metasOf(append(picked, overlaps...)),
            NextFileNumber: h.config.fileNumberAllocator.Peek(),
        }
        if h.manifest != nil {
            if err := h.manifest.Append(edit); err != nil { return err }
            if err := h.manifest.Sync(); err != nil { return err }
        }
        func() {
            h.mu.Lock(); defer h.mu.Unlock(); err = edit.Apply(h.versionSet)
        }()
        if err != nil { return err }
    }
    return nil
}

func (h *SSTableManager) shouldCompact(ctx context.Context, level int, files []FileMeta) bool
func (h *SSTableManager) pickFiles(ctx context.Context, level int, files []FileMeta) []FileMeta
func (h *SSTableManager) findOverlaps(ctx context.Context, level int, inputs []FileMeta) ([]FileMeta, error)
func (h *SSTableManager) merge(ctx context.Context, level int, inputs []FileMeta) ([]FileMeta, error)
```
Helpers resolve `FileMeta.Number` to a `FileSystem` lazily; no `levels` linked lists remain.

## Sequence Number Scan
```go
func getMaxSequenceNumber(ctx context.Context, vs *VersionSet, wal *WAL) (uint64, error) {
    maxSeq := vs.LastSequence
    walSeq, err := wal.MaxSequenceNumber()
    if err != nil { return 0, err }
    return max(maxSeq, walSeq), nil
}
```

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

## Test Adjustments
```go
type testRindbSetup struct {
    T       *testing.T
    Config  *Config
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
Tests in `sstablemgmt_test.go` and helpers in `utils_test.go` seed files via
`AddSSTable` and assert against `Manager.versionSet.Levels`.
