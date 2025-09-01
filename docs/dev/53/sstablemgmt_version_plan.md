# SSTable Manager Version-Based Refactor Plan

Legacy `SSTableManager` keeps per-level linked lists of `*FileSystem` while also
tracking the same data in `VersionSet`.  This plan removes the redundant lists
so that `VersionSet` becomes the sole source of truth.

## Type and Construction
```go
// levels field deleted; versionSet drives all metadata.
type SSTableManager struct {
    openedFsMu  sync.Mutex
    openedFs    map[*FileSystem]struct{}
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
    }
    return &SSTableManager{
        openedFs:    make(map[*FileSystem]struct{}),
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
    h.mu.Lock()
    defer h.mu.Unlock()
    return edit.Apply(h.versionSet)
}
```

## Compaction Flow
```go
func (h *SSTableManager) Compact(ctx context.Context) error {
    h.mu.Lock()
    defer h.mu.Unlock()
    for lvl, files := range h.versionSet.Levels {
        if !h.shouldCompact(ctx, lvl, files) { continue }
        picked := h.pickFiles(ctx, lvl, files)
        overlaps, err := h.findOverlaps(ctx, lvl+1, picked)
        if err != nil { return err }
        metas, err := h.merge(ctx, lvl+1, append(picked, overlaps...))
        if err != nil { return err }
        edit := VersionEdit{
            AddFiles:    metas,
            DeleteFiles: metasOf(append(picked, overlaps...)),
            NextFileNumber: h.config.fileNumberAllocator.Peek(),
        }
        if err := h.commit(edit); err != nil { return err }
    }
    return nil
}

func (h *SSTableManager) shouldCompact(ctx context.Context, level int, files []FileMeta) bool
func (h *SSTableManager) pickFiles(ctx context.Context, level int, files []FileMeta) []FileMeta
func (h *SSTableManager) findOverlaps(ctx context.Context, level int, inputs []FileMeta) ([]FileMeta, error)
func (h *SSTableManager) merge(ctx context.Context, level int, inputs []FileMeta) ([]FileMeta, error)

func (h *SSTableManager) commit(edit VersionEdit) error {
    if h.manifest != nil {
        if err := h.manifest.Append(edit); err != nil { return err }
        if err := h.manifest.Sync(); err != nil { return err }
    }
    return edit.Apply(h.versionSet)
}
```
Helpers resolve `FileMeta.Number` to a `FileSystem` lazily; no `levels` linked lists remain.

## Sequence Number Scan
```go
func getMaxSequenceNumberFromSSTables(ctx context.Context, mgr *SSTableManager) (uint64, error) {
    var maxSeq uint64
    for _, f := range mgr.versionSet.Levels[0] {
        sst, err := mgr.openByNumber(ctx, f.Number)
        if err != nil { return 0, err }
        seq, err := sst.MaxSequenceNumber()
        if err != nil { return 0, err }
        maxSeq = max(maxSeq, seq)
    }
    return maxSeq, nil
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
    fs, _ := ts.Manager.NewSSTableFS(context.Background(), level)
    sst, meta, _ := flush(context.Background(), *ts.Config, populateMemtable(*ts.Config, /* kv */), fs)
    meta.Level = level
    return meta, &sst
}
```
Tests in `sstablemgmt_test.go` and helpers in `utils_test.go` seed files via
`AddSSTable` and assert against `Manager.versionSet.Levels`.
