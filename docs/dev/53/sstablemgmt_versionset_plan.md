# SSTable Manager VersionSet Refactor Plan

Refactor `SSTableManager` to discard the legacy linked-list based level tracking and rely solely on `VersionSet` metadata.

## Remove `levels`
- Drop `levels []*LinkedList[*FileSystem]`.
- Access live SSTables through `versionSet.Levels` only.
```go
// Legacy field deleted; versionSet is the authoritative view.
type SSTableManager struct {
    openedFsMu  sync.Mutex
    openedFs    map[*FileSystem]struct{}
    openedByNum map[uint64]*SStable
    versionSet  *VersionSet
    manifest    ManifestWriter
    config      Config
    mu          sync.RWMutex
    // minSnapshotSeq, writeRate, ...
}
```

## Initialization & AddSSTable
- `InitSSTableManager` no longer hydrates linked lists from `VersionSet`.
- `AddSSTable` takes a `FileMeta` and persists it via `VersionEdit`.
```go
func InitSSTableManager(ctx context.Context, cfg Config, vs *VersionSet, mw ManifestWriter) (*SSTableManager, error) {
    mgr := &SSTableManager{
        openedFs:          make(map[*FileSystem]struct{}),
        openedByNum:       make(map[uint64]*SStable),
        versionSet:        vs,
        manifest:          mw,
        config:            cfg,
        stopIOLoadSampler: make(chan struct{}),
        now:               time.Now,
        minSnapshotSeq:    math.MaxUint64,
        diskSampler:       defaultDiskSampler,
    }
    mgr.ioSamplerWG.Add(1)
    go mgr.startIOLoadSampler()
    return mgr, nil
}

func (h *SSTableManager) AddSSTable(ctx context.Context, meta FileMeta, fs *FileSystem) error {
    edit := VersionEdit{
        AddFiles:       []FileMeta{meta},
        NextFileNumber: h.config.fileNumberAllocator.Peek(),
    }
    if h.manifest != nil {
        if err := h.manifest.Append(edit); err != nil { return err }
        if err := h.manifest.Sync(); err != nil { return err }
    }
    return edit.Apply(h.versionSet)
}
```

## Compaction Helpers
- Source files come from `versionSet.Levels` slices.
- Overlap discovery and merging operate on `[]FileMeta`.
- Replace in-place linked list mutation with `VersionEdit` updates.
```go
func (h *SSTableManager) compactLevel(ctx context.Context, lvl int) error {
    src := h.versionSet.Levels[lvl]
    dst := lvl + 1
    over, err := h.findOverlaps(ctx, dst, src)
    if err != nil { return err }
    outs, err := h.merge(ctx, dst, append(over, src...))
    if err != nil { return err }
    dels := append(over, src...)
    return h.commit(ctx, outs, dels)
}

func (h *SSTableManager) findOverlaps(ctx context.Context, lvl int, inputs []FileMeta) ([]FileMeta, error) {
    if lvl >= len(h.versionSet.Levels) { return nil, nil }
    min, max := keyRange(inputs)
    var out []FileMeta
    for _, f := range h.versionSet.Levels[lvl] {
        if max.Compare(f.Smallest.UserKey) >= 0 && min.Compare(f.Largest.UserKey) <= 0 {
            out = append(out, f)
        }
    }
    return out, nil
}

func (h *SSTableManager) commit(ctx context.Context, outs, dels []FileMeta) error {
    edit := VersionEdit{AddFiles: outs}
    for _, f := range dels {
        edit.DeleteFiles = append(edit.DeleteFiles, DeletedFileMeta{Level: f.Level, Number: f.Number})
    }
    if h.manifest != nil {
        if err := h.manifest.Append(edit); err != nil { return err }
        if err := h.manifest.Sync(); err != nil { return err }
    }
    if err := edit.Apply(h.versionSet); err != nil { return err }
    return removeFiles(h.config.databaseDir, dels)
}
```

## Stats Gathering
- Replace direct `levels` inspection with `versionSet` lookups.
```go
func (r *Rindb) Stats() Stats {
    // ... existing atomic reads ...
    r.ssTableManager.mu.RLock()
    stats.SSTablesPerLevel = make([]int, len(r.ssTableManager.versionSet.Levels))
    for i, files := range r.ssTableManager.versionSet.Levels {
        stats.SSTablesPerLevel[i] = len(files)
    }
    r.ssTableManager.mu.RUnlock()
    return stats
}
```

The startup helper `getMaxSequenceNumberFromSSTables` also scans `versionSet.Levels` rather than `levels`.
```go
func getMaxSequenceNumberFromSSTables(ctx context.Context, mgr *SSTableManager) (uint64, error) {
    var max uint64
    for _, lvl := range mgr.versionSet.Levels {
        for _, meta := range lvl {
            sst, err := mgr.openByNumber(ctx, meta.Number)
            if err != nil { continue }
            seq, err := sst.MaxSequenceNumber()
            if err != nil { return 0, err }
            max = maxUint64(max, seq)
        }
    }
    return max, nil
}
```

## Test Adjustments
- `testRindbSetup` drops `Levels` and seeds `VersionSet` with edits.
- Helpers create `FileMeta` from generated SSTables and apply edits via `versionSet`.
```go
func (ts *testRindbSetup) AddSSTable(meta FileMeta) {
    edit := VersionEdit{AddFiles: []FileMeta{meta}}
    assert.NoError(ts.T, edit.Apply(ts.Manager.versionSet))
}

func newTestRindbSetup(t *testing.T, ctx context.Context, cfg *Config) *testRindbSetup {
    // ... create DB ...
    return &testRindbSetup{T: t, Config: &finalCfg, RinDB: db, Manager: manager}
}
```

Tests in `sstablemgmt_test.go` expect file membership and counts via `versionSet.Levels` and open SSTables through `openByNumber`.
```go
func TestCompactionRewritesVersion(t *testing.T) {
    ts := newTestRindbSetup(t, ctx, &cfg)
    meta := FileMeta{Number: 1, Level: 0, Smallest: k1, Largest: k2}
    ts.AddSSTable(meta)
    assert.Equal(t, []FileMeta{meta}, ts.Manager.versionSet.Levels[0])
}
```

## Summary
This plan removes the linked-list level abstraction from `SSTableManager` and makes `VersionSet` the single source of truth. All metadata mutations flow through `VersionEdit`, compactions operate on `[]FileMeta`, and tests validate state via `VersionSet`.
