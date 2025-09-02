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
        entries := map[int][]FileMeta{}
        err := filepath.WalkDir(cfg.databaseDir, func(path string, d fs.DirEntry, err error) error {
            if filepath.Ext(path) != ".sst" { return nil }
            fs, err := OpenFileSystem(path)
            if err != nil { return err }
            meta, err := loadFileMeta(fs) // reads size and key/seq ranges
            if err != nil { return err }
            entries[meta.Level] = append(entries[meta.Level], meta)
            return fs.Close()
        })
        if err != nil { return nil, err }
        vs.Levels = entries
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

The previous repair-mode `LoadLevels` function is removed; the directory scan above populates `VersionSet` directly. Each SSTable
is opened just long enough to pull its metadata and then closed so recovery does not exhaust descriptors. The metadata is placed
into `vs.Levels` keyed by level number, yielding the same layout as a healthy manifest would have produced.

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

AddSSTable first appends a `VersionEdit` to the manifest so the on-disk log matches memory even if the process crashes. The mutex
only guards the in-memory `versionSet` mutation; readers proceed concurrently. `NextFileNumber` keeps the allocator in sync with
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

func (h *SSTableManager) shouldCompact(ctx context.Context, level int, files []FileMeta) bool {
    limit := h.config.Levels[level].MaxFileCount
    return len(files) > limit
}

func (h *SSTableManager) pickFiles(ctx context.Context, level int, files []FileMeta) []FileMeta {
    sort.Slice(files, func(i, j int) bool { return files[i].Size < files[j].Size })
    need := h.config.Levels[level].TargetFileCount
    if len(files) < need { return files }
    return append([]FileMeta(nil), files[:need]...)
}

func (h *SSTableManager) findOverlaps(ctx context.Context, level int, inputs []FileMeta) ([]FileMeta, error) {
    var overlaps []FileMeta
    for _, fm := range h.versionSet.Levels[level] {
        if overlapsRange(inputs, fm) { // overlapsRange compares key ranges
            overlaps = append(overlaps, fm)
        }
    }
    return overlaps, nil
}

func (h *SSTableManager) merge(ctx context.Context, level int, inputs []FileMeta) ([]FileMeta, error) {
    out, err := h.NewSSTableFS(ctx, level)
    if err != nil { return nil, err }
    // merge logic elided; write to `out` and build meta
    meta := FileMeta{Number: out.FileNumber(), Level: level}
    return []FileMeta{meta}, out.Close()
}
```
Helpers resolve `FileMeta.Number` to a `FileSystem` lazily; no `levels` linked lists remain. Legacy helpers `compactLevel0`, `compactHigherLevel`, `findOverlappingSSTables`, and `removeOverlappingFromLevel` are removed. `shouldCompact` compares the file count against configured limits. `pickFiles` favors smaller tables, `findOverlaps` scans the next level for intersecting ranges, and `merge` writes the new table for the destination level.

## Sequence Number Scan
```go
func getMaxSequenceNumber(ctx context.Context, vs *VersionSet, wal *WAL) (uint64, error) {
    maxSeq := vs.LastSequence
    walSeq, err := wal.MaxSequenceNumber()
    if err != nil { return 0, err }
    return max(maxSeq, walSeq), nil
}
```

`getMaxSequenceNumberFromSSTables` and its Level‑0 scan are deleted; `InitRinDB` invokes this helper instead.

```go
func InitRinDB(ctx context.Context, cfg Config) (*Rindb, error) {
    vs := &VersionSet{}
    wal, err := OpenWAL(cfg.databaseDir)
    if err != nil { return nil, err }
    seq, err := getMaxSequenceNumber(ctx, vs, wal)
    if err != nil { return nil, err }
    db := &Rindb{sequenceNumber: seq, versionSet: vs, wal: wal}
    return db, nil
}
```

During startup the database pulls the greater sequence number from the manifest or WAL, avoiding a Level‑0 scan. The resulting
value seeds `Rindb.sequenceNumber` so new writes continue the sequence monotonically after recovery.

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
`manager.levels` access. The helper builds SSTables through `createSSTable`, registers them, and lets tests verify placement
through the public API only.
