# Compactor Refactor Plan

The current `SSTableManager` embeds all compaction logic.  To align with the manifest
work and enable the `commit` workflow described in `overall_plan.md`, compaction will
be extracted into a dedicated `Compactor` type located in `compactor.go`.

## Type & Constructor
```go
// Low-level helpers supplied by SSTableManager.
type CompactionOps interface {
    ShouldCompact(ctx context.Context, level int, files []FileMeta) bool
    PickFiles(ctx context.Context, level int, files []FileMeta) []FileMeta
    FindOverlaps(ctx context.Context, level int, inputs []FileMeta) ([]FileMeta, error)
    // Merge writes a new set of SSTables for the destination level and returns
    // FileMeta populated with that level and allocator-assigned file numbers
    // (see allocator notes in overall_plan.md).
    Merge(ctx context.Context, level int, inputs []FileMeta) ([]FileMeta, error)
}

// Compactor orchestrates SSTable merges and manifest updates. It is not
// concurrency-safe; callers (e.g., SSTableManager) must hold their own locks
// around Compact to serialize access to VersionSet and filesystem state.
type Compactor struct {
    ops      CompactionOps
    manifest ManifestWriter
    version  *VersionSet
}

func NewCompactor(ops CompactionOps, mw ManifestWriter, vs *VersionSet) *Compactor {
    return &Compactor{ops: ops, manifest: mw, version: vs}
}
```

## Concurrency

`Compactor` relies on its caller for synchronization. `SSTableManager` holds its
`mu` while invoking `Compact` and throughout `commit`, serializing updates to the
`VersionSet` and physical file removals.

## Public Entry
```go
// Compact scans levels and continues dispatching work until no level qualifies.
// After each compaction the scan restarts from level 0 because compaction can
// alter which levels require work.
func (c *Compactor) Compact(ctx context.Context) error {
    for {
        progressed := false
        for lvl, files := range c.version.Levels {
            if !c.ops.ShouldCompact(ctx, lvl, files) {
                continue
            }
            if err := c.compactLevel(ctx, lvl, files); err != nil {
                return err
            }
            progressed = true
            break // restart scan with refreshed view of c.version.Levels
        }
        if !progressed {
            return nil
        }
    }
}
```

Breaking out of the inner loop avoids iterating over a stale snapshot of
`c.version.Levels`. Compaction at one level can change the size of adjacent
levels—for example, compacting level 0 into level 1 may push level 1 over its
threshold—so restarting the scan ensures each decision uses the latest state.

## Commit Hook
```go
// commit persists version edits and removes obsolete files.
func (c *Compactor) commit(ctx context.Context, outs, dels []FileMeta) error {
    last := maxSeq(outs)
    edit := VersionEdit{AddFiles: outs, LastSequence: last}
    for _, f := range dels {
        edit.DeleteFiles = append(edit.DeleteFiles, DeletedFileMeta{Level: f.Level, Number: f.Number})
    }
    if err := c.manifest.Append(edit); err != nil { return err }
    if err := c.manifest.Sync(); err != nil { return err }
    c.version.Apply(edit)
    return removeFiles(dels)
}
```

`removeFiles` deletes obsolete SSTable files from disk after the manifest edit is
durably persisted. A new helper (see `overall_plan.md`) provides this function
and is reused by the compactor.

## Internal Helpers
```go
func (c *Compactor) compactLevel(ctx context.Context, lvl int, files []FileMeta) error {
    picked := c.ops.PickFiles(ctx, lvl, files)
    if len(picked) == 0 { return nil }
    dst := lvl + 1
    over, err := c.ops.FindOverlaps(ctx, dst, picked)
    if err != nil { return err }
    outs, err := c.ops.Merge(ctx, dst, append(over, picked...))
    if err != nil { return err }
    return c.commit(ctx, outs, append(over, picked...))
}
```

## Manager Wiring
```go
type SSTableManager struct {
    versionSet *VersionSet
    compactor  *Compactor
    // ... existing fields ...
}

// SSTableManager implements CompactionOps via helpers such as
// ShouldCompact, PickFiles, FindOverlaps, and Merge.

func InitSSTableManager(ctx context.Context, cfg Config) (*SSTableManager, error) {
    vs := cfg.newVersionSetFunc()
    mgr := &SSTableManager{versionSet: vs, config: cfg}
    // ... existing initialization ...
    mw := cfg.newManifestFunc()
    mgr.compactor = NewCompactor(mgr, mw, vs)
    return mgr, nil
}
```

## Follow Ups
- Delete `Compact`, `compactLevel0`, and `compactHigherLevel` from `sstablemgmt.go`.
- Retain and rename helpers as `ShouldCompact`, `PickFiles`, `FindOverlaps`, and `Merge` on `SSTableManager`; update them to use `[]FileMeta` and `VersionSet` data.
- Adjust tests to construct the manager with a mock `ManifestWriter` and `VersionSet`.
- Extend `Config` with hooks for manifest and version constructors if not already present.
