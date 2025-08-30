# Compactor Refactor Plan

The current `SSTableManager` embeds all compaction logic.  To align with the manifest
work and enable the `commit` workflow described in `overall_plan.md`, compaction will
be extracted into a dedicated `Compactor` type located in `compactor.go`.

## Type & Constructor
```go
// Compactor orchestrates SSTable merges and manifest updates.
type Compactor struct {
    mgr      *SSTableManager
    manifest ManifestWriter
    version  *VersionSet
}

func NewCompactor(m *SSTableManager, mw ManifestWriter, vs *VersionSet) *Compactor {
    return &Compactor{mgr: m, manifest: mw, version: vs}
}
```

## Public Entry
```go
// Compact scans levels and dispatches work just like
// the former SSTableManager.Compact.
func (c *Compactor) Compact(ctx context.Context) error {
    for lvl, ll := range c.mgr.levels {
        if ll == nil { continue }
        if !c.mgr.shouldCompact(ctx, lvl, ll) { continue }
        nxt := lvl + 1
        if lvl == 0 {
            if err := c.compactLevel0(ctx, ll, nxt); err != nil { return err }
        } else {
            if err := c.compactHigherLevel(ctx, ll, nxt); err != nil { return err }
        }
    }
    return nil
}
```

## Commit Hook
```go
// commit persists version edits and removes obsolete files.
func (c *Compactor) commit(ctx context.Context, outs, dels []FileMeta) error {
    edit := VersionEdit{AddFiles: outs}
    for _, f := range dels {
        edit.DeleteFiles = append(edit.DeleteFiles, struct{Level int; Number uint64}{f.Level, f.Number})
    }
    if err := c.manifest.Append(edit); err != nil { return err }
    if err := c.manifest.Sync(); err != nil { return err }
    c.version.Apply(edit)
    return removeFiles(dels)
}
```

## Internal Helpers
```go
func (c *Compactor) compactLevel0(ctx context.Context, src *LinkedList[*FileSystem], dst int) error {
    picked := c.mgr.pickAll(ctx, src)
    over, err := c.mgr.findOverlappingSSTables(ctx, dst, picked)
    if err != nil { return err }
    outs, err := c.mgr.mergeSSTables(ctx, dst, append(over, picked...))
    if err != nil { return err }
    return c.commit(ctx, outs, append(over, picked...))
}

func (c *Compactor) compactHigherLevel(ctx context.Context, src *LinkedList[*FileSystem], dst int) error {
    picked := c.mgr.pickAll(ctx, src)
    if len(picked) == 0 { return nil }
    over, err := c.mgr.findOverlappingSSTables(ctx, dst, picked)
    if err != nil { return err }
    outs, err := c.mgr.mergeSSTables(ctx, dst, append(over, picked...))
    if err != nil { return err }
    return c.commit(ctx, outs, append(over, picked...))
}
```

## Manager Wiring
```go
type SSTableManager struct {
    levels []*LinkedList[*FileSystem]
    compactor *Compactor
    // ... existing fields ...
}

func InitSSTableManager(ctx context.Context, cfg Config) (*SSTableManager, error) {
    mgr := &SSTableManager{levels: make([]*LinkedList[*FileSystem], 0), config: cfg}
    // ... existing initialization ...
    mw := cfg.newManifestFunc()
    vs := cfg.newVersionSetFunc()
    mgr.compactor = NewCompactor(mgr, mw, vs)
    return mgr, nil
}
```

## Follow Ups
- Remove old compaction methods from `sstablemgmt.go` and delegate to `Compactor`.
- Adjust tests to construct the manager with a mock `ManifestWriter` and `VersionSet`.
- Extend `Config` with hooks for manifest and version constructors if not already present.
