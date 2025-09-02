# Manifest and Versioning Plan

This document tracks ongoing work after landing a MANIFEST-based versioning system in `rindb`.  It balances prose and illustrative code snippets at roughly a 3:7 ratio.

## Data Structures
- Introduce in-memory `VersionSet` as the authoritative mapping from levels to files.
- Use `VersionEdit` deltas to record structural changes.
- Record the highest sequence in `VersionSet.LastSequence` to avoid startup scans.
```go
// Describes a new or updated state change.
type VersionEdit struct {
    ComparatorName string
    LastSequence   uint64
    NextFileNumber uint64
    LogNumber      uint64
    PrevLogNumber  uint64
    AddFiles    []FileMeta
    DeleteFiles []DeletedFileMeta
}

// Live SSTable metadata.
type FileMeta struct {
    Number   uint64
    Level    int
    Smallest InternalKey
    Largest  InternalKey
    Size     uint64
    SeqLo    uint64
    SeqHi    uint64
}

type DeletedFileMeta struct {
    Level  int
    Number uint64
}

// In-memory snapshot of all levels.
type VersionSet struct {
    Levels        [][]FileMeta
    Comparator    string
    NextFileNumber uint64
    LogNumber     uint64
    PrevLogNumber uint64
    LastSequence  uint64
}
```

## Manifest IO & CURRENT management
- Append `VersionEdit` records to a MANIFEST with checksum framing.
- Maintain a `CURRENT` file pointing to the active MANIFEST using atomic rename.
```go
// Writer appends edits and fsyncs for durability.
type ManifestWriter interface {
    Append(VersionEdit) error
    Sync() error
    Close() error
}

// Reader replays edits during recovery.
type ManifestReader interface {
    Next() (VersionEdit, error)
    Close() error
}

func WriteCURRENT(ctx context.Context, dir, manifest string) error {
    tmp := filepath.Join(dir, "CURRENT.tmp")
    fs, err := OpenFS(ctx, tmp)
    if err != nil { return err }
    if _, err := fs.Write([]byte(manifest+"\n")); err != nil {
        _ = fs.Close()
        _ = os.Remove(tmp)
        return err
    }
    if err := fs.Sync(); err != nil {
        _ = fs.Close()
        _ = os.Remove(tmp)
        return err
    }
    if err := fs.Rename(filepath.Join(dir, "CURRENT")); err != nil {
        _ = os.Remove(tmp)
        return err
    }
    return syncDir(dir)
}
```

## Recovery Flow
- Read `CURRENT`, replay MANIFEST into a fresh `VersionSet`, then apply WALs.
- Comparator mismatches should abort startup.
```go
func (db *DB) recover(ctx context.Context) error {
    mf := readCURRENT(db.dir)
    r := openManifestReader(mf)
    vs := &VersionSet{}
    for {
        edit, err := r.Next()
        if err == io.EOF {
            break
        }
        if err != nil {
            return err
        }
        if err := edit.Apply(vs); err != nil {
            return err
        }
    }
    return replayWALs(ctx, vs)
}
```

## Compaction and File Lifecycle
- Compactions emit atomic `VersionEdit` batches with `DeleteFile` + `AddFile` pairs.
- Physical file removal occurs only after the edit is synced.
```go
func (c *Compactor) commit(ctx context.Context, outs []FileMeta, dels []FileMeta) error {
    edit := VersionEdit{AddFiles: outs}
    for _, f := range dels {
        edit.DeleteFiles = append(edit.DeleteFiles, DeletedFileMeta{Level: f.Level, Number: f.Number})
    }
    if err := c.manifest.Append(edit); err != nil { return err }
    if err := c.manifest.Sync(); err != nil { return err }
    c.version.Apply(edit)
    return removeFiles(dels)
}
```

`removeFiles` deletes the physical SSTable files referenced by `dels` after the
manifest edit has been synced, ensuring obsolete files are reclaimed only after
the new state is durable.

## Rotation and Checkpointing
- When the MANIFEST grows beyond a threshold, write a new snapshot manifest and swap `CURRENT`.
```go
func (vs *VersionSet) SnapshotEdit() VersionEdit {
    // Serialize entire state as a single edit.
}

func rotateManifest(dir string, vs *VersionSet) error {
    snap := vs.SnapshotEdit()
    w := newManifestWriter(newPath())
    if err := w.Append(snap); err != nil {
        w.Close()
        return err
    }
    if err := w.Sync(); err != nil {
        w.Close()
        return err
    }
    if err := WriteCURRENT(ctx, dir, w.Path()); err != nil {
        w.Close()
        return err
    }
    return w.Close()
}
```

## Allocator Integration
- Replace ad-hoc file numbering with a persistent allocator.
```go
type FileNumberAllocator struct{ next atomic.Uint64 }

func (a *FileNumberAllocator) Next() uint64 {
    return a.next.Add(1) - 1
}

func (a *FileNumberAllocator) Apply(edit VersionEdit) {
    if edit.NextFileNumber != nil {
        a.next.Store(*edit.NextFileNumber)
    }
}
```

## Replacing Directory Scans
- Current `SSTableManager.LoadLevels` scans directories; after MANIFEST integration this becomes a repair-only path.
```go
func InitSSTableManager(ctx context.Context, cfg Config) (*SSTableManager, error) {
    if cfg.repairMode { return loadByScan(ctx, cfg) }
    vs, err := recoverVersionSet(ctx, cfg.databaseDir)
    if err != nil { return nil, err }
    return &SSTableManager{versionSet: vs, config: cfg}, nil
}
```

## Codebase Changes

### `sstablemgmt.go`
- Replace the current level list with a `VersionSet` reference.
```go
type SSTableManager struct {
    openedFs map[uint64]*FileSystem
-   levels   []*LinkedList[*FileSystem]
    config   Config
    mu       sync.RWMutex
+   versionSet *VersionSet // replaces levels as the source of truth
    // ...
}
```
- `LoadLevels` presently walks the filesystem with `os.ReadDir`; keep it only for `repairMode`.
```go
func (h *SSTableManager) LoadLevels(dir string) error {
    dirEntries, err := os.ReadDir(dir)
    // ...
}
```
- Update `AddSSTable`, `Compact`, `compactLevel0`, `compactHigherLevel`,
  and `mergeSSTables` to emit `VersionEdit` records and apply them to `versionSet`.

- Update unit tests to construct `VersionSet` fixtures instead of populating
  `levels` lists.

### `rindb.go`
- `InitRinDB` now bootstraps from the MANIFEST, which dictates both live SSTables and WAL identifiers.
- Flow:
  1. `recoverVersionSet` reads `CURRENT` and replays the manifest to rebuild levels and allocator state.
  2. Seed a `FileNumberAllocator` and WAL plumbing from `vs.NextFileNumber`, `vs.LogNumber`, and `vs.PrevLogNumber`.
  3. Replay the WAL files referenced by those log numbers to restore the memtable and determine `lastSeq`.
  4. Construct `SSTableManager` directly from the recovered `VersionSet`—no directory scan.
```go
func InitRinDB(ctx context.Context, opts ...Option) (*Rindb, error) {
    cfg := NewConfig(opts...)

    vs, err := recoverVersionSet(ctx, cfg.databaseDir)
    if err != nil { return nil, err }

    allocator := cfg.newFileNumberAllocatorFunc(vs.NextFileNumber)
    wal, mem, err := openAndReplayWALs(ctx, cfg, allocator, vs.LogNumber, vs.PrevLogNumber, vs.LastSequence)
    if err != nil { return nil, err }

    mgr := NewSSTableManager(vs, cfg)
    return &Rindb{
        wal:            wal,
        memtable:       mem,
        ssTableManager: mgr,
        sequenceNumber: vs.LastSequence,
    }, nil
}
```

### `filepaths.go`
- Centralize numbered file naming for WALs and SSTables.
```go
func walPath(num uint64) string { return fmt.Sprintf("%06d.wal", num) }
func sstPath(num uint64) string { return fmt.Sprintf("%06d.sst", num) }
```
- Callers use these helpers with numbers from `FileNumberAllocator`.

### `wal.go`
- `DefaultNewWALFunc` uses a fixed "WAL" file; switch to numbered logs via `FileNumberAllocator` and persist `LogNumber`/`PrevLogNumber` in the manifest.
```go
walPath := path.Join(cfg.databaseDir, "WAL")
```
changes to
```go
id := allocator.Next()
walPath := path.Join(cfg.databaseDir, walPath(id))
```

- Update WAL tests to expect numbered log files seeded by the allocator.

### `sstablemgmt.go` `NewSSTableFS`
- Replace ULID-based file names with allocator-issued numbers.
```go
uid := ulid.Make()
sstableFileName := fmt.Sprintf("l%02d_%s.sst", levelNumb, uid)
```
becomes
```go
id := allocator.Next()
sstableFileName := sstPath(id)
```

- Tests should seed the allocator to generate predictable file names.

### `config.go`
- Extend `Config` with constructors for the manifest and file-number allocator (e.g., `newManifestFunc`, `newFileNumberAllocator`).
- Add a `repairMode` flag and option to force directory scans.
```go
type Config struct {
    databaseDir string
    repairMode  bool
    // ... existing fields ...
}

func WithRepairMode(v bool) Option {
    return func(c *Config) { c.repairMode = v }
}
```

### `sstable_builder.go`
- Track `Smallest`, `Largest`, `SeqLo`, `SeqHi`, and total bytes during `Add`.
- `Build` returns the `SSTable` and a `FileMeta` populated with the allocator-issued file number.
- Callers assign the level and forward the metadata when constructing `VersionEdit`s.
- Update `mergeSSTables` and existing tests to handle the new `FileMeta` result.

### Memtable Flush Path
- After flushing the memtable, create a `VersionEdit` containing the returned `FileMeta` and updated sequence.
```go
meta := FileMeta{Number: id, Level: 0, Smallest: s, Largest: l, SeqLo: lo, SeqHi: hi}
last := meta.SeqHi
edit := VersionEdit{AddFiles: []FileMeta{meta}, LastSequence: last}
if err := mw.Append(edit); err != nil { return err }
if err := mw.Sync(); err != nil { return err }
vs.Apply(edit)
```
- Append through a `ManifestWriter`, `Sync`, apply it to the `VersionSet`, then clear the memtable and obsolete WAL.
- Extend tests to verify the flush records the file in `VersionSet` and cleans up the WAL using that metadata.

### `sstable_files.go`
- Introduce `removeFiles(files []FileMeta)` to map file numbers to paths and delete them once the manifest edit is durable.
- Replace ad-hoc file deletions in flush and compaction paths with this helper.
- Cover deletion and error cases with unit tests.

## Next Steps
- Integrate allocator and version set into existing DB paths.
- Add integration tests covering crash recovery and rotation.
