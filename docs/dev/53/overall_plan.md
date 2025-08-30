# Manifest and Versioning Plan

This document outlines how to integrate a MANIFEST-based versioning system into `rindb`.  The plan below balances prose and illustrative code snippets at roughly a 3:7 ratio.

## Data Structures
- Introduce in-memory `VersionSet` as the authoritative mapping from levels to files.
- Use `VersionEdit` deltas to record structural changes.
```go
// Describes a new or updated state change.
type VersionEdit struct {
    ComparatorName *string
    LastSequence   *uint64
    NextFileNumber *uint64
    LogNumber      *uint64
    PrevLogNumber  *uint64
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

func WriteCURRENT(dir, manifest string) error {
    tmp := filepath.Join(dir, "CURRENT.tmp")
    if err := os.WriteFile(tmp, []byte(manifest+"\n"), 0o644); err != nil { return err }
    if err := os.Rename(tmp, filepath.Join(dir, "CURRENT")); err != nil { return err }
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
    if err := WriteCURRENT(dir, w.Path()); err != nil {
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
    vs, err := recoverVersionSet(cfg.databaseDir)
    if err != nil { return nil, err }
    return &SSTableManager{versionSet: vs, config: cfg}, nil
}
```

## Codebase Changes

### `sstablemgmt.go`
- Replace the current level list with a `VersionSet` reference.
```go
type SSTableManager struct {
    openedFs map[*FileSystem]struct{}
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

### `rindb.go`
- `InitRinDB` will replay the manifest instead of scanning SSTables for sequence numbers.
```go
ssTableManager, err := cfg.newSSTableManagerFunc(ctx, cfg)
sstMaxSeqNum, err := getMaxSequenceNumberFromSSTables(ctx, ssTableManager)
```
becomes
```go
vs, err := recoverVersionSet(cfg.databaseDir)
ssTableManager := NewSSTableManager(vs, cfg)
// sequence derived during WAL replay
```

### `wal.go`
- `DefaultNewWALFunc` uses a fixed "WAL" file; switch to numbered logs via `FileNumberAllocator` and persist `LogNumber`/`PrevLogNumber` in the manifest.
```go
walPath := path.Join(cfg.databaseDir, "WAL")
```
changes to
```go
id := allocator.Next()
walPath := path.Join(cfg.databaseDir, fmt.Sprintf("%06d.wal", id))
```

### `sstablemgmt.go` `NewSSTableFS`
- Replace ULID-based file names with allocator-issued numbers.
```go
uid := ulid.Make()
sstableFileName := fmt.Sprintf("l%02d_%s.sst", levelNumb, uid)
```
becomes
```go
id := allocator.Next()
sstableFileName := fmt.Sprintf("%06d.sst", id)
```

### `config.go`
- Extend `Config` with constructors for the manifest and file-number allocator (e.g., `newManifestFunc`, `newFileNumberAllocator`).

## Next Steps
- Implement manifest writer/reader packages.
- Integrate allocator and version set into existing DB paths.
- Add integration tests covering crash recovery and rotation.
