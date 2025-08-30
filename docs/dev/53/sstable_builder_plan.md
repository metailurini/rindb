# SSTableBuilder Metadata Plan

Emit `FileMeta` during table construction so flush and compaction can write manifest edits.

## Plan
- Track smallest/largest keys, sequence bounds, and total bytes in `Add`.
- `Build` takes a file number and returns an `SSTable` plus a populated `FileMeta`.
- Callers assign the target level on the `FileMeta` before creating a `VersionEdit`.
- Tests assert that key bounds, size, and sequence range are recorded.

## Example
```go
// FileMeta records table statistics.
type FileMeta struct {
    Number         uint64
    Level          int
    Smallest, Largest InternalKey
    SeqLo, SeqHi   uint64
    Size           uint64
}

type SSTableBuilder struct {
    w               io.Writer
    smallest, largest InternalKey
    seqLo, seqHi    uint64
    written         uint64
}

func (b *SSTableBuilder) Add(ik InternalKey, val []byte) error {
    // update bounds and sequence range
}

func (b *SSTableBuilder) Build(fileNum uint64) (*SSTable, FileMeta, error) {
    meta := FileMeta{FileNum: fileNum, Smallest: b.smallest, Largest: b.largest,
        SeqLo: b.seqLo, SeqHi: b.seqHi, Size: b.written}
    return &SSTable{FileNum: fileNum}, meta, nil
}
```

