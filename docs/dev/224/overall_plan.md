# Plan: New SSTable Layout with Fixed Footer

## Overview
- Replace trailing sparse index offset with 48-byte footer.
- Footer stores index block offset, size, padding, and magic number `201867972885`.
- Data block area and index block area remain contiguous before footer.
- Reader and builder logic will be updated to handle new layout.

## Implementation Steps
1. **Introduce footer structure and constants** for size and magic value.
2. **Write index block size and offset** in `SSTableBuilder.Build` before appending footer.
3. **Parse footer in `NewSSTable`**, validating index offset and size before loading the sparse index.
4. **Adjust tests** in `sstable_builder_test.go`, `sstable_test.go`, and any others that rely on the old tail offset.
5. **Handle padding** explicitly to maintain fixed footer size.

## Code Sketches

```go
// sstable.go
const (
    footerSize  = 48
    magicNumber = uint64(201867972885)
)

// footer layout: |8 index offset|8 index size|24 padding|8 magic|
type footer struct {
    indexOffset uint64
    indexSize   uint64
    _           [24]byte
    magic       uint64
}

func writeFooter(tx *transaction, f footer) error {
    buf := make([]byte, footerSize)
    byteOrder.PutUint64(buf[0:8], f.indexOffset)
    byteOrder.PutUint64(buf[8:16], f.indexSize)
    byteOrder.PutUint64(buf[40:48], f.magic)
    _, err := tx.write(buf)
    return err
}
```

```go
// sstable_builder.go
func (b *SSTableBuilder) Build(ctx context.Context) (SStable, fileMeta, int64, error) {
    // ... write records ...
    indexOffset := b.tx.size()
    for _, ko := range b.index {
        if err := writeKeyOffset(b.tx, ko); err != nil { return ... }
    }
    indexSize := b.tx.size() - indexOffset
    if err := writeFooter(b.tx, footer{
        indexOffset: uint64(indexOffset),
        indexSize:   uint64(indexSize),
        magic:       magicNumber,
    }); err != nil { return ... }
    // commit and return
}
```

```go
// sstable.go
func NewSSTable(ctx context.Context, cfg Config, fs *FileSystem) (SStable, error) {
    info, err := os.Stat(fs.Path())
    if err != nil { return SStable{}, err }
    tail := info.Size() - footerSize
    f, err := readFooter(fs, tail)
    if err != nil { return SStable{}, err }
    if f.indexOffset+f.indexSize > uint64(tail) {
        return SStable{}, ErrMalFormedSSTable
    }
    sidx, err := loadSparseIndex(fs, int64(f.indexOffset), int64(f.indexSize))
    if err != nil { return SStable{}, err }
    _ = sidx // build bloom
    return SStable{}, nil
}

```

```go
// sstable.go
func loadSparseIndex(fs *FileSystem, off, size int64) (SparseIndex, error) {
    reader := newOffsetReader(fs, off)
    limit := off + size
    var sidx SparseIndex
    for reader.Offset() < limit {
        ko, err := readKeyOffset(reader)
        if err != nil { return nil, err }
        sidx = append(sidx, ko)
    }
    return sidx, nil
}
```

```go
// sstable.go
func readFooter(fs *FileSystem, off int64) (footer, error) {
    var f footer
    buf := make([]byte, footerSize)
    if _, err := fs.ReadAt(buf, off); err != nil { return f, err }
    f.indexOffset = byteOrder.Uint64(buf[0:8])
    f.indexSize = byteOrder.Uint64(buf[8:16])
    f.magic = byteOrder.Uint64(buf[40:48])
    if f.magic != magicNumber {
        return f, ErrMalFormedSSTable
    }
    return f, nil
}
```

```go
// sstable_test.go
func TestFooterRoundTrip(t *testing.T) {
    tx := newMockTx()
    want := footer{indexOffset: 10, indexSize: 20, magic: magicNumber}
    err := writeFooter(tx, want)
    require.NoError(t, err)
    got, err := readFooter(tx.fs, 0)
    require.NoError(t, err)
    assert.Equal(t, want, got)
}
```
