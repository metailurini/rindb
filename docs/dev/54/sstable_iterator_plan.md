# Bidirectional SSTable iterator plan

This document expands step 3 of the reverse range scanning plan and outlines how to walk an SSTable in both directions without materializing all records.

1. Push the file offset of each returned record onto a stack. `Prev` pops the stack and seeks to that offset.
2. When the iterator reaches a new block, preload its footer and remember the previous block's starting offset to allow rewinding.
3. Extend `sstableIterator` and `sstableIRange` to share the stack and expose `HasPrev`/`Prev` while staying compatible with the table cache.

```go
type sstableIterator struct {
    r    io.ReadSeeker
    offs []int64 // stack of visited offsets
    rec  *Record
}

func (it *sstableIterator) Next() (*Record, error) {
    off, err := it.r.Seek(0, io.SeekCurrent)
    if err != nil { return nil, err }
    it.offs = append(it.offs, off)
    rec, err := readRecord(it.r)
    if err != nil { return nil, err }
    it.rec = rec
    return rec, nil
}

func (it *sstableIterator) HasPrev() bool { return len(it.offs) > 1 }

func (it *sstableIterator) Prev() (*Record, error) {
    last := it.offs[len(it.offs)-2]
    it.offs = it.offs[:len(it.offs)-1]
    if _, err := it.r.Seek(last, io.SeekStart); err != nil { return nil, err }
    rec, err := readRecord(it.r)
    if err != nil { return nil, err }
    it.rec = rec
    return rec, nil
}
```

Considerations:
- Keep the stack bounded by dropping offsets when the iterator moves far ahead.
- Ensure block-prefetch and cache eviction strategies remain valid when seeking backwards.
