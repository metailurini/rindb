# Bidirectional SSTable iterator plan

This document expands step 3 of the reverse range scanning plan and outlines how to walk an SSTable in both directions without materializing all records.

1. Seed the offset stack with the file's starting offset so `HasPrev` is false before the first `Next`.
2. Push the offset of each returned record onto the stack. `Prev` pops the current record's offset and seeks to the new top-of-stack to reread the previous record.
3. Bound the stack by a configurable `maxOffs`. When the length exceeds `maxOffs`, drop the oldest entry; attempting to `Prev` past this point will return `EOI`.
4. When the iterator reaches a new block, preload its footer and remember the previous block's starting offset to allow rewinding.
5. Extend `sstableIterator` and `sstableIRange` to share the stack and expose `HasPrev`/`Prev` while staying compatible with the table cache.

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
- Make `maxOffs` large enough to cover typical scan ranges (e.g., 64K entries) while keeping memory usage predictable.
- Ensure block-prefetch and cache eviction strategies remain valid when seeking backwards.
