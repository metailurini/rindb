# Bidirectional merging and range iterators plan

This document expands step 4 of the reverse range scanning plan, detailing how multiple iterators can be merged while supporting backward traversal.

1. Seed `fwd` with the first record from every source iterator; `rev` starts empty so calling `Prev` before any `Next` returns `EOI`.
2. `Next` pops from `fwd`, pushes the item onto `rev`, and advances the underlying iterator; `Prev` removes the current item from `rev`, rewinds its source, and returns the new top of `rev`, or `EOI` if the heap becomes empty.
3. Tombstone filtering and sequence-number suppression is handled by the `RangeIterator` before records reach the `MergingIterator`.
4. Remove exhausted sources from the heaps to keep `HasPrev`/`HasNext` accurate.

```go
type MergingIterator struct {
    fwd pqMin
    rev pqMax
    cur pqItem
}

func (m *MergingIterator) Next() (Record, error) {
    m.cur = heap.Pop(&m.fwd).(pqItem)
    heap.Push(&m.rev, m.cur)
    advance(m.cur.src) // may push new item into fwd
    return m.cur.rec, nil
}

func (m *MergingIterator) HasPrev() bool { return m.rev.Len() > 0 }

func (m *MergingIterator) Prev() (Record, error) {
    cur := heap.Pop(&m.rev).(pqItem)
    heap.Push(&m.fwd, cur)
    if prev := rewind(cur.src); prev != nil {
        heap.Push(&m.fwd, prev)
        heap.Push(&m.rev, prev)
    }
    if m.rev.Len() == 0 {
        return Record{}, io.EOF
    }
    m.cur = m.rev.Peek().(pqItem)
    return m.cur.rec, nil
}
```

```go
func advance(src Iterator) {
    if rec, err := src.Next(); err == nil {
        item := pqItem{rec: rec, src: src}
        heap.Push(&m.fwd, item)
    } else if err == io.EOF {
        dropSource(src) // remove from heaps
    }
}

func rewind(src Iterator) *pqItem {
    if rec, err := src.Prev(); err == nil {
        return &pqItem{rec: rec, src: src}
    }
    return nil
}
```

Concerns:
- `RangeIterator` must perform tombstone filtering and sequence-number suppression so deleted or stale keys stay hidden before records reach the merger.
- Dual heaps require rebalancing when sources are exhausted, otherwise `HasPrev` may leak duplicates.
- Edge tests should exhaust a source, alternate `Next`/`Prev`, and traverse across memtables and SSTables to verify correctness.
