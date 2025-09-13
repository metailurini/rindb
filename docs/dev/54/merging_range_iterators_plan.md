# Bidirectional merging and range iterators plan

This document expands step 4 of the reverse range scanning plan, detailing how multiple iterators can be merged while supporting backward traversal.

1. Seed both heaps with the first record from every source iterator. `fwd` is a min-heap by key/seq; `rev` is a max-heap containing the same `pqItem` pointers.
2. `Next` pops from `fwd`, pushes the item onto `rev`, and advances the underlying iterator; `Prev` removes the current item from `rev`, rewinds its source, and returns the new top of `rev`.
3. Tombstones and sequence-number de-duplication occur in both directions before items are pushed onto either heap.
4. Remove exhausted sources from both heaps to keep `HasPrev`/`HasNext` accurate.

```go
type MergingIterator struct {
    fwd pqMin
    rev pqMax
    cur pqItem
}

func (m *MergingIterator) Next() (Record, error) {
    m.cur = heap.Pop(&m.fwd).(pqItem)
    heap.Push(&m.rev, m.cur)
    advance(m.cur.src) // may push new item into both heaps
    return m.cur.rec, nil
}

func (m *MergingIterator) HasPrev() bool { return m.rev.Len() > 1 }

func (m *MergingIterator) Prev() (Record, error) {
    cur := heap.Pop(&m.rev).(pqItem)
    heap.Push(&m.fwd, cur)
    if prev := rewind(cur.src); prev != nil {
        heap.Push(&m.fwd, prev)
        heap.Push(&m.rev, prev)
    }
    m.cur = m.rev.Peek().(pqItem)
    return m.cur.rec, nil
}
```

```go
func advance(src Iterator) {
    if rec, err := src.Next(); err == nil && !isTombstoned(rec) && isNewest(rec) {
        item := pqItem{rec: rec, src: src}
        heap.Push(&m.fwd, item)
        heap.Push(&m.rev, item)
    } else if err == io.EOF {
        dropSource(src) // remove from both heaps
    }
}

func rewind(src Iterator) *pqItem {
    if rec, err := src.Prev(); err == nil && !isTombstoned(rec) && isNewest(rec) {
        return &pqItem{rec: rec, src: src}
    }
    return nil
}
```

Concerns:
- Rewind helpers must reapply tombstone filtering and sequence-number suppression so deleted or stale keys stay hidden.
- Dual heaps require rebalancing when sources are exhausted, otherwise `HasPrev` may leak duplicates.
- Edge tests should exhaust a source, alternate `Next`/`Prev`, and traverse across memtables and SSTables to verify correctness.
