# Bidirectional merging and range iterators plan

This document expands step 4 of the reverse range scanning plan, detailing how multiple iterators can be merged while supporting backward traversal.

1. Maintain two heaps: `fwd` (min-heap) and `rev` (max-heap) containing the same `pqItem` pointers.
2. `Next` pops from `fwd`, pushes the item onto `rev`, and advances the underlying iterator; `Prev` mirrors this process.
3. Tombstones and sequence-number de-duplication occur in both directions before returning each record.

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
    m.cur = heap.Pop(&m.rev).(pqItem)
    heap.Push(&m.fwd, m.cur)
    rewind(m.cur.src) // may push item into rev
    return m.cur.rec, nil
}
```

Concerns:
- Rewind helpers must reapply tombstone filtering so that deleted keys stay hidden.
- Dual heaps require rebalancing when sources are exhausted, otherwise `HasPrev` may leak duplicates.
