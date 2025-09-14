# Bidirectional merging and range iterators plan

This document expands step 4 of the reverse range scanning plan, detailing how multiple iterators can be merged while supporting backward traversal.

1. The `fwd` (min-heap) is seeded with the first record from each source iterator. The `rev` (max-heap ordered by key then sequence descending) starts empty.
2. On `Next()`, the iterator pops the lowest-ordered item from `fwd`. This item is pushed into `rev` to build a history for backward traversal. The source iterator that provided the item is advanced, and its next item is pushed back into `fwd`.
3. Because `Next()` yields records in sorted order, the most recently visited record always has the highest key/sequence. `Prev()` pops the top of `rev`, rewinds the record's source iterator by one step, and pushes the record back into `fwd` so it can be returned again by a subsequent `Next()` call.
4. Tombstone filtering and sequence-number suppression are handled by a higher-level iterator (`RangeIterator`) before records reach the `MergingIterator`.

```go
type MergingIterator struct {
    fwd     *PriorityQueue[pqItem] // min-heap
    rev     *PriorityQueue[pqItem] // max-heap
    cur     pqItem
    curSet  bool
}

func (m *MergingIterator) Next() (Record, error) {
    item := m.fwd.PopItem()
    m.rev.PushItem(item)
    m.cur = item
    m.curSet = true

    if item.iter.HasNext() {
        rec, err := item.iter.Next()
        if err == nil {
            m.fwd.PushItem(pqItem{rec: rec, iter: item.iter})
        }
    }
    return item.rec, nil
}

func (m *MergingIterator) HasPrev() bool {
    return m.rev.Len() > 0
}

func (m *MergingIterator) Prev() (Record, error) {
    cur := m.rev.PopItem()
    _, _ = cur.iter.Prev() // rewind underlying iterator
    m.fwd.PushItem(cur)
    if m.rev.Len() > 0 {
        m.cur = m.rev.PeekItem()
    } else {
        m.curSet = false
    }
    return cur.rec, nil
}
```

Concerns:
- `RangeIterator` is responsible for all filtering (tombstones, sequence numbers) before records are passed to the `MergingIterator`.
- The `rev` heap only contains items already yielded by `Next()`. A call to `Prev()` is only possible after at least one `Next()` call has succeeded.
- Exhausted iterators are naturally handled as they stop providing new items to the `fwd` heap.
