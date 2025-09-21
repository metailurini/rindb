# Step 2 – Merging Iterator Descending Seeding Plan

## Goals
We need the range iterator stack to honor an initial descending orientation without forcing callers to walk forward first.
The merging iterator must seed both priority queues consistently, preserve crossing-anchor semantics, and still guarantee deterministic cleanup.
This plan focuses on the mechanics inside `RangeIterator` and `MergingIterator` so that Step 3 and Step 4 can build on a stable contract.
It assumes Step 1b (see `docs/dev/312/step1b_iterator_last_plan.md`) has added a shared `Last()` helper to the `Iterator` interface so we can prime descending heaps without bespoke adapter code.

## Implementation Steps
1. **Carry the requested order through RangeIterator state.** The wrapper should initialize anchor bookkeeping according to the incoming order and surface the correct `HasNext/HasPrev` answers before iteration begins.

```go
// range_iterator.go
func NewRangeIterator(mi *MergingIterator, order RangeOrder) *RangeIterator {
    ri := &RangeIterator{
        mi:      mi,
        order:   order,
        forward: order == RangeAsc,
    }
    if order == RangeDesc {
        ri.crossingAnchor.initDescending()
        ri.cachedPrev, ri.prevErr = mi.peekReverse()
        ri.hasPrevPrimed = ri.prevErr == nil
    }
    return ri
}

func (ri *RangeIterator) HasPrev() bool {
    if ri.order == RangeDesc && !ri.forward {
        return ri.hasPrevPrimed
    }
    return ri.mi.HasPrev()
}
```

2. **Prime both heaps inside MergingIterator based on the chosen order.** Construct helpers that load the forward heap with the minimal record or the reverse heap with the maximal record from each child while respecting sequence filtering hooks. The reverse path relies on the new `Iterator.Last()` contract introduced in Step 1b so every child can expose its tail element consistently.

```go
// merging_iterator.go
type MergingIterator struct {
    cleanup   func()
    order     RangeOrder
    forward   bool
    fwdHeap   *recordHeap
    revHeap   *recordHeap
    anchors   crossingAnchor
}

func NewMergingIterator(children []Iterator[Record], cleanup func(), order RangeOrder) (*MergingIterator, error) {
    m := &MergingIterator{
        cleanup: cleanup,
        order:   order,
        forward: order == RangeAsc,
        fwdHeap: newForwardHeap(),
        revHeap: newReverseHeap(),
    }
    switch order {
    case RangeAsc:
        if err := m.seedForward(children); err != nil {
            m.cleanup()
            return nil, err
        }
    case RangeDesc:
        if err := m.seedReverse(children); err != nil {
            m.cleanup()
            return nil, err
        }
    }
    return m, nil
}

func (m *MergingIterator) seedReverse(children []Iterator[Record]) error {
    for _, child := range children {
        rec, err := child.Last()
        if errors.Is(err, io.EOF) {
            continue
        }
        if err != nil {
            return err
        }
        m.revHeap.Push(rec, child)
    }
    if m.revHeap.Len() > 0 {
        m.anchors.initFromReverse(m.revHeap.PeekKey())
    }
    return nil
}
```

3. **Harmonize direction flips with crossing anchors.** Calling `Next` after `Prev` (and vice versa) should reuse the cached anchors to avoid duplicates while keeping tombstones hidden.

```go
func (m *MergingIterator) Next() (Record, error) {
    if m.order == RangeDesc && !m.forward {
        if err := m.syncFromReverse(); err != nil {
            return Record{}, err
        }
    }
    rec, src, err := m.popForward()
    if err != nil {
        return Record{}, err
    }
    m.forward = true
    m.anchors.updateFromForward(rec.InternalKey)
    if refill := src.HasNext(); refill {
        nxt, err := src.Next()
        if err == nil {
            m.fwdHeap.Push(nxt, src)
        }
    }
    return rec, nil
}

func (m *MergingIterator) Prev() (Record, error) {
    if m.order == RangeAsc && m.forward {
        if err := m.syncFromForward(); err != nil {
            return Record{}, err
        }
    }
    rec, src, err := m.popReverse()
    if err != nil {
        return Record{}, err
    }
    m.forward = false
    m.anchors.updateFromReverse(rec.InternalKey)
    if refill := src.HasPrev(); refill {
        prv, err := src.Prev()
        if err == nil {
            m.revHeap.Push(prv, src)
        }
    }
    return rec, nil
}
```

4. **Centralize cleanup guarantees and error propagation.** Ensure both heaps drain correctly and the shared `cleanup` runs exactly once, even if seeding fails or a child iterator bubbles an error mid-iteration.

```go
func (m *MergingIterator) Close() error {
    if m.cleanup == nil {
        return nil
    }
    defer func() { m.cleanup = nil }()
    for m.fwdHeap.Len() > 0 {
        _, child := m.fwdHeap.Pop()
        child.Close()
    }
    for m.revHeap.Len() > 0 {
        _, child := m.revHeap.Pop()
        child.Close()
    }
    m.cleanup()
    return nil
}

func (m *MergingIterator) syncFromReverse() error {
    if m.revHeap.Len() == 0 {
        return io.EOF
    }
    pivot := m.revHeap.PeekKey()
    if m.anchors.isForwardDuplicate(pivot) {
        _, _, _ = m.revHeap.Pop()
    }
    if m.fwdHeap.Len() == 0 {
        if err := m.backfillForward(); err != nil {
            return err
        }
    }
    return nil
}
```

## Pitfalls & Test Hooks
- **Duplicate emission when switching direction.** Guard with a regression that alternates `Next`/`Prev` starting from descending mode and asserts strict key monotonicity.

```go
func TestRangeIterator_DescendingOscillation(t *testing.T) {
    it := buildRangeIterator(order: RangeDesc, keys: []string{"k3","k2","k1"})
    assertKey(t, it.Prev(), "k3")
    assertKey(t, it.Next(), "k2")
    assertKey(t, it.Prev(), "k2")
    assertKey(t, it.Next(), "k1")
}
```

- **Reverse seeding skips tombstone filtering.** Add a memtable-backed fixture with deletes and verify descending reads hide shadowed entries just like ascending reads.

```go
func TestMergingIterator_DescTombstoneFiltering(t *testing.T) {
    mt := fakeMemtable([]record{{Key: "a", Deleted: true}, {Key: "a", Seq: 1}})
    it := newMergingIterator(order: RangeDesc, children: []Iterator{mt})
    _, err := it.Prev()
    require.ErrorIs(t, err, io.EOF)
}
```

- **Cleanup leaks when seeding fails.** Inject a child iterator that errors on `Last` and assert that `cleanup` fires once and all children are closed.

```go
func TestMergingIterator_SeedReverseFailure(t *testing.T) {
    called := 0
    cleanup := func() { called++ }
    bad := &faultyIterator{lastErr: errors.New("boom")}
    _, err := NewMergingIterator([]Iterator{bad}, cleanup, RangeDesc)
    require.Error(t, err)
    require.Equal(t, 1, called)
    require.True(t, bad.closed)
}
```

- **Crossing anchor drift across SSTables.** Feed records that straddle SSTable boundaries and confirm the reverse heap drops anything ≥ anchor before replaying the forward heap.

```go
func TestMergingIterator_AnchorAlignmentAcrossSources(t *testing.T) {
    left := fakeIterator(keys: []string{"k1","k2"})
    right := fakeIterator(keys: []string{"k2","k3"})
    it := newMergingIterator(order: RangeDesc, children: []Iterator{left, right})
    assertKey(t, it.Prev(), "k3")
    assertKey(t, it.Prev(), "k2") // single occurrence
    assertKey(t, it.Prev(), "k1")
    _, err := it.Prev()
    require.ErrorIs(t, err, io.EOF)
}
```
