# Step 2 – Merging Iterator Descending Seeding Plan

## Goals
We need the range iterator stack to honor an initial descending orientation without forcing callers to walk forward first.
The merging iterator must seed both priority queues consistently, preserve crossing-anchor semantics, and still guarantee deterministic cleanup.
This plan focuses on the mechanics inside `RangeIterator` and `MergingIterator` so that Step 3 and Step 4 can build on a stable contract.
It assumes Step 1b (see `docs/dev/312/step1b_iterator_last_plan.md`) has added a shared `Last()` helper to the `Iterator` interface so we can prime descending heaps without bespoke adapter code.

## Implementation Steps
1. **Carry the requested order through RangeIterator state.** The wrapper should initialize anchor bookkeeping according to the incoming order and surface the correct `HasNext/HasPrev` answers before iteration begins. Extend the struct in `range.go` with:
   - `order RangeOrder`
   - `cachedReverse Record`
   - `reverseErr error`
   - `hasReversePrimed bool`

   The constructor keeps the forward-only default for ascending scans but primes descending scans using `MergingIterator.peekReverse()` without immediately draining the reverse heap so the first `Next()` can yield the largest key:

   ```go
   // range.go
   func NewRangeIterator(mi *MergingIterator, order RangeOrder) *RangeIterator {
       ri := &RangeIterator{
           mi:      mi,
           order:   order,
           forward: order == RangeAsc,
       }
       if order == RangeDesc {
           ri.crossingAnchor.initDescending()
           ri.cachedReverse, ri.reverseErr = mi.peekReverse()
           ri.hasReversePrimed = true
       }
       return ri
   }

   func (ri *RangeIterator) HasNext() bool {
       if ri.order == RangeDesc {
           if !ri.hasReversePrimed {
               ri.cachedReverse, ri.reverseErr = ri.mi.peekReverse()
               ri.hasReversePrimed = true
           }
           if ri.reverseErr != nil {
               if !errors.Is(ri.reverseErr, EOI) {
                   ri.err = ri.reverseErr
               }
               return false
           }
           return true
       }
       return ri.mi.HasNext()
   }
   ```

   Update `prepareNext` to consume the cached primed value before delegating to the merging iterator for additional reverse reads. When `hasReversePrimed` is true, branch on `reverseErr`: propagate non-`EOI` failures into `ri.err`; otherwise copy `cachedReverse` into `ri.next`, mark it prepared, clear the flag, and invoke `mi.commitPeekedReverse()` (see Step 2) so the first `Next()` on a descending iterator removes the max key from the heap exactly once. Reset `cachedReverse`/`reverseErr` after the commit and only fall back to `mi.Next()` once the cache has been drained. Leave `ri.forward` false until a `Prev()` call flips direction so the range iterator continues treating the scan as reverse-first. For `HasPrev` in descending mode we can delegate to `mi.HasPrev()` since the reverse heap is already synchronized via the commit helper.

2. **Prime both heaps inside MergingIterator based on the chosen order.** Construct helpers that load the forward heap with the minimal record or the reverse heap with the maximal record from each child while respecting sequence filtering hooks. The reverse path relies on the new `Iterator.Last()` contract introduced in Step 1b so every child can expose its tail element consistently. Extend `merging_iterator.go` with the following fields:
   - `order RangeOrder`
   - `cachedReverse pqItem`
   - `reverseErr error`
   - `hasReversePrimed bool`

   ```go
   // merging_iterator.go
   type MergingIterator struct {
       cleanup func()
       order   RangeOrder
       forward bool
       fwdHeap *recordHeap
       revHeap *recordHeap
       anchors crossingAnchor
       err     error

       cachedReverse    pqItem
       reverseErr       error
       hasReversePrimed bool
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
           // Step 1b extends Iterator with Last so each child can surface its tail.
           rec, err := child.Last()
           if errors.Is(err, EOI) {
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

   Add a `peekReverse()` helper that peeks at the reverse heap and memoizes the result without disturbing heap or iterator state:

   ```go
   func (m *MergingIterator) peekReverse() (Record, error) {
       if !m.hasReversePrimed {
           if m.revHeap.Len() == 0 {
               m.cachedReverse = pqItem{}
               m.reverseErr = EOI
           } else {
               m.cachedReverse = m.revHeap.PeekItem()
               m.reverseErr = nil
           }
           m.hasReversePrimed = true
       }
       if m.reverseErr != nil {
           return Record{}, m.reverseErr
       }
       return m.cachedReverse.rec, nil
   }
   ```

   Implement a companion `commitPeekedReverse()` helper that is invoked once the cached record has been handed to the range iterator. The helper should:
   - Return immediately if `hasReversePrimed` is false.
   - When `reverseErr` is non-nil, clear `hasReversePrimed`, reset `cachedReverse`, and assign `m.err` for non-`EOI` errors.
   - Otherwise pop `cachedReverse` from `revHeap` (safe because `peekReverse()` never mutates the heap), run the same backwards bookkeeping as `preparePrev()`—namely call `matchesCrossingAnchor` before reinserting the item into `fwdHeap`, rewind the child iterator via `Prev()` and requeue the predecessor in `revHeap`, and surface any underlying error through `m.err`.
   - Flip `m.forward` to false so subsequent `Next()` calls continue treating the iterator as walking in reverse order until a caller explicitly changes direction.
   - Finally, clear `hasReversePrimed`, `cachedReverse`, and `reverseErr`.

   With that helper in place, `RangeIterator.prepareNext()` becomes the sole consumer of the primed state: it copies `cachedReverse` into `ri.next`, invokes `commitPeekedReverse()` so the merging iterator advances its heaps, and clears the flag before control ever reaches `MergingIterator.Next()`. Consequently `Next()` does not branch on `hasReversePrimed`; it can assume the cached record has been retired and proceed with the usual forward traversal (sync direction, pop the heap, and refill from children) without risking a duplicate first record when the client keeps asking for `Next()`.

3. **Harmonize direction flips with crossing anchors.** Calling `Next` after `Prev` (and vice versa) should reuse the cached anchors to avoid duplicates while keeping tombstones hidden.

```go
func (m *MergingIterator) Next() (Record, error) {
    if m.err != nil {
        return Record{}, m.err
    }
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
        switch {
        case err == nil:
            m.fwdHeap.Push(nxt, src)
        case errors.Is(err, EOI):
            // Child exhausted; no refill required.
        default:
            m.err = err
            return Record{}, err
        }
    }
    return rec, nil
}

func (m *MergingIterator) Prev() (Record, error) {
    if m.err != nil {
        return Record{}, m.err
    }
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
        switch {
        case err == nil:
            m.revHeap.Push(prv, src)
        case errors.Is(err, EOI):
            // Iterator hit the beginning; nothing to repopulate.
        default:
            m.err = err
            return Record{}, err
        }
    }
    return rec, nil
}
```

4. **Centralize cleanup guarantees and error propagation.** Let the injected `cleanup` closure continue to own all child lifecycle work, and make sure it runs exactly once while any stored iterator error is returned to callers.

```go
func (m *MergingIterator) Close() error {
    if m.cleanup == nil {
        return m.err
    }
    m.cleanup()
    m.cleanup = nil
    return m.err
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

func (m *MergingIterator) backfillForward() error {
    type drained struct {
        rec Record
        src Iterator[Record]
    }
    scratch := make([]drained, 0, m.revHeap.Len())
    var failure error
    for m.revHeap.Len() > 0 {
        rec, src := m.revHeap.Pop()
        scratch = append(scratch, drained{rec: rec, src: src})
        if m.anchors.isForwardDuplicate(rec.InternalKey) {
            continue
        }
        m.fwdHeap.Push(rec, src)
        if src.HasNext() {
            nxt, err := src.Next()
            switch {
            case err == nil:
                m.fwdHeap.Push(nxt, src)
            case errors.Is(err, EOI):
                // No additional forward elements remain for this iterator.
            default:
                m.err = err
                failure = err
            }
        }
        if failure != nil {
            break
        }
    }
    for i := range scratch {
        entry := scratch[len(scratch)-1-i]
        m.revHeap.Push(entry.rec, entry.src)
    }
    return failure
}
```

## Pitfalls & Test Hooks
- **Duplicate emission when switching direction.** Guard with a regression that alternates `Next`/`Prev` starting from descending mode and asserts strict key monotonicity.

```go
func TestRangeIterator_DescendingOscillation(t *testing.T) {
    it := buildRangeIterator(t, RangeDesc, []string{"k3", "k2", "k1"})
    assertKey(t, it.Next(), "k3")
    assertKey(t, it.Next(), "k2")
    assertKey(t, it.Prev(), "k2")
    assertKey(t, it.Next(), "k1")
}
```

- **First Next consumes the peeked maximum exactly once.** Add a regression that builds a descending range iterator and asserts the first `Next()` returns the largest key and the next `Next()` immediately advances to the next key without duplication.

```go
func TestRangeIterator_DescendingFirstNext(t *testing.T) {
    it := buildRangeIterator(t, RangeDesc, []string{"k3", "k2"})
    assertKey(t, it.Next(), "k3")
    assertKey(t, it.Next(), "k2")
    assertNoDuplicate(t, it)
}
```

- **Reverse seeding skips tombstone filtering.** Add a memtable-backed fixture with deletes and verify descending `Next()` reads hide shadowed entries just like ascending scans.

```go
func TestMergingIterator_DescTombstoneFiltering(t *testing.T) {
    mt := fakeMemtable([]record{{Key: "a", Deleted: true}, {Key: "a", Seq: 1}})
    it := newMergingIterator(t, RangeDesc, []Iterator[Record]{mt})
    _, err := it.Next()
    require.ErrorIs(t, err, io.EOF)
}
```

- **Cleanup leaks when seeding fails.** Inject a child iterator that errors on `Last` and assert that `cleanup` fires once and all children are closed.

```go
func TestMergingIterator_SeedReverseFailure(t *testing.T) {
    called := 0
    cleanup := func() { called++ }
    bad := &faultyIterator{lastErr: errors.New("boom")}
    _, err := NewMergingIterator([]Iterator[Record]{bad}, cleanup, RangeDesc)
    require.Error(t, err)
    require.Equal(t, 1, called)
    require.True(t, bad.closed)
}
```

- **Crossing anchor drift across SSTables.** Feed records that straddle SSTable boundaries and confirm the reverse heap drops anything ≥ anchor before replaying the forward heap.

```go
func TestMergingIterator_AnchorAlignmentAcrossSources(t *testing.T) {
    left := fakeIterator([]string{"k1", "k2"})
    right := fakeIterator([]string{"k2", "k3"})
    it := newMergingIterator(t, RangeDesc, []Iterator[Record]{left, right})
    assertKey(t, it.Next(), "k3")
    assertKey(t, it.Next(), "k2") // single occurrence
    assertKey(t, it.Next(), "k1")
    _, err := it.Next()
    require.ErrorIs(t, err, io.EOF)
}
```
