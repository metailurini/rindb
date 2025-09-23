# Step 2 – Merging Iterator Descending Seeding Plan

## Goals
We need the range iterator stack to honor an initial descending orientation without forcing callers to walk forward first.
The merging iterator must seed both priority queues consistently, preserve crossing-anchor semantics, and still guarantee deterministic cleanup.
This plan focuses on the mechanics inside `RangeIterator` and `MergingIterator` so that Step 3 and Step 4 can build on a stable contract.
It assumes Step 1b (see `docs/dev/312/step1b_iterator_last_plan.md`) has added a shared `Last()` helper to the `Iterator` interface so we can prime descending heaps without bespoke adapter code.

## Implementation Steps
1. **Carry the requested order through RangeIterator state.** Extend `range.go` so `RangeIterator` records the requested orientation:
   - add `order RangeOrder`
   - add `reverseCached Record`
   - add `reverseErr error`
   - add `reversePrimed bool`

   The constructor now accepts the order and primes descending scans by peeking without draining the reverse heap. Use the existing error handling helpers (`errors.Is(err, EOI)`) and keep the struct’s `forward` flag describing the last movement relative to ascending order.

   While touching this code, rename the staging helpers from `prepare*` to `prime*` (e.g. `primeNext`, `primePrev`) so the terminology matches the descending `primeDescending` entrypoint and the broader iterator plans.

   ```go
   func NewRangeIterator(mi *MergingIterator, order RangeOrder) *RangeIterator {
       ri := &RangeIterator{mi: mi, order: order, forward: order != RangeDesc}
       if order == RangeDesc {
           rec, err := mi.peekReverse()
           switch {
           case errors.Is(err, EOI):
               // Empty range; leave reversePrimed false so HasNext falls through.
           case err != nil:
               ri.err = err
           default:
               ri.reverseCached = rec
               ri.reversePrimed = true
           }
       }
       return ri
   }

   func (ri *RangeIterator) HasNext() bool {
       if ri.order == RangeDesc {
           if !ri.reversePrimed && ri.err == nil {
               rec, err := ri.mi.peekReverse()
               switch {
               case errors.Is(err, EOI):
                   ri.reversePrimed = false
                   ri.reverseErr = err
               case err != nil:
                   ri.err = err
               default:
                   ri.reverseCached = rec
                   ri.reversePrimed = true
               }
           }
           return ri.reversePrimed && ri.err == nil
       }
       ri.primeNext()
       return ri.nextPrepared
   }
   ```

   Update `primeNext` to fetch candidates from the cache when descending: reuse the existing duplicate/tombstone filtering loop by substituting the call that sources the next record. When `reversePrimed` is true, treat `reverseCached` as the candidate, run the same `lastKey`/tombstone guards, and after choosing it invoke `mi.commitPeekedReverse()` so the merging iterator advances its heaps. Clear `reversePrimed` and keep `ri.forward = false` because the most recent movement was reverse relative to ascending order. For the ascending path continue to call `mi.Next()` as today.

   Mirror the change inside `primePrev`: when `order == RangeDesc`, pull candidates from `mi.Next()` (because walking “backwards” relative to a descending scan means moving forward through the merged stream). Reuse the same duplicate/tombstone logic so oscillating between `Next`/`Prev` still honours `lastKey` and `matchesCrossingAnchor`. Once the descending path is in place, drop the temporary `ErrRangeOrderNotReady` guard from Step 1 and update API/docs/tests to expect `RangeDesc` to succeed.

2. **Seed and cache the reverse heap inside MergingIterator.** Keep using `PriorityQueue[pqItem]` for both heaps so we remain aligned with the current implementation. Extend the struct with `order RangeOrder`, `reversePrimed bool`, `reverseErr error`, and `cachedReverse pqItem`. Update the constructor signature to `NewMergingIterator(children []Iterator[Record], cleanup func(), order RangeOrder)` so Step 1’s caller wiring compiles. The existing forward seeding logic stays, and descending scans add a mirrored seeding loop that relies on Step 1b’s `Last()` helper:

   ```go
   type MergingIterator struct {
       fwd     *PriorityQueue[pqItem]
       rev     *PriorityQueue[pqItem]
       cleanup func()
       err     error

       nextPrepared bool
       nextItem     pqItem
       prevPrepared bool
       prevItem     pqItem
       forward      bool

       crossingAnchor    pqItem
       crossingAnchorSet bool

       order         RangeOrder
       cachedReverse pqItem
       reverseErr    error
       reversePrimed bool
   }

   func NewMergingIterator(children []Iterator[Record], cleanup func(), order RangeOrder) (*MergingIterator, error) {
       m := &MergingIterator{
           fwd:     NewPriorityQueue(lessFwd),
           rev:     NewPriorityQueue(lessRev),
           cleanup: cleanup,
           forward: order != RangeDesc,
           order:   order,
       }
       if err := m.seedForward(children); err != nil {
           if cleanup != nil {
               cleanup()
           }
           return nil, err
       }
       if order == RangeDesc {
           if err := m.seedReverse(children); err != nil {
               if cleanup != nil {
                   cleanup()
               }
               return nil, err
           }
       }
       return m, nil
   }

   func (m *MergingIterator) seedReverse(children []Iterator[Record]) error {
       for _, child := range children {
           rec, err := child.Last()
           switch {
           case errors.Is(err, EOI):
               continue
           case err != nil:
               return err
           default:
               m.rev.PushItem(pqItem{rec: rec, iter: child})
           }
       }
       return nil
   }
   ```

   Introduce two helpers that mirror the existing forward-preparation cache:

   ```go
   func (m *MergingIterator) peekReverse() (Record, error) {
       if !m.reversePrimed {
           if m.rev.Len() == 0 {
               m.reverseErr = EOI
           } else {
               m.cachedReverse = m.rev.PeekItem()
               m.reverseErr = nil
           }
           m.reversePrimed = true
       }
       if m.reverseErr != nil {
           return Record{}, m.reverseErr
       }
       return m.cachedReverse.rec, nil
   }

   func (m *MergingIterator) commitPeekedReverse() {
       if !m.reversePrimed {
           return
       }
       if m.reverseErr != nil {
           if !errors.Is(m.reverseErr, EOI) {
               m.err = m.reverseErr
           }
           m.reversePrimed = false
           m.cachedReverse = pqItem{}
           m.reverseErr = nil
           return
       }

       item := m.rev.PopItem()
       m.fwd.PushItem(item)
       if item.iter.HasPrev() {
           prev, err := item.iter.Prev()
           switch {
           case err == nil:
               m.rev.PushItem(pqItem{rec: prev, iter: item.iter})
           case errors.Is(err, EOI):
               // iterator exhausted in reverse direction
           default:
               m.err = err
           }
       }
       m.forward = false
       m.reversePrimed = false
       m.cachedReverse = pqItem{}
       m.reverseErr = nil
   }
   ```

   These helpers ensure the reverse heap stays populated incrementally and that already-emitted records are available in the forward heap for oscillating callers.

3. **Synchronize direction flips without draining the reverse heap.** Replace the eager `backfillForward` helper with incremental state kept by `commitPeekedReverse()`. When a descending scan switches to ascending (`Prev()` after `Next()` in `RangeDesc` mode), `syncFromReverse` only needs to drop any duplicate at the boundary and rely on the forward heap entries that were enqueued during the commits above. Symmetrically, `syncFromForward` keeps working for the ascending-first case. Update both helpers to call `m.matchesCrossingAnchor` against `PriorityQueue[pqItem].PeekItem()`/`PopItem()` and remove the scratch-buffer drain. This change keeps direction flips at O(log n) instead of O(n log n) because we never walk every reverse element just to prime the forward heap.

   Adjust `Next`/`Prev` to respect the stored `order` when deciding which sync helper to call; the rest of their logic (popping from `fwd`/`rev` via `PopItem` and refilling with `PushItem`) can stay as-is. The goal is to reuse today’s mechanics but make them order-aware without introducing new heap types.

4. **Keep cleanup deterministic.** No functional change is required beyond carrying `order` through to `Close`, but double-check that the constructor still invokes `cleanup` if either seeding phase fails and that `Close` returns `m.err`. The existing code already follows that contract; mention it here so reviewers confirm we have not regressed resource handling while teaching the iterator about descending order.

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

- **Direction flips stay sub-linear.** Add a benchmark or tracing test that alternates `Next`/`Prev` several hundred times on a long descending scan and asserts the fake child iterator only receives one additional `Prev` per emitted record (catching any accidental full-heap drains).
