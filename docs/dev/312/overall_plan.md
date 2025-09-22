# Asc/Desc Range Iteration Plan

RinDB's range iterators currently assume callers walk forward before moving backward, so exposing an `asc`/`desc` flag needs deeper coordination than simply toggling a boolean.
The goal is to thread an explicit order value through the public API and iterator stack while keeping tombstone filtering, crossing-anchor semantics, and resource cleanup intact.
We will tackle the work in layered increments so each stage has a clear set of invariants and guardrails.

1. **Step 1 – Expand the public API with an order parameter (Complexity 5/10).** We add an ergonomic option for callers and make sure defaults remain ascending so existing integrations behave the same. The range config continues to accept an optional snapshot cut-off: we replace the loose variadic sequence argument with a dedicated option helper so wrappers like `Snapshot.IRange` can forward their sequence before layering on the order flag.
   This step keeps the existing contract for inverted ranges (`start > end`) by returning an empty iterator while letting callers pick either iteration direction with the same bounds. Callers will compose options—e.g. `db.IRange(ctx, start, end, IRangeSnapshot(seq), IRangeOrder(RangeDesc))`—to request both a historical view and the initial iteration direction.

```go
// Step 1: surface RangeOrder + snapshot option
// type RangeOrder int
const (
  RangeAsc RangeOrder = iota
  RangeDesc
)
type rangeConfig struct {
  order       RangeOrder
  snapshotSeq *uint64
}
type RangeOption func(*rangeConfig)
func IRangeOrder(order RangeOrder) RangeOption { ... }
func IRangeSnapshot(seq uint64) RangeOption { ... }
func (s *Snapshot) IRange(ctx, start, end Bytes, opts ...RangeOption) (*RangeIterator, error) {
  opts = append([]RangeOption{IRangeSnapshot(s.sequence)}, opts...)
  return s.db.IRange(ctx, start, end, opts...)
}
func (r *Rindb) IRange(ctx, start, end Bytes, opts ...RangeOption) (*RangeIterator, error) {
  cfg := rangeDefaultConfig()
  for _, opt := range opts { opt(&cfg) }
  iterators, cleanup := r.buildSources(ctx, start, end, cfg.snapshotSeq)
  if start.Compare(end) == CmpGreater {
    cleanup()
    return newEmptyRangeIterator(), nil // preserve today’s “empty on inverted bounds” contract
  }
  if cfg.order == RangeDesc {
    cleanup()
    return nil, ErrRangeOrderNotReady // Step 1 gate: descending rolls out in Step 2+
  }
  mi, err := NewMergingIterator(iterators, cleanup, cfg.order)
  return NewRangeIterator(mi, cfg.order), err
  // RangeOrder flips iteration direction without swapping start/end semantics.
}
```

`newEmptyRangeIterator` can wrap the existing forward heap without exposing any records so inverted bounds continue to fall through to the empty case. The temporary `ErrRangeOrderNotReady` guard keeps the public API honest until Step 2 wires the descending implementation through the merging iterator.

Callers must supply `start <= end`; `RangeOrder` only determines whether we traverse that span from low-to-high or high-to-low once descending support lands in Step 2. This keeps the contract consistent for skip list, memtable, and SSTable updates later in the plan while letting Step 1 update documentation/tests to mention the temporary error for `RangeDesc` requests. Step 5’s rollout checklist should include removing the guard and flipping the docs/examples when descending iteration is fully enabled.

2. **Step 1b – Add iterator tail priming support (Complexity 6/10).** Before the merging iterator can seed descending order we need a shared `Last()` helper on every iterator implementation.
   This step updates the core `Iterator` contract plus `slIterator`, `memtableIRange`, `sstableIRange`, and test fixtures so they can surface their final record without duplicating bespoke plumbing. See `docs/dev/312/step1b_iterator_last_plan.md` for the detailed design.

```go
// Step 1b: iterator tail priming
type Iterator[T any] interface {
  ...
  Last() (T, error)
}
func (it *sstableIRange) Last() (Record, error) {
  if err := it.seekTail(); err != nil {
    return Record{}, err
  }
  return it.curr, nil
}
```

3. **Step 2 – Teach RangeIterator/MergingIterator to honor the initial order (Complexity 9/10).** The iterators must seed their forward/backward heaps based on the requested orientation and cope with direction changes without duplicating records. In particular, descending ranges should return `Next()` results in descending key order so clients can consume `6,5,4…` without switching APIs.
   Because this rewrites core iteration mechanics, we will drive the finer design in `docs/dev/312/step2_merging_iterator_plan.md`.

```go
// Step 2: order-aware merging
func NewRangeIterator(mi *MergingIterator, order RangeOrder) *RangeIterator {
  ri := &RangeIterator{mi: mi, order: order, forward: order != RangeDesc}
  if order == RangeDesc {
    if rec, err := mi.peekReverse(); err == nil {
      ri.reverseCached = rec
      ri.reversePrimed = true
    } else if !errors.Is(err, EOI) {
      ri.err = err
    }
  }
  return ri
}

func (ri *RangeIterator) prepareNext() {
  if ri.order == RangeDesc {
    for !ri.nextPrepared && ri.err == nil {
      if !ri.reversePrimed {
        rec, err := ri.mi.peekReverse()
        if err != nil {
          if !errors.Is(err, EOI) {
            ri.err = err
          }
          return
        }
        ri.reverseCached = rec
        ri.reversePrimed = true
      }
      candidate := ri.reverseCached
      // reuse existing duplicate/tombstone filtering before accepting candidate
      ri.next = candidate
      ri.nextPrepared = true
      ri.mi.commitPeekedReverse()
      ri.reversePrimed = false
      ri.forward = false
    }
    return
  }
  // existing ascending path ...
}

func NewMergingIterator(children []Iterator[Record], cleanup func(), order RangeOrder) (*MergingIterator, error) { ... }
func (m *MergingIterator) peekReverse() (Record, error) { ... } // uses PriorityQueue[pqItem].PeekItem()
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
      // iterator exhausted
    default:
      m.err = err
    }
  }
  m.forward = false
  m.reversePrimed = false
  m.cachedReverse = pqItem{}
}
// Maintain crossingAnchor invariants regardless of initial direction using PriorityQueue[pqItem].
```

4. **Step 3 – Provide descending cursors for memtable/skiplist layers (Complexity 7/10).** We modify the skip list range iterator so it can begin from the predecessor of the end key and update memtable filtering to respect that cursor, while still treating `start` as the inclusive lower bound and `end` as the upper bound.
   The sequence filtering must stay symmetric so deletions remain hidden in both directions.

```go
// Step 3: skip list + memtable
func (list *SkipList[K,V]) IRange(start, end K, order RangeOrder) Iterator[V] {
  if order == RangeDesc {
    curr := findFloor(end)
    return &slIRange{curr: curr, startKey: start, endKey: end, order: RangeDesc}
  }
  ...
}
func (mi *memtableIRange) Next()/Prev() {
  if mi.order == RangeDesc {
    mi.prepareNextDescending()
  }
  // ensure preparedNext/preparedPrev flip correctly when alternating directions
}
```

5. **Step 4 – Add descending seed logic to `sstableIRange` (Complexity 8/10).** Introduce a `findOffsetLE` helper that binary searches the sparse index for the final block whose key is ≤ `end`, then have `primeDescending` step backward with `PrevOffset` until the first in-range record is prepared.
   Follow-up `Next()` calls in descending mode should invoke a new `prepareNextDescending()` helper so steady-state iteration also walks backward and respects the `startKey` guard.
   This keeps the work logarithmic in table size and ensures the iterator exposes the same `start <= end` contract regardless of initial direction.

```go
// Step 4: SSTable tail seeding
func (s SStable) IRange(start, end Bytes, order RangeOrder, seq ...uint64) (Iterator[Record], error) {
  if order == RangeDesc {
    offset, haveOffset := s.findOffsetLE(end)
    if !haveOffset { return emptyIterator(), nil }
    sri := &sstableIRange{offset: offset, cursor: offset, lowerBound: findLowerBound(start), haveLowerBound: true, order: RangeDesc}
    if err := sri.primeDescending(); err != nil { return nil, err }
    return sri, nil
  }
  ...
}
func (s *SStable) findOffsetLE(end Bytes) (int64, bool) {
  // binary search SparseIndex for block starting key <= end
}
func (sri *sstableIRange) primeDescending() error {
  // use PrevOffset to walk blocks/records until key < start or seq too new
}
func (sri *sstableIRange) Next() (Record, error) {
  if sri.order == RangeDesc {
    return sri.prepareNextDescending()
  }
  ...
}
func (sri *sstableIRange) prepareNextDescending() (Record, error) {
  // mirror primeDescending for steady-state iteration and stop once key < startKey
}
```

6. **Step 5 – Update regression coverage and documentation (Complexity 6/10).** We extend unit and integration suites to cover descending usage and make sure docs & CLI helpers describe the new flag.
   Coverage should include asc/desc parity checks and alternating direction smoke tests.

```go
// Step 5: verification assets
// - rindb_test.go: add desc cases mirroring TestRindb_IRangeDirections where Next() yields the highest key first
// - integration/range_iterator_next_prev_test.go: table-driven asc vs desc checks that Next() streams k3,k2,k1
// - sstable_iteration_test.go & memtable_test.go: ensure Prev before Next works when order == RangeDesc and Next() remains monotonic
// - README + cmd/main.go usage banner: document the new order option
```

Pitfalls and test hooks: we will guard against regressions by pairing each risk with an existing or new test so failures surface early.
Maintaining this mapping clarifies how to validate every layer as we iterate on the detailed designs.

```go
// Pitfall: reverse seeding skips first record -> Add desc variant of TestSSTableIRange_ReverseIteration that checks Next() returns the end key first.
// Pitfall: duplicates during direction flip -> Extend integration/range_iterator_next_prev_test.go with desc oscillation covering Next()/Prev() alternation.
// Pitfall: tombstones leaking -> Mirror TestMemtableIRange_Reverse under desc mode using Next() reads.
// Pitfall: invalid range inputs -> New unit tests for RangeOrder validation in rindb_test.go.
```
