# Asc/Desc Range Iteration Plan

RinDB's range iterators currently assume callers walk forward before moving backward, so exposing an `asc`/`desc` flag needs deeper coordination than simply toggling a boolean.
The goal is to thread an explicit order value through the public API and iterator stack while keeping tombstone filtering, crossing-anchor semantics, and resource cleanup intact.
We will tackle the work in layered increments so each stage has a clear set of invariants and guardrails.

1. **Step 1 – Expand the public API with an order parameter (Complexity 5/10).** We add an ergonomic option for callers and make sure defaults remain ascending so existing integrations behave the same.
   This step also validates bounds early so we do not hand invalid ranges to lower layers.

```go
// Step 1: surface RangeOrder
// type RangeOrder int
// const (
//   RangeAsc RangeOrder = iota
//   RangeDesc
// )
// func IRangeOrder(order RangeOrder) RangeOption { ... }
// func (r *Rindb) IRange(ctx, start, end Bytes, opts ...RangeOption) (*RangeIterator, error) {
//   cfg := rangeDefaultConfig()
//   for _, opt := range opts { opt(&cfg) }
//   if cfg.order == RangeAsc && start.Compare(end) == CmpGreater { return nil, ErrInvalidRange }
//   if cfg.order == RangeDesc && start.Compare(end) == CmpLess { return nil, ErrInvalidRange }
//   mi, err := NewMergingIterator(iterators, cleanup, cfg.order)
//   return NewRangeIterator(mi, cfg.order), err
// }
```

2. **Step 1b – Add iterator tail priming support (Complexity 6/10).** Before the merging iterator can seed descending order we need a shared `Last()` helper on every iterator implementation.
   This step updates the core `Iterator` contract plus `slIterator`, `memtableIRange`, `sstableIRange`, and test fixtures so they can surface their final record without duplicating bespoke plumbing. See `docs/dev/312/step1b_iterator_last_plan.md` for the detailed design.

```go
// Step 1b: iterator tail priming
// type Iterator[T any] interface {
//   ...
//   Last() (T, error)
// }
// func (it *sstableIRange) Last() (Record, error) {
//   if err := it.seekTail(); err != nil {
//     return Record{}, err
//   }
//   return it.curr, nil
// }
```

3. **Step 2 – Teach RangeIterator/MergingIterator to honor the initial order (Complexity 9/10).** The iterators must seed their forward/backward heaps based on the requested orientation and cope with direction changes without duplicating records.
   Because this rewrites core iteration mechanics, we will drive the finer design in `docs/dev/312/step2_merging_iterator_plan.md`.

```go
// Step 2: order-aware merging
// func NewRangeIterator(mi *MergingIterator, order RangeOrder) *RangeIterator { ... }
// func NewMergingIterator(iterators []Iterator[Record], cleanup func(), order RangeOrder) (*MergingIterator, error) {
//   switch order {
//   case RangeAsc:
//     primeForward(iterators)
//   case RangeDesc:
//     primeReverse(iterators) // new helper that seeds rev heap and backfills fwd for oscillation
//   }
//   return &MergingIterator{fwd: fwd, rev: rev, forward: order == RangeAsc}, nil
// }
// func (m *MergingIterator) HasNext() bool {
//   if m.order == RangeDesc && !m.forward && !m.revPrimed {
//     m.preparePrev() // first call in desc mode should surface rev heap
//   }
//   ...
// }
// // Maintain crossingAnchor invariants regardless of initial direction.
```

4. **Step 3 – Provide descending cursors for memtable/skiplist layers (Complexity 7/10).** We modify the skip list range iterator so it can begin from the predecessor of the end key and update memtable filtering to respect that cursor.
   The sequence filtering must stay symmetric so deletions remain hidden in both directions.

```go
// Step 3: skip list + memtable
// func (list *SkipList[K,V]) IRange(start, end K, order RangeOrder) Iterator[V] {
//   if order == RangeDesc {
//     curr := findFloor(end)
//     return &slIRange{curr: curr, startKey: start, endKey: end, order: RangeDesc}
//   }
//   ...
// }
// func (mi *memtableIRange) Next()/Prev() {
//   if mi.order == RangeDesc {
//     mi.preparePrevDescending()
//   }
//   // ensure preparedNext/preparedPrev flip correctly when alternating directions
// }
```

5. **Step 4 – Add descending seed logic to `sstableIRange` (Complexity 8/10).** Introduce a `findOffsetLE` helper that binary searches the sparse index for the final block whose key is ≤ `end`, then have `primeDescending` step backward with `PrevOffset` until the first in-range record is prepared.
   This keeps the work logarithmic in table size and ensures the iterator exposes the same contract regardless of initial direction.

```go
// Step 4: SSTable tail seeding
// func (s SStable) IRange(start, end Bytes, order RangeOrder, seq ...uint64) (Iterator[Record], error) {
//   if order == RangeDesc {
//     offset, haveOffset := s.findOffsetLE(end)
//     if !haveOffset { return emptyIterator(), nil }
//     sri := &sstableIRange{offset: offset, cursor: offset, lowerBound: findLowerBound(start), order: RangeDesc}
//     if err := sri.primeDescending(); err != nil { return nil, err }
//     return sri, nil
//   }
//   ...
// }
// func (s *SStable) findOffsetLE(end Bytes) (int64, bool) {
//   // binary search SparseIndex for block starting key <= end
// }
// func (sri *sstableIRange) primeDescending() error {
//   // use PrevOffset to walk blocks/records until key < start or seq too new
// }
```

6. **Step 5 – Update regression coverage and documentation (Complexity 6/10).** We extend unit and integration suites to cover descending usage and make sure docs & CLI helpers describe the new flag.
   Coverage should include asc/desc parity checks and alternating direction smoke tests.

```go
// Step 5: verification assets
// - rindb_test.go: add desc cases mirroring TestRindb_IRangeDirections
// - integration/range_iterator_next_prev_test.go: table-driven asc vs desc checks
// - sstable_iteration_test.go & memtable_test.go: ensure Prev before Next works when order == RangeDesc
// - README + cmd/main.go usage banner: document the new order option
```

Pitfalls and test hooks: we will guard against regressions by pairing each risk with an existing or new test so failures surface early.
Maintaining this mapping clarifies how to validate every layer as we iterate on the detailed designs.

```go
// Pitfall: reverse seeding skips first record -> Add desc variant of TestSSTableIRange_ReverseIteration.
// Pitfall: duplicates during direction flip -> Extend integration/range_iterator_next_prev_test.go with desc oscillation.
// Pitfall: tombstones leaking -> Mirror TestMemtableIRange_Reverse under desc mode.
// Pitfall: invalid range inputs -> New unit tests for RangeOrder validation in rindb_test.go.
```
