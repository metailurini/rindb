# Step 1b – Iterator Tail Priming Plan

## Goals
Introduce a uniform way to position iterators at their last record so the
merging iterator can seed descending order without bespoke knowledge of each
child implementation.

## Context
* Descending range iteration requires the merging iterator to inspect the tail
  record of every child iterator when the range is first created.
* Today `Iterator[T]` only supports `Next`/`Prev`. Each implementation provides
  its own helper (e.g., `seekLT`, `preparePrev`) to reach the last record, and
  callers do not have a shared entry point.
* Adding a `Last`-style primitive modifies multiple iterators (`sstableIRange`,
  `memtableIRange`, `slIterator`, and future external iterators). We therefore
  keep this change isolated so the broader descending seeding work can assume a
  consistent API.

## Implementation Steps
1. **Extend the iterator contract.**
   * Update `iterator.go` so every iterator exposes a `Last()` helper.
   * The method should return the final record and position the iterator so the
     next `Prev` call yields the preceding record (mirroring how `Next` keeps the
     cursor past the returned item).
   * Surface `EOI` when the iterator is empty and forward existing errors.

   ```go
   // iterator.go
   type Iterator[T any] interface {
       HasNext() bool
       Next() (T, error)
       HasPrev() bool
       Prev() (T, error)
       // Last positions the iterator at the final element and returns it.
       // Implementations should behave as if the iterator just advanced to
       // the tail using their native primitives.
       Last() (T, error)
   }
   ```

2. **Teach foundational iterators how to reach the tail.**
   * **`slIterator`.** Reuse the skip list cursor helpers to locate the final
     node, return its value, and then rewind `curr` to the predecessor so a
     follow-up `Prev()` yields the penultimate element. This mirrors the way
     `Next()` advances past the returned record.
   * **`slIRange`.** Walk the range until the highest key `<= endKey` while
     continuing to respect the `startKey` guard. `Last()` must leave `curr`
     pointing to the predecessor of the returned node so `Prev()` retains its
     boundary-aware filtering.
   * **`memtableIRange`.** Delegate to the wrapped skip list iterator but pipe
     the result through the existing sequence/tombstone filtering loops used by
     `preparePrev()`. The goal is to surface the same logical record that `Prev`
     would choose while keeping `preparedNext`/`preparedPrev` bookkeeping in
     sync.
   * **`sstableIterator`.** Use the file offsets gathered during forward scans
     to position the reader at the final block (mirroring the `PrevOffset()`
     dance already present in `Prev`). Return that record and update `offset` so
     another `Prev()` walks to the previous entry without re-reading the tail.
   * **`sstableIRange`.** Reuse the `prepare()` loop so key-range and sequence
     filtering stay intact. `Last()` should respect `lowerBound`/`cursor`
     invariants by rewinding the iterator state machine exactly as the existing
     `Prev()` path expects.

3. **Adapt composed iterators and wrappers.**
   * **`MergingIterator`.** Implement `Last()` by leveraging the current
     forward/reverse heap choreography—drain the forward heap into the reverse
     heap until `preparePrev()` produces the maximal item, update
     `crossingAnchor`, set `forward` to `false`, and return that record while
     keeping both heaps ready for a subsequent `Prev()`.
   * **`RangeIterator`.** Feed `Last()` through `preparePrev()` so tombstone and
     duplicate suppression remains centralized. The method should set the anchor
     just like `Prev()` and toggle the iterator into reverse mode so another
     `Prev()` exposes the next visible record.
   * **Test doubles.** Update fixtures such as the `errIterator` in
     `merging_iterator_test.go` (and any future `Iterator[Record]` fakes) to grow
     a minimal `Last()` that iterates to the tail while preserving their
     failure-injection semantics.
   * **Wrapper helpers.** Audit adapters that embed other iterators—e.g., caching
     decorators or integration-test instrumentation that forward calls—to ensure
     they simply delegate `Last()` to the wrapped iterator. Call this out in
     helper docs so new wrappers remember to add the pass-through implementation.

4. **Update tests.**
   * Extend iterator-focused suites (`skiplist_test.go`, `memtable_test.go`,
     `sstable_iteration_test.go`) with `Last()` assertions that mirror the
     existing `Prev` expectations.
   * Add a unit test that wires a `MergingIterator` with fake children whose
     `Last()` implementations fail to confirm the error is surfaced during
     construction.

## Pitfalls & Validation
* **Skipping tombstone filtering.** `Last()` must pass through the same sequence
  and delete handling performed by `Prev` so merging iterators never see
  shadowed entries. Guard this with a memtable regression test that verifies the
  returned tail record respects visibility rules.
* **Leaking resources when Last fails.** Implementations that open temporary
  readers (e.g., `sstableIRange`) should reuse existing cleanup code paths so the
  upcoming merging iterator seeding can rely on the shared `cleanup` hook for
  failure handling.
* **Divergent iterator semantics.** Document the expectation that calling
  `Last()` immediately followed by `Prev()` returns the same element, keeping the
  iteration contract symmetric with `Next()`.
