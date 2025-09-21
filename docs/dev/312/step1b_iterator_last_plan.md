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
   * `slIterator` already has the skip list cursor helpers necessary to jump to
     the final node. Wrap that logic inside `Last()` so `Prev` continues from the
     returned element.
   * `memtableIRange` should delegate to the underlying skip list iterator while
     keeping sequence/tombstone filtering intact.
   * `sstableIRange` should binary search the sparse index to locate the last
     block whose starting key is less than or equal to `end` (mirroring the
     `IRange` startup logic) and then walk backwards with `PrevOffset` until the
     terminal record is in range.
     Cross-reference the `sstable_iteration.go` state machine so `Last()` seeds
     `offset`, `cursor`, and `haveLowerBound` exactly as `Prev` expects before the
     first reverse step.

3. **Adapt composed iterators.**
   * The step 2 merging iterator plan will call `child.Last()` while seeding the
     reverse heap. During this step, update any other iterator wrappers (e.g.,
     caching adapters or integration test doubles) to expose the new method and
     delegate to their inner iterator.
   * Ensure helper constructors used in tests (such as `fakeIterator` fixtures)
     grow a trivial `Last()` implementation so existing coverage keeps compiling.

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
* **Large SSTables.** Avoid rescanning the entire table when priming the tail;
  rely on the sparse index + `PrevOffset` walk so `Last()` remains logarithmic in
  table size even for multi-gigabyte files.
