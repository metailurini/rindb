# Goals
- Decompose `RangeIterator` into composable collaborators (cursor, filter, anchor state) while preserving the existing `Iterator[Record]` contract.
- Provide clear direction-aware mechanics that make ASC/DESC traversal symmetrical and maintainable without proliferating `RangeOrder` branching.
- Ensure snapshot visibility, deduplication, and tombstone suppression remain correct under forward/backward iteration and direction switches.

# Context
The current `RangeIterator` mixes cursor staging, key deduplication, direction transitions, and tombstone filtering. This coupling makes the iterator hard to reason about and risks regressions whenever traversal semantics change. Refactoring toward smaller components (direction enum, cursor abstraction, record filter, anchor manager) will isolate concerns and enable easier validation of reverse traversal and snapshot visibility.

# Implementation Steps
1. **Introduce local `Direction` enum and shared helpers**  
   - *Rationale*: Encapsulate traversal direction without leaking `RangeOrder` everywhere. Helpers can switch behavior based on direction instead of order-specific branches.  
   - *Code*:
     ```diff
     + type Direction int
     +
     + const (
     +     DirForward Direction = iota
     +     DirReverse
     + )
     +
     + func directionFromOrder(order RangeOrder) Direction {
     +     if order == RangeAsc {
     +         return DirForward
     +     }
     +     return DirReverse
     + }
     ```
   - *Rollout Notes*: Update only new components to consume `Direction`; keep public iterator signature unchanged.  
   - *Complexity*: 3

2. **Extract `rangeCursor` to wrap `MergingIterator` quirks**  
   - *Rationale*: Isolate peek/staging logic for DESC scans and version collapsing so `RangeIterator` no longer micromanages `MergingIterator`.  
   - *Code*:
     ```diff
     + type rangeCursor struct {
     +     it *MergingIterator
     +     staged *Record
     + }
     +
     + func newRangeCursor(mi *MergingIterator) *rangeCursor { return &rangeCursor{it: mi} }
     +
     + func (c *rangeCursor) next(dir Direction) (*Record, bool) {
     +     if dir == DirReverse {
     +         return c.nextReverse()
     +     }
     +     return c.nextForward()
     + }
     ```
   - *Rollout Notes*: Provide symmetric `nextForward`, `nextReverse`, and `stageReverse` helpers so callers never branch on order. Integration with `MergingIterator` remains local to this type.  
   - *Complexity*: 6

3. **Implement `recordFilter` for visibility and dedup**  
   - *Rationale*: Centralize key deduplication, tombstone skipping, and snapshot sequence validation to avoid scattered checks.  
   - *Code*:
    ```diff
    + type recordFilter struct {
    +     lastKey []byte
    +     lastDir Direction
    +     snapshot sequence.Sequence
    + }
    +
    + func (f *recordFilter) Accept(rec *Record, dir Direction) (*Record, bool) {
    +     if rec == nil {
    +         return nil, false
    +     }
    +     if dir != f.lastDir {
    +         f.lastKey = f.lastKey[:0]
    +         f.lastDir = dir
    +     }
    +     if isTombstone(rec) || rec.Seq > f.snapshot {
    +         return nil, false
    +     }
    +     if bytes.Equal(rec.Key, f.lastKey) {
    +         return nil, false
    +     }
    +     f.lastKey = append(f.lastKey[:0], rec.Key...)
    +     return rec, true
    + }
    +
    + func (f *recordFilter) Reset() {
    +     f.lastKey = f.lastKey[:0]
    + }
    +
    + func (f *recordFilter) MarkEmitted(rec *Record) {
    +     if rec == nil {
    +         return
    +     }
    +     f.lastKey = append(f.lastKey[:0], rec.Key...)
    + }
    ```
  - *Rollout Notes*: `RangeIterator` invokes `Reset` on direction change before replaying the anchor so `Accept` can permit the replayed key. `MarkEmitted` keeps dedupe state consistent for anchors that bypass filtering.
   - *Complexity*: 5

4. **Add `anchorState` to manage direction switches**  
   - *Rationale*: Keep track of staged anchors when direction changes so previously peeked records can be replayed without duplicating logic in iterator methods.  
   - *Code*:
    ```diff
    + type anchorState struct {
    +     pending *Record
    +     lastDir Direction
    + }
    +
    + func (a *anchorState) OnDirectionChange(next Direction, lastEmitted *Record) (changed bool) {
    +     if a.lastDir == next {
    +         return false
    +     }
    +     a.lastDir = next
    +     a.pending = cloneRecord(lastEmitted)
    +     return true
    + }
    +
    + func (a *anchorState) popPending() (*Record, bool) {
    +     if a.pending == nil {
    +         return nil, false
    +     }
    +     rec := a.pending
    +     a.pending = nil
    +     return rec, true
    + }
    ```
  - *Rollout Notes*: `OnDirectionChange` is invoked with the last emitted record before pulling new cursor data so the anchor is staged in time for the next `advance` call. `cloneRecord` copies key/value buffers to decouple the staged anchor from future cursor reuse.
   - *Complexity*: 4

5. **Refactor `RangeIterator` as coordinator using new collaborators**  
   - *Rationale*: Simplify `Next`, `Prev`, and `Last` to orchestrate cursor, filter, and anchor state, eliminating duplicated ASC/DESC logic.  
   - *Code*:
    ```diff
    - func (ri *RangeIterator) Next() (*Record, bool) {
    -     // existing combined logic...
    - }
    + func (ri *RangeIterator) Next() (*Record, bool) {
    +     return ri.advance(DirForward, func(c *rangeCursor) (*Record, bool) {
    +         return c.next(DirForward)
    +     })
    + }
    +
    + func (ri *RangeIterator) advance(dir Direction, pull func(*rangeCursor) (*Record, bool)) (*Record, bool) {
    +     if changed := ri.anchors.OnDirectionChange(dir, ri.lastEmitted); changed {
    +         ri.filter.Reset()
    +         if rec, ok := ri.anchors.popPending(); ok {
    +             ri.filter.MarkEmitted(rec)
    +             ri.lastEmitted = rec
    +             return rec, true
    +         }
    +     }
    +     if rec, ok := ri.anchors.popPending(); ok {
    +         ri.filter.MarkEmitted(rec)
    +         ri.lastEmitted = rec
    +         return rec, true
    +     }
    +     for {
    +         rec, ok := pull(ri.cursor)
    +         if !ok {
    +             return nil, false
    +         }
    +         if accepted, ok := ri.filter.Accept(rec, dir); ok {
    +             ri.lastEmitted = accepted
    +             return accepted, true
    +         }
    +     }
    + }
    ```
  - *Rollout Notes*: Update constructor to inject collaborators, persist `lastEmitted` inside `RangeIterator`, wire `Prev`/`Last` through `advance`, and ensure tests cover direction flips. Detailed coordination plan is in [`01_range_iterator_coordinator.md`](./01_range_iterator_coordinator.md).
   - *Complexity*: 8 ➜ see linked sub-plan.

# Pitfalls & Validation
- **Risk**: Incorrect deduplication when switching directions could re-emit stale versions.  
  - *Tests*: Add `TestRangeIterator_DirectionFlipDedup` exercising forward→backward traversal over duplicate keys to ensure only newest version per direction emits.  
- **Risk**: Tombstones or snapshot bounds bypass filter due to sequencing mistakes.  
  - *Tests*: Extend `TestRangeIterator_SnapshotVisibility` with a tombstoned key and snapshot older/newer than the delete to assert visibility boundaries.  
- **Risk**: Anchor replay may miss staged records after switching direction.  
  - *Tests*: Create `TestRangeIterator_AnchorReplay` ensuring pending record from previous direction is emitted first after flip; include negative case where no anchor exists.
- **Regression Guard**: Re-run existing `range_test` and `merging_iterator` suites to confirm parity.  
- **Invalidation**: Add fuzz-style test `TestRangeIterator_FuzzDirectionSwitch` to stress random direction flips and confirm no panics or duplicates.

# Draft end-state sketch
```
RangeIterator
  ├── direction Direction
  ├── cursor    *rangeCursor   // wraps MergingIterator
  ├── filter    recordFilter   // handles visibility
  ├── anchors   anchorState    // manages direction flips
  └── lastEmitted *Record      // staged for anchor replay

Next/Prev/Last:
  rec := anchors.popPending() ?? cursor.next(direction)
  if filter.Accept(rec, direction): emit
  else: loop
```
