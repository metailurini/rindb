# RangeIterator Coordination Refactor

## Objectives
- Implement `RangeIterator.advance` orchestration with cursor, filter, and anchor collaborators.
- Ensure `Next`, `Prev`, and `Last` paths share the same loop semantics while respecting direction-specific cursor pulls.

## Tasks
1. **Constructor wiring**  
   - Inject `rangeCursor`, `recordFilter`, and `anchorState` in `NewRangeIterator` (or equivalent builder).  
   - Ensure snapshot sequence is propagated into `recordFilter`.

2. **Shared advance loop**  
   - Implement `advance(dir Direction, pull func(*rangeCursor) (*Record, bool))` following the pseudo-code diff in the main plan.  
   - Loop until `recordFilter.Accept` succeeds or cursor exhausts; reuse for `Next`, `Prev`, and anchor replay.

3. **Method rewrites**  
   - Replace bodies of `Next`, `Prev`, and `Last` with `advance` invocations.  
   - `Prev` uses `DirReverse` with cursor reverse helper; `Last` seeds anchors/cursor appropriately before calling `advance`.

4. **State resets**
   - `anchorState.OnDirectionChange` returns a boolean when it stages the last emitted record as an anchor.
   - When that boolean is true, `RangeIterator.advance` must call `filter.Reset()` before replaying the staged anchor so the dedupe gate allows the anchor key, then call `filter.MarkEmitted(anchor)` to re-prime the dedupe state.

5. **Testing focus**  
   - Update existing iterator tests to pass new collaborators.  
   - Add explicit direction-switch tests outlined in the main plan.

## Notes
- Ensure no exported API signatures change; keep `RangeIterator` type public behavior identical.
- Maintain existing error propagation from `MergingIterator` (if any) by surfacing errors through cursor methods.
