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
   - On direction change detected by `anchorState`, reset `recordFilter` dedupe state if necessary (e.g., `filter.resetKey()`).

5. **Testing focus**  
   - Update existing iterator tests to pass new collaborators.  
   - Add explicit direction-switch tests outlined in the main plan.

## Notes
- Ensure no exported API signatures change; keep `RangeIterator` type public behavior identical.
- Maintain existing error propagation from `MergingIterator` (if any) by surfacing errors through cursor methods.
