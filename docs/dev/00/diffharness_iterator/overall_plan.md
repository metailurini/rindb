# Diffharness iterator tests plan

## 1. Expose `IterRange` helper
Bypass `Engine.Range` to drive raw iterators.
```go
func (e *RinDBEngine) IterRange(ctx context.Context, lo, hi []byte, snap uint64) (*rindb.RangeIterator, error) {
    return e.db.IRange(ctx, rindb.Bytes(lo), rindb.Bytes(hi), snap)
}
```

## 2. Extend `Cfg` with `IterWalk`
Limit the number of iterator steps during invariants.
```go
type Cfg struct {
    // ...existing fields...
    IterWalk int // max elements to walk when testing iterators
}
```

## 3. Implement `checkRangeIterNextPrev` invariant
Alternate `Next`/`Prev` calls against an oracle slice.
Detailed plan: `step_3_range_iterator_invariant_plan.md`.

## 4. Add `TestHarness_RangeIteratorPrev`
Ensure walking forward then backward yields the original sequence.
```go
func TestHarness_RangeIteratorPrev(t *testing.T) {
    it, err := h.My.IterRange(ctx, []byte("a"), []byte("z"), h.Snap)
    require.NoError(t, err)
    forward := collectNext(it, 100)
    reverse := walkPrev(it, len(forward))
    require.Equal(t, reverseSlice(forward), reverse)
}
```

## 5. Validate `lo < hi`
Resample `hi` until it sorts after `lo` to avoid empty ranges. Folded into Step 3.

## 6. Introduce `IteratorEngine` interface
Decouple invariants from `RinDBEngine`.
Detailed plan: `step_6_iterator_engine_interface_plan.md`.

### Complexity & further planning
| Step | Complexity (1-10) | Dedicated plan? |
|------|-------------------|-----------------|
| 1 | 3 | No |
| 2 | 4 | No |
| 3 | 7 | Yes |
| 4 | 6 | No |
| 5 | 2 (folded into 3) | No |
| 6 | 5 | Yes |
