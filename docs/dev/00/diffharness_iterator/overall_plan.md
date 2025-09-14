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
Randomly alternate `Next`/`Prev` against an oracle slice while
respecting iterator bounds. Skip when `h.Ref == nil`, `cfg.RangeMax <= 0`,
or `cfg.IterWalk <= 0`. Resample `hi` until `lo < hi` (exclusive) and use
`max(cfg.RangeMax, cfg.IterWalk)` to cap the oracle slice. Detailed plan:
`step_3_range_iterator_invariant_plan.md`.

## 4. Add `TestHarness_RangeIteratorPrev`
Ensure walking forward then backward yields the original sequence.
```go
func TestHarness_RangeIteratorPrev(t *testing.T) {
    it, err := h.My.IterRange(ctx, []byte("a"), []byte("z"), h.Snap)
    require.NoError(t, err)

    var forward [][]byte
    for step := 0; step < 100; step++ {
        rec, err := it.Next()
        if errors.Is(err, rindb.EOI) {
            break
        }
        require.NoError(t, err)
        forward = append(forward, slices.Clone(rec.GetKey()))
    }

    var backward [][]byte
    for step := 0; step < len(forward); step++ {
        rec, err := it.Prev()
        if errors.Is(err, rindb.EOI) {
            break
        }
        require.NoError(t, err)
        backward = append(backward, slices.Clone(rec.GetKey()))
    }

    slices.Reverse(forward)
    require.Equal(t, forward, backward)
    require.NoError(t, it.Close())
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
