# Diffharness iterator tests plan

## 1. Expose iterator helper in `RinDBEngine`
Enable diffharness invariants to drive raw iterators by wrapping `IRange` in a helper.
```go
func (e *RinDBEngine) IterRange(ctx context.Context, lo, hi []byte, snap uint64) (*rindb.RangeIterator, error) {
    return e.db.IRange(ctx, rindb.Bytes(lo), rindb.Bytes(hi), snap)
}
```

## 2. Add invariant for alternating `Next`/`Prev`
Validate bidirectional iteration by comparing a random walk against the oracle sequence.
```go
var defaultInvariants = []Invariant{
    checkMonotonicReads,
    checkRangeConcat,
    checkRangeIterNextPrev,
}

func checkRangeIterNextPrev(ctx context.Context, h *Harness, r *rand.Rand, cfg Cfg) error {
    re, ok := h.My.(*RinDBEngine)
    if !ok || h.Ref == nil { return nil }
    lo, hi := randKey(r, cfg.KeyLen), randKey(r, cfg.KeyLen)
    snap := pickSnapshot(r, h.Seq, h.Snapshots, cfg.SnapshotReuseEvery)
    want, err := h.Ref.RangeWithSeq(lo, hi, snap, cfg.RangeMax)
    if err != nil {
        return err
    }
    it, err := re.IterRange(ctx, lo, hi, snap)
    if err != nil {
        return err
    }
    defer it.Close()
    // randomly call it.Next() or it.Prev() and compare with `want`
    return nil
}
```
Detailed plan: see `step_2_range_iterator_invariant_plan.md`.

## 3. Add harness test verifying reverse iteration
Ensure iterator walk reverses to the original sequence through a dedicated test.
```go
func TestHarness_RangeIteratorPrev(t *testing.T) {
    ctx := context.Background()
    h := newTestHarness(t)
    it, _ := h.My.(*RinDBEngine).IterRange(ctx, []byte("a"), []byte("z"), 0)
    // collect forward then backward results and compare
}
```

### Complexity & further planning
| Step | Complexity (1-10) | Dedicated plan? |
|------|-------------------|-----------------|
| 1    | 3                 | No              |
| 2    | 8                 | Yes             |
| 3    | 5                 | No              |
