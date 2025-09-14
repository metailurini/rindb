# Step 2 plan: Range iterator Next/Prev invariant

## Goal
Validate bidirectional iteration by comparing a random walk of `Next`/`Prev` calls against an oracle sequence produced by SQLite.

## Steps
1. Gather the full key/value slice from SQLite via `RangeWithSeq` for `[lo, hi)` at a chosen snapshot.
   - This provides the authoritative ordering for both forward and reverse walks.
2. Acquire the `RangeIterator` using `IterRange` and ensure it is closed after the walk.
   - The helper bypasses the Engine interface so invariants can drive raw iterators.
3. Execute a bounded number of random `Next` or `Prev` calls, updating an index over the oracle slice.
   - Mix directions to surface cursor state bugs and boundary handling.
4. After each call, compare returned records and handle `EOI` when the index crosses slice boundaries.
   - Report mismatches immediately to preserve debug context.
5. Start the walk at index `0` to trigger edge cases such as `Prev` before any `Next` and direction changes at range limits.

## Implementation sketch
The invariant mirrors the index into the oracle slice and validates each iterator step.

```go
func checkRangeIterNextPrev(ctx context.Context, h *Harness, r *rand.Rand, cfg Cfg) error {
    re := h.My.(*RinDBEngine)
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

    idx := 0
    for step := 0; step < cfg.IterWalk; step++ {
        if r.Intn(2) == 0 {
            rec, err := it.Next()
            if idx >= len(want) {
                if !errors.Is(err, EOI) {
                    return fmt.Errorf("expected EOI")
                }
                continue
            }
            if err != nil {
                return err
            }
            if !bytes.Equal(rec.GetKey(), want[idx].Key) {
                return fmt.Errorf("key mismatch")
            }
            idx++
        } else {
            rec, err := it.Prev()
            if idx <= 0 {
                if !errors.Is(err, EOI) {
                    return fmt.Errorf("expected EOI")
                }
                continue
            }
            idx--
            if err != nil {
                return err
            }
            if !bytes.Equal(rec.GetKey(), want[idx].Key) {
                return fmt.Errorf("key mismatch")
            }
        }
    }
    return nil
}
```

## Notes
- `cfg.IterWalk` controls walk length and should be small to avoid lengthy invariant checks.
- Using index arithmetic allows forward and reverse comparison without needing a reverse oracle iterator.
