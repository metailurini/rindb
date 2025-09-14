# Step 3 plan: Range iterator Next/Prev invariant

## Goal
Validate bidirectional iteration by comparing a random walk of `Next`/`Prev` calls against an oracle slice from SQLite.

## Steps
1. Sample `lo` and `hi`; resample `hi` until `lo < hi` to avoid empty ranges.
2. Collect the authoritative slice via `RangeWithSeq(lo, hi, snap, cfg.RangeMax)`.
3. Acquire the `RangeIterator` through the `IterRange` helper and ensure it is closed.
4. Walk the iterator up to `cfg.IterWalk` steps, randomly choosing `Next` or `Prev`.
5. Mirror the position within the oracle slice and compare keys/values after each move.
6. Handle boundary conditions by expecting `EOI` when stepping past the slice edges.

## Implementation sketch
The invariant uses the `IteratorEngine` interface to obtain the raw iterator and mirrors an index into the oracle slice.

```go
func checkRangeIterNextPrev(ctx context.Context, h *Harness, r *rand.Rand, cfg Cfg) error {
    eng, ok := h.My.(IteratorEngine)
    if !ok || h.Ref == nil {
        return nil
    }
    lo, hi := randKey(r, cfg.KeyLen), randKey(r, cfg.KeyLen)
    for bytes.Compare(hi, lo) <= 0 {
        hi = randKey(r, cfg.KeyLen)
    }
    snap := pickSnapshot(r, h.Seq, h.Snapshots, cfg.SnapshotReuseEvery)

    want, err := h.Ref.RangeWithSeq(lo, hi, snap, cfg.RangeMax)
    if err != nil {
        return err
    }

    it, err := eng.IterRange(ctx, lo, hi, snap)
    if err != nil {
        return err
    }
    defer it.Close()

    idx := 0
    for step := 0; step < cfg.IterWalk; step++ {
        if r.Intn(2) == 0 {
            rec, err := it.Next()
            if idx >= len(want) {
                if !errors.Is(err, EOI) { return fmt.Errorf("expected EOI") }
                continue
            }
            if err != nil { return err }
            if !bytes.Equal(rec.GetKey(), want[idx].Key) { return fmt.Errorf("key mismatch") }
            idx++
        } else {
            rec, err := it.Prev()
            if idx <= 0 {
                if !errors.Is(err, EOI) { return fmt.Errorf("expected EOI") }
                continue
            }
            if err != nil { return err }
            idx--
            if !bytes.Equal(rec.GetKey(), want[idx].Key) { return fmt.Errorf("key mismatch") }
        }
    }
    return nil
}
```

## Notes
- `cfg.IterWalk` keeps the walk short to maintain fast invariant runs.
- Resampling `hi` ensures `lo < hi`, avoiding spurious empty ranges.
