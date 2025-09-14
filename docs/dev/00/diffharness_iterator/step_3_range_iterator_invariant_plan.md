# Step 3 plan: Range iterator Next/Prev invariant

## Goal
Validate bidirectional iteration by comparing a random walk of `Next`/`Prev` calls against an oracle slice from SQLite.
Skip when `h.Ref == nil`, `cfg.RangeMax <= 0`, or `cfg.IterWalk <= 0`.

## Steps
1. Sample `lo`/`hi`; resample `hi` until `lo < hi`. `hi` is exclusive.
2. Fetch an oracle slice with limit `max(cfg.RangeMax, cfg.IterWalk)` to avoid early `EOI`.
3. Acquire the `RangeIterator` through `IterRange` and ensure it is closed.
4. Walk up to `cfg.IterWalk` steps, randomly choosing `Next` or `Prev`.
5. Mirror the position within the oracle slice and compare keys/values after each move.
6. Break after two consecutive `EOI` responses to avoid spinning at the edges.

## Implementation sketch
The invariant uses the `IteratorEngine` interface to obtain the raw iterator and mirrors an index into the oracle slice.

```go
func checkRangeIterNextPrev(ctx context.Context, h *Harness, r *rand.Rand, cfg Cfg) error {
    eng, ok := h.My.(IteratorEngine)
    if !ok || h.Ref == nil || cfg.RangeMax <= 0 || cfg.IterWalk <= 0 {
        return nil
    }

    lo, hi := randKey(r, cfg.KeyLen), randKey(r, cfg.KeyLen)
    for bytes.Compare(hi, lo) <= 0 {
        hi = randKey(r, cfg.KeyLen)
    }
    snap := pickSnapshot(r, h.Seq, h.Snapshots, cfg.SnapshotReuseEvery)

    limit := max(cfg.RangeMax, cfg.IterWalk)
    want, err := h.Ref.RangeWithSeq(lo, hi, snap, limit)
    if err != nil {
        return err
    }

    it, err := eng.IterRange(ctx, lo, hi, snap)
    if err != nil {
        return err
    }
    defer it.Close()

    idx, eoi := 0, 0
    for step := 0; step < cfg.IterWalk && eoi < 2; step++ {
        if r.Intn(2) == 0 {
            rec, err := it.Next()
            if idx >= len(want) {
                if !errors.Is(err, rindb.EOI) { return fmt.Errorf("expected EOI") }
                eoi++
                continue
            }
            if err != nil { return err }
            if !bytes.Equal(rec.GetKey(), want[idx].K) || !bytes.Equal(rec.GetValue(), want[idx].V) {
                return fmt.Errorf("Next mismatch at %d", idx)
            }
            idx++
            eoi = 0
        } else {
            rec, err := it.Prev()
            if idx <= 0 {
                if !errors.Is(err, rindb.EOI) { return fmt.Errorf("expected EOI") }
                eoi++
                continue
            }
            if err != nil { return err }
            idx--
            if !bytes.Equal(rec.GetKey(), want[idx].K) || !bytes.Equal(rec.GetValue(), want[idx].V) {
                return fmt.Errorf("Prev mismatch at %d", idx)
            }
            eoi = 0
        }
    }
    return nil
}
```

## Notes
- `cfg.IterWalk` keeps the walk short to maintain fast invariant runs.
- Resampling `hi` ensures `lo < hi`, avoiding spurious empty ranges.
