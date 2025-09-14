# Step 6 plan: IteratorEngine interface

## Goal
Decouple invariants from the concrete `RinDBEngine` by introducing an `IteratorEngine` interface exposing `IterRange`.

## Steps
1. Define `IteratorEngine` in `diffharness/engine.go` with an `IterRange` method.
2. Implement the interface in `RinDBEngine` by forwarding to `IRange`.
3. Update invariants and harness tests to type-assert `h.My` to `IteratorEngine`.
4. Adjust any helpers to accept the interface and close iterators after use.
5. Document that the interface bypasses the higher-level `Engine` abstraction.

## Implementation sketch

```go
// diffharness/engine.go
type IteratorEngine interface {
    IterRange(ctx context.Context, lo, hi []byte, snap uint64) (*rindb.RangeIterator, error)
}

// diffharness/rindb_engine.go
func (e *RinDBEngine) IterRange(ctx context.Context, lo, hi []byte, snap uint64) (*rindb.RangeIterator, error) {
    return e.db.IRange(ctx, rindb.Bytes(lo), rindb.Bytes(hi), snap)
}

// diffharness/invariants.go
eng, ok := h.My.(IteratorEngine)
if !ok {
    return nil // skip if engine lacks IterRange
}

// diffharness/harness_test.go
it, err := h.My.IterRange(ctx, []byte("a"), []byte("z"), snap)
require.NoError(t, err)
defer it.Close()
```

## Notes
- The interface lets other engines participate in iterator tests without exposing concrete types.
- Keeping invariants generic reduces coupling and simplifies future engine integrations.
