# Step 6 plan: IteratorEngine interface

## Goal
Decouple invariants from the concrete `RinDBEngine` by introducing an `IteratorEngine` interface exposing `IterRange`.

## Steps
1. Define `IteratorEngine` in `diffharness/engine.go` with an `IterRange` method.
2. Implement the interface in `RinDBEngine` by forwarding to `IRange`.
3. Update invariants to assert `h.My` satisfies `IteratorEngine` instead of casting to `*RinDBEngine`.
4. Adjust tests or helpers that rely on iterator access to use the interface.
5. Document the interface to clarify it bypasses the top-level `Engine` abstraction.

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
    return nil // Skip invariant if not supported
}
```

## Notes
- The interface lets other engines participate in iterator tests without exposing concrete types.
- Keeping invariants generic reduces coupling and simplifies future engine integrations.
