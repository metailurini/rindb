# Step 2: Operation interface and handlers

## Plan
- Define a small `Operation` interface with `Apply(ctx, *Harness, PhaseLogger)` returning `(bool, error)` to unify begin/mutate/commit logic.
- Convert existing `applyPut`, `applyDel`, `applyGet`, `applyRange`, and `applySnap` helpers into structs implementing `Operation`.
- Keep idempotence by letting each handler decide whether it "committed" and increment the harness sequence only on true.
- Provide a factory to translate parsed `Op` structs into concrete `Operation` values so the fuzz generator can stay unchanged.
- Update `Harness.Step` to accept an `Operation`, invoke `Apply`, and handle errors uniformly.
- Make `Operation` implementations responsible for updating snapshot bookkeeping so `Harness` remains slim.
- Return a concrete error type for mismatches to aid invariant debugging.
- Expose a helper `wrapErr(op, phase, err)` to annotate errors with operation context.
- Supply table-driven tests per `Operation` to verify error propagation and logging.
- Document the concurrency assumption that operations run serially; guard shared state within `Harness` accordingly.
- Provide future extension point via `OperationName() string` if tracing or metrics need explicit names.

These changes isolate operational logic and simplify extensions like batch ops or tracing.
The snippet below sketches primary types and a factory for constructing operations from parsed input.

```go
// op.go
package diffharness

type Operation interface {
    Apply(ctx context.Context, h *Harness, log PhaseLogger) (committed bool, err error)
}

type PutOp struct{ K, V []byte }
func (p PutOp) Apply(ctx context.Context, h *Harness, log PhaseLogger) (bool, error) {
    if err := log.Log(Op{Kind: OpPut, K: p.K, V: p.V}, h.Seq, Phase("Begin")); err != nil {
        return false, err
    }
    if err := h.My.Put(ctx, p.K, p.V); err != nil {
        return false, err
    }
    if err := h.Ref.Put(ctx, p.K, p.V); err != nil {
        return false, err
    }
    log.Log(Op{Kind: OpPut, K: p.K, V: p.V}, h.Seq, Phase("Commit"))
    return true, nil
}

type DelOp struct{ K []byte }
func (d DelOp) Apply(ctx context.Context, h *Harness, log PhaseLogger) (bool, error) {
    log.Log(Op{Kind: OpDel, K: d.K}, h.Seq, Phase("Begin"))
    if err := h.My.Del(ctx, d.K); err != nil {
        return false, err
    }
    if err := h.Ref.Del(ctx, d.K); err != nil {
        return false, err
    }
    log.Log(Op{Kind: OpDel, K: d.K}, h.Seq, Phase("Commit"))
    return true, nil
}

func opFrom(o Op) Operation {
    switch o.Kind {
    case OpPut:
        return PutOp{K: o.K, V: o.V}
    case OpDel:
        return DelOp{K: o.K}
    // case OpGet, OpRange, OpSnap ...
    default:
        panic(fmt.Sprintf("unhandled op kind: %v", o.Kind))
    }
}
```
