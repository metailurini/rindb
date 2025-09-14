# Step 4: Logging, crash, and telemetry hooks

## Plan
- Introduce a `PhaseLogger` interface and move existing phase printouts into implementations; keep `Harness` agnostic of output medium.
- Bundle crash and telemetry callbacks in a `HookSet`, allowing tests to inject failures or custom metrics collection.
- Embed `PhaseLogger` and `HookSet` into `Harness` and rewrite `Step` to call the hooks while keeping sequencing logic centralized.
- Provide a default no-op logger and hooks so basic fuzzing runs without extra wiring.
- Expose helpers for composing hooks (e.g., `WithCrash`, `WithTelemetry`) to keep setup declarative.
- Offer a `MultiLogger` helper so multiple loggers (e.g., console + JSON) can be chained.
- Clarify that crash hooks run after telemetry so failing hooks don't skip metrics.
- Supply `WithHooks(h HookSet)` to replace the entire hook set in tests.
- Add table-driven tests verifying order and nil safety of hooks.
- Document that hooks should return quickly to keep fuzzing throughput high.
- Reserve room for future metrics by letting `Telemetry` receive total op count.
- Show how to serialize logs in a stable format for regression triage.

These additions decouple side effects from the harness and permit richer observability without cluttering core logic.
The snippet outlines key interfaces, `Harness` fields, and a `Step` method invoking the hooks.

```go
// logger.go
package diffharness

type PhaseLogger interface {
    Log(op Op, seq uint64, phase Phase) error
}

type HookSet struct {
    Crash     func() error
    Telemetry func(seq uint64, ops int)
}

type Harness struct {
    // ... existing fields ...
    logger PhaseLogger
    hooks  HookSet
}

func (h *Harness) Step(ctx context.Context, op Operation) error {
    committed, err := op.Apply(ctx, h, h.logger)
    if err != nil {
        return err
    }
    if committed {
        h.Seq++
        if h.hooks.Telemetry != nil {
            h.hooks.Telemetry(h.Seq, h.ops)
        }
    }
    if h.hooks.Crash != nil {
        if err := h.hooks.Crash(); err != nil {
            return err
        }
    }
    return nil
}

var nopLogger PhaseLogger = PhaseLoggerFunc(func(Op, uint64, Phase) error { return nil })

type PhaseLoggerFunc func(Op, uint64, Phase) error
func (f PhaseLoggerFunc) Log(op Op, seq uint64, phase Phase) error { return f(op, seq, phase) }
```
