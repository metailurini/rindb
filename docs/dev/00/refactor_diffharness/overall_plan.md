# Diffharness refactor plan

## 1. Split core types and harness struct into `types.go`
Consolidate `OpKind`, `Op`, `Phase`, `Cfg`, and a trimmed `Harness` struct in a dedicated file to separate domain data from logic.
```go
// types.go
package diffharness

type OpKind uint8
const (
    OpPut OpKind = iota
    OpDel
    OpGet
    OpRange
    OpSnap
)

type Op struct {
    Kind    OpKind
    K, V    []byte
    Lo, Hi  []byte
    SnapSeq uint64
    Limit   int
}

type Phase string

type Cfg struct {
    KeyLen, ValLenMin, ValLenMax, RangeMax int
    Weights                                 map[OpKind]int
    CrashEvery, TelemetryEvery              int
}

type Harness struct {
    Seed int64
    My   Engine
    Ref  *SQLiteOracle
    Seq       uint64
    Snapshots []uint64
}
```

## 2. Introduce an `Operation` interface and handlers
Replace `applyPut`, `applyDel`, etc. with structs implementing a common `Operation` interface. Centralize phase handling and logging inside `Operation.Apply`.
```go
// op.go
package diffharness

type Operation interface {
     Apply(ctx context.Context, h *Harness, log PhaseLogger) (committed bool, err error)
 }

 type PutOp struct { K, V []byte }
 func (p PutOp) Apply(ctx context.Context, h *Harness, log PhaseLogger) (bool, error) {
     // begin, mutate, commit, log phases
     return true, nil
 }

 type DelOp struct { K []byte }
 func (d DelOp) Apply(ctx context.Context, h *Harness, log PhaseLogger) (bool, error) {
     return true, nil
 }

 // Additional ops: GetOp, RangeOp, SnapOp
```
Detailed plan: see `step_2_operation_interface_plan.md`.

## 3. Extract randomness and key tracking into `generator.go`
Isolate fuzzing helpers so the harness focuses on orchestration rather than random data generation.
```go
// generator.go
package diffharness

type KeyTracker struct {
    keys   []string
    keySet map[string]struct{}
}
func (kt *KeyTracker) Add(k []byte) { /* ... */ }
func (kt *KeyTracker) Del(k []byte) { /* ... */ }
func (kt *KeyTracker) Random(r *rand.Rand) []byte { /* ... */ }

type RandOps struct { cfg Cfg }
func (ro RandOps) Next(r *rand.Rand, kt *KeyTracker) Operation { /* ... */ }
```

## 4. Isolate logging, crash, and telemetry hooks
Move `phaseLogger`, crash, and telemetry callbacks into a dedicated `logger.go`, allowing `Harness.Step` to delegate side effects.
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

 func (h *Harness) Step(ctx context.Context, op Operation) error {
    committed, err := op.Apply(ctx, h, h.logger)
    if err != nil { return err }
    if committed { h.Seq++ }
    return nil
}
```
Detailed plan: see `step_4_logging_hooks_plan.md`.

## 5. Move invariant checks into `invariants.go`
Create composable invariant functions and have `run` iterate over them, improving readability and testability.
```go
// invariants.go
package diffharness

type Invariant func(ctx context.Context, h *Harness) error

func checkMonotonicReads(ctx context.Context, h *Harness) error { /* ... */ }
func checkRangeConcat(ctx context.Context, h *Harness) error  { /* ... */ }

var defaultInvariants = []Invariant{
    checkMonotonicReads,
    checkRangeConcat,
}
```

### Complexity & further planning
| Step | Complexity (1-10) | Dedicated plan? |
|------|-------------------|-----------------|
| 1    | 4                 | No              |
| 2    | 8                 | Yes             |
| 3    | 5                 | No              |
| 4    | 7                 | Yes             |
| 5    | 3                 | No              |
