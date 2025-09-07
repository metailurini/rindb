package fuzzing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
)

// OpKind enumerates supported operation types.
type OpKind uint8

const (
	OpPut OpKind = iota
	OpDel
	OpGet
	OpRange
	OpSnap
)

// Op models a single fuzzing operation.
type Op struct {
	Kind    OpKind
	K       []byte
	V       []byte
	Lo      []byte
	Hi      []byte
	SnapSeq uint64
	Limit   int
}

// Harness coordinates both engines and records every executed operation.
type Harness struct {
	Seed int64

	My  Engine
	Ref *SQLiteOracle

	Seq       uint64
	Snapshots []uint64

	log *os.File
	enc *json.Encoder
	ops int

	crash     func() error
	telemetry func(seq uint64, ops int)
}

// Cfg controls random operation generation.
type Cfg struct {
	KeyLen    int
	ValLenMin int
	ValLenMax int
	RangeMax  int
	Weights   map[OpKind]int

	CrashEvery     int
	TelemetryEvery int
}

// NewHarness creates a harness with the given engines and log file path.
func NewHarness(my Engine, ref *SQLiteOracle, seed int64, logPath string) (*Harness, error) {
	f, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	return &Harness{My: my, Ref: ref, Seed: seed, log: f, enc: json.NewEncoder(f)}, nil
}

// Close closes underlying resources.
func (h *Harness) Close() error { return h.log.Close() }

func randBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	_, _ = r.Read(b)
	return b
}

func randKey(r *rand.Rand, n int) []byte { return randBytes(r, n) }

func (h *Harness) genOp(r *rand.Rand, cfg Cfg) Op {
	sum := 0
	for _, w := range cfg.Weights {
		sum += w
	}
	x := r.Intn(sum)
	var k OpKind
	for kind, w := range cfg.Weights {
		if x < w {
			k = kind
			break
		}
		x -= w
	}
	switch k {
	case OpPut:
		vlen := cfg.ValLenMin + r.Intn(cfg.ValLenMax-cfg.ValLenMin+1)
		return Op{Kind: OpPut, K: randKey(r, cfg.KeyLen), V: randBytes(r, vlen)}
	case OpDel:
		return Op{Kind: OpDel, K: randKey(r, cfg.KeyLen)}
	case OpGet:
		s := h.pickSnapshot(r)
		return Op{Kind: OpGet, K: randKey(r, cfg.KeyLen), SnapSeq: s}
	case OpRange:
		lo := randKey(r, cfg.KeyLen)
		hi := randKey(r, cfg.KeyLen)
		for bytes.Compare(hi, lo) <= 0 {
			hi = randKey(r, cfg.KeyLen)
		}
		s := h.pickSnapshot(r)
		return Op{Kind: OpRange, Lo: lo, Hi: hi, SnapSeq: s, Limit: cfg.RangeMax}
	case OpSnap:
		return Op{Kind: OpSnap}
	default:
		panic("unknown op kind")
	}
}

func (h *Harness) pickSnapshot(r *rand.Rand) uint64 {
	if len(h.Snapshots) == 0 || r.Intn(10) == 0 {
		return h.Seq
	}
	return h.Snapshots[r.Intn(len(h.Snapshots))]
}

// SetCrashHook registers a callback invoked after CrashEvery operations.
// The callback should close and reopen both engines to simulate recovery.
func (h *Harness) SetCrashHook(fn func() error) { h.crash = fn }

// SetTelemetryHook registers a callback executed every TelemetryEvery ops.
// It receives the current sequence and op count.
func (h *Harness) SetTelemetryHook(fn func(seq uint64, ops int)) { h.telemetry = fn }

// Step applies a single operation, logging it before execution.
func (h *Harness) Step(ctx context.Context, op Op) error {
	if err := h.enc.Encode(struct {
		I   int    `json:"i"`
		Seq uint64 `json:"seq"`
		Op  Op     `json:"op"`
	}{h.ops, h.Seq, op}); err != nil {
		return err
	}
	i := h.ops
	h.ops++

	switch op.Kind {
	case OpPut:
		h.Seq++
		if err := h.My.Put(ctx, op.K, op.V); err != nil {
			return h.fail(i, op, err)
		}
		if h.Ref != nil {
			if err := h.Ref.PutWithSeq(op.K, op.V, h.Seq); err != nil {
				return h.fail(i, op, err)
			}
		}
	case OpDel:
		h.Seq++
		if err := h.My.Delete(ctx, op.K); err != nil {
			return h.fail(i, op, err)
		}
		if h.Ref != nil {
			if err := h.Ref.DelWithSeq(op.K, h.Seq); err != nil {
				return h.fail(i, op, err)
			}
		}
	case OpGet:
		mv, mok, me := h.My.Get(ctx, op.K, op.SnapSeq)
		if me != nil {
			return h.fail(i, op, me)
		}
		if h.Ref != nil {
			sv, sok, se := h.Ref.GetWithSeq(op.K, op.SnapSeq)
			if se != nil {
				return h.fail(i, op, se)
			}
			if mok != sok || !bytes.Equal(mv, sv) {
				return h.mismatch(i, op, mv, mok, sv, sok)
			}
		}
	case OpRange:
		mres, me := h.My.Range(ctx, op.Lo, op.Hi, op.SnapSeq, op.Limit)
		if me != nil {
			return h.fail(i, op, me)
		}
		if h.Ref != nil {
			sres, se := h.Ref.RangeWithSeq(op.Lo, op.Hi, op.SnapSeq, op.Limit)
			if se != nil {
				return h.fail(i, op, se)
			}
			if err := compareKVLists(mres, sres); err != nil {
				return h.fail(i, op, err)
			}
		}
	case OpSnap:
		h.Snapshots = append(h.Snapshots, h.Seq)
	}
	return nil
}

func (h *Harness) run(ctx context.Context, r *rand.Rand, cfg Cfg, n int) error {
	h.Snapshots = append(h.Snapshots, h.Seq)
	for i := 0; n < 0 || i < n; i++ {
		op := h.genOp(r, cfg)
		if err := h.Step(ctx, op); err != nil {
			return err
		}
		if err := h.checkInvariants(ctx, r, cfg); err != nil {
			return h.fail(h.ops, Op{Kind: 255}, err)
		}
		if cfg.TelemetryEvery > 0 && h.telemetry != nil && h.ops%cfg.TelemetryEvery == 0 {
			h.telemetry(h.Seq, h.ops)
		}
		if cfg.CrashEvery > 0 && h.crash != nil && h.ops%cfg.CrashEvery == 0 {
			if err := h.crash(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Run executes n randomized operations. If n < 0, it runs indefinitely.
func (h *Harness) Run(ctx context.Context, cfg Cfg, n int) error {
	r := rand.New(rand.NewSource(h.Seed))
	return h.run(ctx, r, cfg, n)
}

// RunForever starts the fuzz loop and never returns unless an error occurs.
func (h *Harness) RunForever(ctx context.Context, cfg Cfg) error {
	return h.Run(ctx, cfg, -1)
}

func (h *Harness) fail(i int, op Op, cause error) error {
	_ = h.log.Sync()
	return fmt.Errorf("fuzz fail at i=%d seq=%d kind=%d: %w", i, h.Seq, op.Kind, cause)
}

func (h *Harness) mismatch(i int, op Op, mv []byte, mok bool, sv []byte, sok bool) error {
	return h.fail(i, op, fmt.Errorf("mismatch my=%v ref=%v", mok, sok))
}

func compareKVLists(a, b []KV) error {
	if len(a) != len(b) {
		return fmt.Errorf("length mismatch %d vs %d", len(a), len(b))
	}
	for i := range a {
		if !bytes.Equal(a[i].K, b[i].K) || !bytes.Equal(a[i].V, b[i].V) {
			return fmt.Errorf("kv mismatch at %d", i)
		}
	}
	return nil
}

func (h *Harness) checkInvariants(ctx context.Context, r *rand.Rand, cfg Cfg) error {
	if h.Ref == nil {
		return nil
	}

	if len(h.Snapshots) == 0 {
		return nil
	}

	// Monotonic reads: pick k and two snapshots s1 <= s2
	k := randKey(r, cfg.KeyLen)
	s1 := h.Snapshots[r.Intn(len(h.Snapshots))]
	s2 := h.Snapshots[r.Intn(len(h.Snapshots))]
	if s1 > s2 {
		s1, s2 = s2, s1
	}
	mv1, mok1, err := h.My.Get(ctx, k, s1)
	if err != nil {
		return err
	}
	mv2, mok2, err := h.My.Get(ctx, k, s2)
	if err != nil {
		return err
	}
	sv1, sok1, err := h.Ref.GetWithSeq(k, s1)
	if err != nil {
		return err
	}
	sv2, sok2, err := h.Ref.GetWithSeq(k, s2)
	if err != nil {
		return err
	}
	if mok1 != sok1 || !bytes.Equal(mv1, sv1) {
		return fmt.Errorf("monotonic: s1 mismatch")
	}
	if mok2 != sok2 || !bytes.Equal(mv2, sv2) {
		return fmt.Errorf("monotonic: s2 mismatch")
	}

	// Range concatenation: [lo,mid) + [mid,hi) == [lo,hi)
	if cfg.RangeMax > 0 {
		lo := randKey(r, cfg.KeyLen)
		hi := randKey(r, cfg.KeyLen)
		for bytes.Compare(hi, lo) <= 0 {
			hi = randKey(r, cfg.KeyLen)
		}
		mid := randKey(r, cfg.KeyLen)
		for bytes.Compare(mid, lo) <= 0 || bytes.Compare(mid, hi) >= 0 {
			mid = randKey(r, cfg.KeyLen)
		}
		snap := h.pickSnapshot(r)
		left, err := h.My.Range(ctx, lo, mid, snap, cfg.RangeMax)
		if err != nil {
			return err
		}
		right, err := h.My.Range(ctx, mid, hi, snap, cfg.RangeMax)
		if err != nil {
			return err
		}
		full, err := h.My.Range(ctx, lo, hi, snap, cfg.RangeMax*2)
		if err != nil {
			return err
		}
		concat := append(append([]KV{}, left...), right...)
		if err := compareKVLists(concat, full); err != nil {
			return fmt.Errorf("range concat: %w", err)
		}
		// also compare with reference for coverage
		l2, err := h.Ref.RangeWithSeq(lo, mid, snap, cfg.RangeMax)
		if err != nil {
			return err
		}
		if err := compareKVLists(left, l2); err != nil {
			return fmt.Errorf("range left mismatch: %w", err)
		}
		r2, err := h.Ref.RangeWithSeq(mid, hi, snap, cfg.RangeMax)
		if err != nil {
			return err
		}
		if err := compareKVLists(right, r2); err != nil {
			return fmt.Errorf("range right mismatch: %w", err)
		}
		f2, err := h.Ref.RangeWithSeq(lo, hi, snap, cfg.RangeMax*2)
		if err != nil {
			return err
		}
		if err := compareKVLists(full, f2); err != nil {
			return fmt.Errorf("range full mismatch: %w", err)
		}
	}
	return nil
}
