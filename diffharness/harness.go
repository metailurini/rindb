package diffharness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
)

// NewHarness creates a harness with the given engines and log file path.
func NewHarness(my Engine, ref *SQLiteOracle, seed int64, logPath string) (*Harness, error) {
	l, err := newJSONLogger(logPath)
	if err != nil {
		return nil, err
	}
	return &Harness{My: my, Ref: ref, Seed: seed, logger: l, invariants: defaultInvariants}, nil
}

// Close closes underlying resources.
func (h *Harness) Close() error {
	_ = h.releaseSnapshots(context.Background(), true)
	if c, ok := h.logger.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// randomness helpers moved to generator.go

// Step applies a single operation and invokes hooks.
func (h *Harness) Step(ctx context.Context, op Operation) (bool, error) {
	committed, err := op.Apply(ctx, h, h.logger)
	if err != nil {
		return false, err
	}
	if committed {
		h.Seq++
	}
	h.ops++
	if h.hooks.Telemetry != nil {
		h.hooks.Telemetry(h.Seq, h.ops)
	}
	if h.hooks.Crash != nil {
		if err := h.hooks.Crash(h.ops); err != nil {
			return committed, err
		}
	}
	return committed, nil
}

func (h *Harness) releaseSnapshots(ctx context.Context, logErrors bool) error {
	if len(h.Snapshots) == 0 {
		return nil
	}
	for _, s := range h.Snapshots[1:] {
		if err := h.My.ReleaseSnapshot(ctx, s); err != nil {
			if logErrors {
				_, _ = fmt.Fprintf(os.Stderr, "diffharness: failed to release 'My' snapshot %d: %v\n", s, err)
			} else {
				return err
			}
		}
		if h.Ref != nil {
			if err := h.Ref.ReleaseSnapshot(ctx, s); err != nil {
				if logErrors {
					_, _ = fmt.Fprintf(os.Stderr, "diffharness: failed to release 'Ref' snapshot %d: %v\n", s, err)
				} else {
					return err
				}
			}
		}
	}
	h.Snapshots = h.Snapshots[:1]
	return nil
}

func (h *Harness) run(ctx context.Context, r *rand.Rand, cfg Cfg, n int) error {
	h.Snapshots = append(h.Snapshots, h.Seq)
	kt := KeyTracker{}
	ro := RandOps{cfg: cfg}
	for i := 0; n < 0 || i < n; i++ {
		op := ro.Next(r, &kt, h.Seq, h.Snapshots)
		committed, err := h.Step(ctx, op)
		if err != nil {
			return err
		}
		if committed {
			switch t := op.(type) {
			case PutOp:
				kt.Add(t.K)
			case DelOp:
				kt.Del(t.K)
			}
		}
		for _, inv := range h.invariants {
			if err := inv(ctx, h, r, cfg); err != nil {
				return h.fail(h.ops, Op{Kind: OpInvariantCheck}, err)
			}
		}
		if len(h.Snapshots) > 1 && r.Intn(10) == 0 {
			idx := 1 + r.Intn(len(h.Snapshots)-1)
			seq := h.Snapshots[idx]
			if err := h.My.ReleaseSnapshot(ctx, seq); err != nil {
				return err
			}
			if h.Ref != nil {
				if err := h.Ref.ReleaseSnapshot(ctx, seq); err != nil {
					return err
				}
			}
			h.Snapshots = append(h.Snapshots[:idx], h.Snapshots[idx+1:]...)
		}
	}
	return h.releaseSnapshots(ctx, false)
}

// Run executes n randomized operations. If n < 0, it runs indefinitely.
func (h *Harness) Run(ctx context.Context, cfg Cfg, n int) error {
	r := rand.New(rand.NewSource(h.Seed))
	return h.run(ctx, r, cfg, n)
}

// RunForever starts the fuzz loop and never returns unless an error occurs.
func (h *Harness) RunForever(ctx context.Context, cfg Cfg) error { return h.Run(ctx, cfg, -1) }

func (h *Harness) fail(i int, op Op, cause error) error {
	return fmt.Errorf("fuzz fail at i=%d seq=%d kind=%d: %w", i, h.Seq, op.Kind, cause)
}

// Replay replays operations from logPath against the provided engines.
// It returns the final sequence after applying all operations.
func Replay(ctx context.Context, my Engine, ref *SQLiteOracle, logPath string) (uint64, error) {
	f, err := os.OpenFile(logPath, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	type logEntry struct {
		I     int    `json:"i"`
		Seq   uint64 `json:"seq"`
		Op    Op     `json:"op"`
		Phase Phase  `json:"phase"`
	}
	var entries []logEntry
	for {
		var e logEntry
		if err := dec.Decode(&e); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, err
		}
		entries = append(entries, e)
	}
	if len(entries) == 0 {
		return 0, nil
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return 0, err
	}
	enc := json.NewEncoder(f)
	write := func(e logEntry) error {
		if err := enc.Encode(e); err != nil {
			return err
		}
		return f.Sync()
	}
	last := entries[len(entries)-1]

	// Reapply committed mutations to the reference engine in case it lost
	// state during a crash. Inserts use OR IGNORE semantics so replays are
	// idempotent.
	for _, e := range entries {
		if e.Phase != PhaseCommitted {
			continue
		}
		switch e.Op.Kind {
		case OpPut:
			if ref != nil {
				if err := ref.PutWithSeq(e.Op.K, e.Op.V, e.Seq+1); err != nil {
					return 0, err
				}
			}
		case OpDel:
			if ref != nil {
				if err := ref.DelWithSeq(e.Op.K, e.Seq+1); err != nil {
					return 0, err
				}
			}
		}
	}

	finalize := func(entry logEntry) (uint64, error) {
		logger := PhaseLoggerFunc(func(o Op, seq uint64, p Phase, _ int) error {
			return write(logEntry{I: entry.I, Seq: seq, Op: o, Phase: p})
		})
		htemp := &Harness{My: my, Ref: ref, Seq: entry.Seq}
		op := opFrom(entry.Op, entry.Phase)
		committed, err := op.Apply(ctx, htemp, logger)
		if err != nil {
			return 0, err
		}
		if entry.Op.Kind == OpSnap {
			switch entry.Phase {
			case PhasePrepared:
				if err := my.ReleaseSnapshot(ctx, entry.Seq); err != nil {
					return 0, err
				}
				if ref != nil {
					if err := ref.ReleaseSnapshot(ctx, entry.Seq); err != nil {
						return 0, err
					}
				}
			case PhaseMyDone:
				if ref != nil {
					if err := ref.ReleaseSnapshot(ctx, entry.Seq); err != nil {
						return 0, err
					}
				}
			}
		}
		if committed {
			return entry.Seq + 1, nil
		}
		return entry.Seq, nil
	}
	var finalSeq uint64
	for _, e := range entries {
		if e.Phase == PhaseCommitted {
			if e.Op.Kind == OpPut || e.Op.Kind == OpDel {
				finalSeq = e.Seq + 1
			} else {
				finalSeq = e.Seq
			}
		}
	}
	if last.Phase != PhaseCommitted {
		fs, err := finalize(last)
		if err != nil {
			return 0, err
		}
		finalSeq = fs
	}
	return finalSeq, nil
}
