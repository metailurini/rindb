package diffharness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
)

// NewHarness creates a harness with the given engines and log file path.
func NewHarness(my Engine, ref *SQLiteOracle, seed int64, logPath string) (*Harness, error) {
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &Harness{My: my, Ref: ref, Seed: seed, log: f, enc: json.NewEncoder(f)}, nil
}

// Close closes underlying resources.
func (h *Harness) Close() error {
	_ = h.releaseSnapshots(context.Background(), true)
	return h.log.Close()
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randPrefixedBytes(r *rand.Rand, n int, prefix, suffix string) []byte {
	minLen := len(prefix) + len(suffix)
	if n < minLen {
		n = minLen
	}
	b := make([]byte, n)
	copy(b, prefix)
	for i := len(prefix); i < n-len(suffix); i++ {
		b[i] = letters[r.Intn(len(letters))]
	}
	copy(b[n-len(suffix):], suffix)
	return b
}

func randKey(r *rand.Rand, n int) []byte {
	return randPrefixedBytes(r, n, "sk", "ek")
}

func randValue(r *rand.Rand, n int) []byte {
	return randPrefixedBytes(r, n, "sv", "ev")
}

const maxKnownKeys = 100

func (h *Harness) addKey(k []byte) {
	if h.keySet == nil {
		h.keySet = make(map[string]struct{})
	}
	s := string(k)
	if _, ok := h.keySet[s]; ok {
		return
	}
	if len(h.keys) >= maxKnownKeys {
		oldest := h.keys[0]
		h.keys = h.keys[1:]
		delete(h.keySet, oldest)
	}
	h.keys = append(h.keys, s)
	h.keySet[s] = struct{}{}
}

func (h *Harness) delKey(k []byte) {
	if h.keySet == nil {
		return
	}
	s := string(k)
	if _, ok := h.keySet[s]; !ok {
		return
	}
	delete(h.keySet, s)
	for i, v := range h.keys {
		if v == s {
			h.keys = append(h.keys[:i], h.keys[i+1:]...)
			break
		}
	}
}

func (h *Harness) pickKnownKey(r *rand.Rand) []byte {
	if len(h.keys) == 0 {
		return nil
	}
	return []byte(h.keys[r.Intn(len(h.keys))])
}

func (h *Harness) genKey(r *rand.Rand, n int) []byte {
	k := randKey(r, n)
	if ex := h.pickKnownKey(r); ex != nil && r.Intn(2) == 0 {
		k = ex
	}
	return k
}

func (h *Harness) genOp(r *rand.Rand, cfg Cfg) Op {
	sum := 0
	for _, w := range cfg.Weights {
		sum += w
	}
	x := r.Intn(sum)
	var kind OpKind
	for op, w := range cfg.Weights {
		if x < w {
			kind = op
			break
		}
		x -= w
	}
	switch kind {
	case OpPut:
		vlen := cfg.ValLenMin + r.Intn(cfg.ValLenMax-cfg.ValLenMin+1)
		key := h.genKey(r, cfg.KeyLen)
		return Op{Kind: OpPut, K: key, V: randValue(r, vlen)}
	case OpDel:
		key := h.genKey(r, cfg.KeyLen)
		return Op{Kind: OpDel, K: key}
	case OpGet:
		key := h.genKey(r, cfg.KeyLen)
		s := h.pickSnapshot(r)
		return Op{Kind: OpGet, K: key, SnapSeq: s}
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
func (h *Harness) Step(ctx context.Context, op Operation) error {
	i := h.ops
	logger := phaseLoggerFunc(func(o Op, seq uint64, p Phase) error {
		if err := h.enc.Encode(struct {
			I     int    `json:"i"`
			Seq   uint64 `json:"seq"`
			Op    Op     `json:"op"`
			Phase Phase  `json:"phase"`
		}{i, seq, o, p}); err != nil {
			return err
		}
		return h.log.Sync()
	})
	committed, err := op.Apply(ctx, h, logger)
	if err != nil {
		return err
	}
	if committed {
		h.Seq++
	}
	h.ops++
	return nil
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
	for i := 0; n < 0 || i < n; i++ {
		op := h.genOp(r, cfg)
		if err := h.Step(ctx, opFrom(op, "")); err != nil {
			return err
		}
		if err := h.checkInvariants(ctx, r, cfg); err != nil {
			return h.fail(h.ops, Op{Kind: OpInvariantCheck}, err)
		}
		if cfg.TelemetryEvery > 0 && h.telemetry != nil && h.ops%cfg.TelemetryEvery == 0 {
			h.telemetry(h.Seq, h.ops)
		}
		if cfg.CrashEvery > 0 && h.crash != nil && h.ops%cfg.CrashEvery == 0 {
			if err := h.crash(); err != nil {
				return err
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
	_ = h.log.Sync()
	return fmt.Errorf("fuzz fail at i=%d seq=%d kind=%d: %w", i, h.Seq, op.Kind, cause)
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

		if len(left) < cfg.RangeMax && len(right) < cfg.RangeMax {
			limit := len(left) + len(right) + 1
			full, err := h.My.Range(ctx, lo, hi, snap, limit)
			if err != nil {
				return err
			}
			concat := append(append([]KV{}, left...), right...)
			if err := compareKVLists(concat, full); err != nil {
				return fmt.Errorf("range concat: %w", err)
			}
			// Compare full range with reference only when not truncated
			f2, err := h.Ref.RangeWithSeq(lo, hi, snap, limit)
			if err != nil {
				return err
			}
			if err := compareKVLists(full, f2); err != nil {
				return fmt.Errorf("range full mismatch: %w", err)
			}
		}

		// Compare sub-ranges with reference for coverage
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
	}
	return nil
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
		logger := phaseLoggerFunc(func(o Op, seq uint64, p Phase) error {
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
