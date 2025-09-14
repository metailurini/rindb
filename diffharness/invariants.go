package diffharness

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
)

// Invariant checks harness state after each operation.
type Invariant func(ctx context.Context, h *Harness, r *rand.Rand, cfg Cfg) error

// WithInvariants replaces the invariant set.
func (h *Harness) WithInvariants(invs []Invariant) { h.invariants = invs }

// defaultInvariants lists invariants checked by NewHarness.
var defaultInvariants = []Invariant{
	checkMonotonicReads,
	checkRangeConcat,
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

func checkMonotonicReads(ctx context.Context, h *Harness, r *rand.Rand, cfg Cfg) error {
	if h.Ref == nil || len(h.Snapshots) == 0 {
		return nil
	}
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
		return fmt.Errorf("monotonic: s1 mismatch: key=%q s1=%d my=(ok=%v val=%q) ref=(ok=%v val=%q)", k, s1, mok1, mv1, sok1, sv1)
	}
	if mok2 != sok2 || !bytes.Equal(mv2, sv2) {
		return fmt.Errorf("monotonic: s2 mismatch: key=%q s2=%d my=(ok=%v val=%q) ref=(ok=%v val=%q)", k, s2, mok2, mv2, sok2, sv2)
	}
	return nil
}

func checkRangeConcat(ctx context.Context, h *Harness, r *rand.Rand, cfg Cfg) error {
	if h.Ref == nil || len(h.Snapshots) == 0 || cfg.RangeMax <= 0 {
		return nil
	}
	lo := randKey(r, cfg.KeyLen)
	hi := randKey(r, cfg.KeyLen)
	for bytes.Compare(hi, lo) <= 0 {
		hi = randKey(r, cfg.KeyLen)
	}
	mid := randKey(r, cfg.KeyLen)
	for bytes.Compare(mid, lo) <= 0 || bytes.Compare(mid, hi) >= 0 {
		mid = randKey(r, cfg.KeyLen)
	}
	snap := pickSnapshot(r, h.Seq, h.Snapshots, cfg.SnapshotReuseEvery)
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
			return fmt.Errorf("range concat mismatch: lo=%q mid=%q hi=%q snap=%d: %w", lo, mid, hi, snap, err)
		}
		f2, err := h.Ref.RangeWithSeq(lo, hi, snap, limit)
		if err != nil {
			return err
		}
		if err := compareKVLists(full, f2); err != nil {
			return fmt.Errorf("range full mismatch: lo=%q hi=%q snap=%d: %w", lo, hi, snap, err)
		}
	}
	l2, err := h.Ref.RangeWithSeq(lo, mid, snap, cfg.RangeMax)
	if err != nil {
		return err
	}
	if err := compareKVLists(left, l2); err != nil {
		return fmt.Errorf("range left mismatch: lo=%q mid=%q snap=%d: %w", lo, mid, snap, err)
	}
	r2, err := h.Ref.RangeWithSeq(mid, hi, snap, cfg.RangeMax)
	if err != nil {
		return err
	}
	if err := compareKVLists(right, r2); err != nil {
		return fmt.Errorf("range right mismatch: mid=%q hi=%q snap=%d: %w", mid, hi, snap, err)
	}
	return nil
}
