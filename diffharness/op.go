package diffharness

import (
	"bytes"
	"context"
	"fmt"
)

// PhaseLogger records the phase transitions of an operation.
type PhaseLogger interface {
	Log(op Op, seq uint64, phase Phase) error
}

type phaseLoggerFunc func(op Op, seq uint64, phase Phase) error

func (f phaseLoggerFunc) Log(op Op, seq uint64, phase Phase) error { return f(op, seq, phase) }

// Operation defines a harness action.
type Operation interface {
	Apply(ctx context.Context, h *Harness, log PhaseLogger) (committed bool, err error)
}

// MismatchError reports a divergence between engines.
type MismatchError struct {
	Op    Op
	MyV   []byte
	MyOK  bool
	RefV  []byte
	RefOK bool
}

func (m *MismatchError) Error() string {
	return fmt.Sprintf("mismatch: my=(ok=%v, val=%q) ref=(ok=%v, val=%q)", m.MyOK, m.MyV, m.RefOK, m.RefV)
}

func wrapErr(op Op, phase Phase, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("op %v phase %s: %w", op.Kind, phase, err)
}

// PutOp inserts or replaces a key/value pair.
type PutOp struct {
	K, V  []byte
	start Phase
}

func (p PutOp) Apply(ctx context.Context, h *Harness, log PhaseLogger) (bool, error) {
	op := Op{Kind: OpPut, K: p.K, V: p.V}
	if p.start == "" {
		if err := log.Log(op, h.Seq, PhasePrepared); err != nil {
			return false, wrapErr(op, PhasePrepared, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		p.start = PhasePrepared
	}
	switch p.start {
	case PhasePrepared:
		if err := h.My.Begin(ctx); err != nil {
			return false, wrapErr(op, PhasePrepared, err)
		}
		if err := h.My.Put(ctx, p.K, p.V); err != nil {
			_ = h.My.Rollback(ctx)
			return false, wrapErr(op, PhasePrepared, err)
		}
		if err := h.My.Commit(ctx); err != nil {
			return false, wrapErr(op, PhasePrepared, err)
		}
		if err := log.Log(op, h.Seq, PhaseMyDone); err != nil {
			return false, wrapErr(op, PhaseMyDone, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		fallthrough
	case PhaseMyDone:
		if h.Ref != nil {
			if err := h.Ref.Begin(ctx); err != nil {
				return false, wrapErr(op, PhaseMyDone, err)
			}
			if err := h.Ref.PutWithSeq(p.K, p.V, h.Seq+1); err != nil {
				_ = h.Ref.Rollback(ctx)
				return false, wrapErr(op, PhaseMyDone, err)
			}
			if err := h.Ref.Commit(ctx); err != nil {
				return false, wrapErr(op, PhaseMyDone, err)
			}
		}
		if err := log.Log(op, h.Seq, PhaseRefDone); err != nil {
			return false, wrapErr(op, PhaseRefDone, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		fallthrough
	case PhaseRefDone:
		if err := log.Log(op, h.Seq, PhaseCommitted); err != nil {
			return false, wrapErr(op, PhaseCommitted, err)
		}
		h.addKey(p.K)
		return true, nil
	default:
		return true, nil
	}
}

// DelOp deletes a key.
type DelOp struct {
	K     []byte
	start Phase
}

func (d DelOp) Apply(ctx context.Context, h *Harness, log PhaseLogger) (bool, error) {
	op := Op{Kind: OpDel, K: d.K}
	if d.start == "" {
		if err := log.Log(op, h.Seq, PhasePrepared); err != nil {
			return false, wrapErr(op, PhasePrepared, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		d.start = PhasePrepared
	}
	switch d.start {
	case PhasePrepared:
		if err := h.My.Begin(ctx); err != nil {
			return false, wrapErr(op, PhasePrepared, err)
		}
		if err := h.My.Delete(ctx, d.K); err != nil {
			_ = h.My.Rollback(ctx)
			return false, wrapErr(op, PhasePrepared, err)
		}
		if err := h.My.Commit(ctx); err != nil {
			return false, wrapErr(op, PhasePrepared, err)
		}
		if err := log.Log(op, h.Seq, PhaseMyDone); err != nil {
			return false, wrapErr(op, PhaseMyDone, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		fallthrough
	case PhaseMyDone:
		if h.Ref != nil {
			if err := h.Ref.Begin(ctx); err != nil {
				return false, wrapErr(op, PhaseMyDone, err)
			}
			if err := h.Ref.DelWithSeq(d.K, h.Seq+1); err != nil {
				_ = h.Ref.Rollback(ctx)
				return false, wrapErr(op, PhaseMyDone, err)
			}
			if err := h.Ref.Commit(ctx); err != nil {
				return false, wrapErr(op, PhaseMyDone, err)
			}
		}
		if err := log.Log(op, h.Seq, PhaseRefDone); err != nil {
			return false, wrapErr(op, PhaseRefDone, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		fallthrough
	case PhaseRefDone:
		if err := log.Log(op, h.Seq, PhaseCommitted); err != nil {
			return false, wrapErr(op, PhaseCommitted, err)
		}
		h.delKey(d.K)
		return true, nil
	default:
		return true, nil
	}
}

// GetOp retrieves a single key at a snapshot.
type GetOp struct {
	K       []byte
	SnapSeq uint64
	start   Phase
}

func (g GetOp) Apply(ctx context.Context, h *Harness, log PhaseLogger) (bool, error) {
	op := Op{Kind: OpGet, K: g.K, SnapSeq: g.SnapSeq}
	if g.start == "" {
		if err := log.Log(op, h.Seq, PhasePrepared); err != nil {
			return false, wrapErr(op, PhasePrepared, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		g.start = PhasePrepared
	}
	var mv []byte
	var mok bool
	switch g.start {
	case PhasePrepared:
		v, ok, err := h.My.Get(ctx, g.K, g.SnapSeq)
		if err != nil {
			return false, wrapErr(op, PhasePrepared, err)
		}
		mv, mok = v, ok
		if err := log.Log(op, h.Seq, PhaseMyDone); err != nil {
			return false, wrapErr(op, PhaseMyDone, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		fallthrough
	case PhaseMyDone:
		if h.Ref != nil {
			sv, sok, err := h.Ref.GetWithSeq(g.K, g.SnapSeq)
			if err != nil {
				return false, wrapErr(op, PhaseMyDone, err)
			}
			if mok != sok || !bytes.Equal(mv, sv) {
				return false, &MismatchError{Op: op, MyV: mv, MyOK: mok, RefV: sv, RefOK: sok}
			}
		}
		if err := log.Log(op, h.Seq, PhaseRefDone); err != nil {
			return false, wrapErr(op, PhaseRefDone, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		fallthrough
	case PhaseRefDone:
		if err := log.Log(op, h.Seq, PhaseCommitted); err != nil {
			return false, wrapErr(op, PhaseCommitted, err)
		}
	}
	return false, nil
}

// RangeOp scans keys in a range.
type RangeOp struct {
	Lo, Hi  []byte
	SnapSeq uint64
	Limit   int
	start   Phase
}

func (r RangeOp) Apply(ctx context.Context, h *Harness, log PhaseLogger) (bool, error) {
	op := Op{Kind: OpRange, Lo: r.Lo, Hi: r.Hi, SnapSeq: r.SnapSeq, Limit: r.Limit}
	if r.start == "" {
		if err := log.Log(op, h.Seq, PhasePrepared); err != nil {
			return false, wrapErr(op, PhasePrepared, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		r.start = PhasePrepared
	}
	var mres []KV
	switch r.start {
	case PhasePrepared:
		res, err := h.My.Range(ctx, r.Lo, r.Hi, r.SnapSeq, r.Limit)
		if err != nil {
			return false, wrapErr(op, PhasePrepared, err)
		}
		mres = res
		if err := log.Log(op, h.Seq, PhaseMyDone); err != nil {
			return false, wrapErr(op, PhaseMyDone, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		fallthrough
	case PhaseMyDone:
		if h.Ref != nil {
			sres, err := h.Ref.RangeWithSeq(r.Lo, r.Hi, r.SnapSeq, r.Limit)
			if err != nil {
				return false, wrapErr(op, PhaseMyDone, err)
			}
			if err := compareKVLists(mres, sres); err != nil {
				return false, wrapErr(op, PhaseMyDone, err)
			}
		}
		if err := log.Log(op, h.Seq, PhaseRefDone); err != nil {
			return false, wrapErr(op, PhaseRefDone, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		fallthrough
	case PhaseRefDone:
		if err := log.Log(op, h.Seq, PhaseCommitted); err != nil {
			return false, wrapErr(op, PhaseCommitted, err)
		}
	}
	return false, nil
}

// SnapOp creates snapshots in both engines.
type SnapOp struct{ start Phase }

func (s SnapOp) Apply(ctx context.Context, h *Harness, log PhaseLogger) (bool, error) {
	op := Op{Kind: OpSnap}
	if s.start == "" {
		if err := log.Log(op, h.Seq, PhasePrepared); err != nil {
			return false, wrapErr(op, PhasePrepared, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		s.start = PhasePrepared
	}
	switch s.start {
	case PhasePrepared:
		seq, err := h.My.NewSnapshot(ctx)
		if err != nil {
			return false, wrapErr(op, PhasePrepared, err)
		}
		if seq != h.Seq {
			return false, wrapErr(op, PhasePrepared, fmt.Errorf("snapshot sequence mismatch: my=%d have=%d", h.Seq, seq))
		}
		if err := log.Log(op, h.Seq, PhaseMyDone); err != nil {
			return false, wrapErr(op, PhaseMyDone, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		fallthrough
	case PhaseMyDone:
		if h.Ref != nil {
			refSeq, err := h.Ref.NewSnapshot(ctx)
			if err != nil {
				return false, wrapErr(op, PhaseMyDone, err)
			}
			if refSeq != h.Seq {
				return false, wrapErr(op, PhaseMyDone, fmt.Errorf("snapshot sequence mismatch: my=%d ref=%d", h.Seq, refSeq))
			}
		}
		if err := log.Log(op, h.Seq, PhaseRefDone); err != nil {
			return false, wrapErr(op, PhaseRefDone, err)
		}
		if h.crash != nil {
			if err := h.crash(); err != nil {
				return false, err
			}
		}
		fallthrough
	case PhaseRefDone:
		if err := log.Log(op, h.Seq, PhaseCommitted); err != nil {
			return false, wrapErr(op, PhaseCommitted, err)
		}
		h.Snapshots = append(h.Snapshots, h.Seq)
	}
	return false, nil
}

// opFrom constructs an Operation from an Op and a starting phase.
func opFrom(o Op, start Phase) Operation {
	switch o.Kind {
	case OpPut:
		return PutOp{K: o.K, V: o.V, start: start}
	case OpDel:
		return DelOp{K: o.K, start: start}
	case OpGet:
		return GetOp{K: o.K, SnapSeq: o.SnapSeq, start: start}
	case OpRange:
		return RangeOp{Lo: o.Lo, Hi: o.Hi, SnapSeq: o.SnapSeq, Limit: o.Limit, start: start}
	case OpSnap:
		return SnapOp{start: start}
	default:
		panic(fmt.Sprintf("unhandled op kind: %v", o.Kind))
	}
}
