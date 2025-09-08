package diffharness

import (
	"bytes"
	"context"

	rindb "github.com/metailurini/rindb"
)

// RinDBEngine adapts RinDB to the diffharness Engine interface.
type RinDBEngine struct {
	db    *rindb.Rindb
	snaps map[uint64]*rindb.Snapshot
}

// NewRinDBEngine wraps a RinDB instance as an Engine.
func NewRinDBEngine(db *rindb.Rindb) *RinDBEngine {
	return &RinDBEngine{db: db, snaps: make(map[uint64]*rindb.Snapshot)}
}

func (e *RinDBEngine) Put(ctx context.Context, k, v []byte) error {
	return e.db.Put(ctx, rindb.Bytes(k), rindb.Bytes(v))
}

func (e *RinDBEngine) Delete(ctx context.Context, k []byte) error {
	return e.db.Remove(ctx, rindb.Bytes(k))
}

func (e *RinDBEngine) Get(ctx context.Context, k []byte, snapshot uint64) ([]byte, bool, error) {
	v, err := e.db.Get(ctx, rindb.Bytes(k), snapshot)
	if err != nil {
		if err == rindb.ErrKeyNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	return append([]byte(nil), []byte(v)...), true, nil
}

func (e *RinDBEngine) Range(ctx context.Context, lo, hi []byte, snapshot uint64, limit int) ([]KV, error) {
	it, err := e.db.IRange(ctx, rindb.Bytes(lo), rindb.Bytes(hi), snapshot)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var res []KV
	for it.HasNext() && len(res) < limit {
		rec, err := it.Next()
		if err != nil {
			return nil, err
		}
		if bytes.Compare([]byte(rec.GetKey()), hi) >= 0 {
			break
		}
		res = append(res, KV{K: append([]byte(nil), []byte(rec.GetKey())...), V: append([]byte(nil), []byte(rec.GetValue())...)})
	}
	return res, nil
}

func (e *RinDBEngine) NewSnapshot(ctx context.Context) (uint64, error) {
	snap, err := e.db.NewSnapshot(ctx)
	if err != nil {
		return 0, err
	}
	seq := snap.Sequence()
	if e.snaps == nil {
		e.snaps = make(map[uint64]*rindb.Snapshot)
	}
	e.snaps[seq] = snap
	return seq, nil
}

func (e *RinDBEngine) ReleaseSnapshot(ctx context.Context, seq uint64) error {
	snap, ok := e.snaps[seq]
	if !ok {
		return nil
	}
	delete(e.snaps, seq)
	return snap.Release(ctx)
}

func (e *RinDBEngine) Close() error { return e.db.Close() }
