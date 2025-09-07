package fuzzing

import (
	"bytes"
	"context"

	rindb "github.com/metailurini/rindb"
)

// RindbEngine adapts a RinDB instance to the fuzzing Engine interface.
type RindbEngine struct{ db *rindb.Rindb }

// NewRindbEngine wraps the provided RinDB instance.
func NewRindbEngine(db *rindb.Rindb) *RindbEngine { return &RindbEngine{db: db} }

func (e *RindbEngine) Put(ctx context.Context, k, v []byte) error {
	return e.db.Put(ctx, rindb.Bytes(k), rindb.Bytes(v))
}

func (e *RindbEngine) Delete(ctx context.Context, k []byte) error {
	return e.db.Remove(ctx, rindb.Bytes(k))
}

func (e *RindbEngine) Get(ctx context.Context, k []byte, snapshot uint64) ([]byte, bool, error) {
	v, err := e.db.Get(ctx, rindb.Bytes(k), snapshot)
	if err != nil {
		if err == rindb.ErrKeyNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	return append([]byte(nil), []byte(v)...), true, nil
}

func (e *RindbEngine) Range(ctx context.Context, lo, hi []byte, snapshot uint64, limit int) ([]KV, error) {
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

func (e *RindbEngine) Close() error { return e.db.Close() }
