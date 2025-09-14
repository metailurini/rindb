package diffharness

import (
	"context"

	rindb "github.com/metailurini/rindb"
)

// KV represents a key/value pair used by range operations.
type KV struct{ K, V []byte }

// Engine is the common interface implemented by RinDB and the oracle.
type Engine interface {
	Begin(ctx context.Context) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Put(ctx context.Context, k, v []byte) error // insert or replace key with value
	Delete(ctx context.Context, k []byte) error // delete key (tombstone)
	Get(ctx context.Context, k []byte, snapshot uint64) ([]byte, bool, error)
	Range(ctx context.Context, lo, hi []byte, snapshot uint64, limit int) ([]KV, error)
	NewSnapshot(ctx context.Context) (uint64, error)
	ReleaseSnapshot(ctx context.Context, seq uint64) error
	Close() error
}

// IteratorEngine exposes raw range iterators, bypassing the higher-level
// Engine abstraction. Engines implementing this interface allow invariants to
// drive iterators directly.
type IteratorEngine interface {
	IterRange(ctx context.Context, lo, hi []byte, snap uint64) (*rindb.RangeIterator, error)
}
