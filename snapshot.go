package rindb

import (
	"context"
	"math"
	"sort"
	"sync"
)

// Snapshot represents a point-in-time view of the database.
//
// It captures the sequence number at the time of creation and a reference to
// the parent database, allowing callers to perform read operations (e.g., Get,
// IRange) against a consistent view of the data as it existed when the
// snapshot was taken.
type Snapshot struct {
	db       *Rindb
	sequence uint64
	mu       sync.Mutex
	released bool
}

// Sequence returns the captured sequence number for this snapshot.
func (s *Snapshot) Sequence() uint64 {
	return s.sequence
}

// Get returns the value associated with the key as of the snapshot's
// sequence.
func (s *Snapshot) Get(ctx context.Context, key Bytes) (Bytes, error) {
	ctx, span := tracer.Start(ctx, "Snapshot.Get")
	defer span.End()
	return s.db.Get(ctx, key, s.sequence)
}

// IRange returns an iterator over records with keys in [start, end] as of the
// snapshot's sequence.
func (s *Snapshot) IRange(ctx context.Context, start, end Bytes, opts ...RangeOption) (*RangeIterator, error) {
	ctx, span := tracer.Start(ctx, "Snapshot.IRange")
	defer span.End()
	opts = append([]RangeOption{IRangeSnapshot(s.sequence)}, opts...)
	return s.db.IRange(ctx, start, end, opts...)
}

// Release removes the snapshot from the list of active snapshots.
func (s *Snapshot) Release(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "Snapshot.Release")
	defer span.End()
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.released {
		return nil
	}
	if err := s.db.release(ctx, s); err != nil {
		return err
	}
	s.released = true
	return nil
}

// minSnapshotSeq returns the minimum sequence number among active snapshots.
//
// r.mu must be held before calling this method.
func (r *Rindb) minSnapshotSeq() uint64 {
	if len(r.activeSnapshots) == 0 {
		return r.sequenceNumber
	}
	// activeSnapshots is maintained in ascending order, so the first element
	// is always the smallest sequence number.
	return r.activeSnapshots[0]
}

// cleanupObsoleteLocked removes memtable entries and WAL segments older than
// the minimum active snapshot sequence up to maxSeq. r.mu must be held when
// calling.
func (r *Rindb) cleanupObsoleteLocked(ctx context.Context, maxSeq uint64) error {
	ctx, span := tracer.Start(ctx, "Rindb.cleanupObsoleteLocked")
	defer span.End()

	minSnapSeq := r.minSnapshotSeq()
	cutoff := min(maxSeq, minSnapSeq)

	r.Memtable.Cleanup(cutoff)

	if len(r.activeSnapshots) == 0 {
		r.SSTableManager.setMinSnapshotSeq(math.MaxUint64)
	} else {
		r.SSTableManager.setMinSnapshotSeq(cutoff)
	}

	if len(r.activeSnapshots) == 0 && r.Memtable.ByteSize() > 0 {
		// Memtable has unflushed data that is only in the WAL.
		// To prevent data loss on crash, we must not clean the WAL yet.
		return nil
	}

	return r.WAL.Clean(ctx, cutoff)
}

// NewSnapshot captures the current sequence number and tracks it in the list
// of active snapshots.
func (r *Rindb) NewSnapshot(ctx context.Context) (*Snapshot, error) {
	_, span := tracer.Start(ctx, "Rindb.NewSnapshot")
	defer span.End()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, ErrDatabaseClosed
	}

	snap := &Snapshot{db: r, sequence: r.sequenceNumber}
	prev := len(r.activeSnapshots)
	r.activeSnapshots = append(r.activeSnapshots, snap.sequence)

	if prev == 0 {
		r.SSTableManager.setMinSnapshotSeq(snap.sequence)
	}

	return snap, nil
}

// release removes the snapshot from the list of active snapshots.
// It is intended for internal use by Snapshot.Release.
func (r *Rindb) release(ctx context.Context, snap *Snapshot) error {
	ctx, span := tracer.Start(ctx, "Rindb.release")
	defer span.End()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrDatabaseClosed
	}

	idx := sort.Search(len(r.activeSnapshots), func(i int) bool {
		return r.activeSnapshots[i] >= snap.sequence
	})
	if idx < len(r.activeSnapshots) && r.activeSnapshots[idx] == snap.sequence {
		r.activeSnapshots = append(r.activeSnapshots[:idx], r.activeSnapshots[idx+1:]...)
	}
	if len(r.activeSnapshots) == 0 {
		r.SSTableManager.setMinSnapshotSeq(math.MaxUint64)
	}
	return r.cleanupObsoleteLocked(ctx, r.sequenceNumber)
}
