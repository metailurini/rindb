package rindb

import "context"

// Snapshot represents a point-in-time view of the database.
//
// It captures the sequence number at the time of creation and a reference to
// the parent database, allowing callers to perform read operations (e.g., Get,
// IRange) against a consistent view of the data as it existed when the
// snapshot was taken.
type Snapshot struct {
	db       *Rindb
	sequence uint64
}

// Sequence returns the captured sequence number for this snapshot.
func (s Snapshot) Sequence() uint64 {
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
func (s *Snapshot) IRange(ctx context.Context, start, end Bytes) (*RangeIterator, error) {
	ctx, span := tracer.Start(ctx, "Snapshot.IRange")
	defer span.End()
	return s.db.IRange(ctx, start, end, s.sequence)
}

// minSnapshotSeq returns the minimum sequence number among active snapshots.
//
// r.mu must be held before calling this method.
func (r *Rindb) minSnapshotSeq() uint64 {
	if len(r.activeSnapshots) == 0 {
		return r.sequenceNumber
	}
	minSeq := r.activeSnapshots[0]
	for _, s := range r.activeSnapshots[1:] {
		if s < minSeq {
			minSeq = s
		}
	}
	return minSeq
}

// cleanupObsoleteLocked removes memtable entries and WAL segments older than
// the minimum active snapshot sequence up to maxSeq. r.mu must be held when
// calling.
func (r *Rindb) cleanupObsoleteLocked(ctx context.Context, maxSeq uint64) error {
	ctx, span := tracer.Start(ctx, "Rindb.cleanupObsoleteLocked")
	defer span.End()

	cutoff := r.minSnapshotSeq()
	if maxSeq < cutoff {
		cutoff = maxSeq
	}

	r.memtable.Cleanup(cutoff)

	r.ssTableManager.mu.Lock()
	r.ssTableManager.minSnapshotSeq = cutoff
	r.ssTableManager.mu.Unlock()

	if len(r.activeSnapshots) == 0 && r.memtable.ByteSize() > 0 {
		// Memtable has unflushed data that is only in the WAL.
		// To prevent data loss on crash, we must not clean the WAL yet.
		return nil
	}

	// When no snapshots, r.minSnapshotSeq() is r.sequenceNumber.
	// When snapshots exist, it is the minimum sequence.
	// This correctly cleans the WAL in both cases.
	return r.wal.Clean(ctx, r.minSnapshotSeq())
}

// NewSnapshot captures the current sequence number and tracks it in the list
// of active snapshots.
func (r *Rindb) NewSnapshot(ctx context.Context) (*Snapshot, error) {
	ctx, span := tracer.Start(ctx, "Rindb.NewSnapshot")
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
		r.ssTableManager.mu.Lock()
		r.ssTableManager.minSnapshotSeq = snap.sequence
		r.ssTableManager.mu.Unlock()
	}

	return snap, nil
}

// Release removes the snapshot from the list of active snapshots.
func (r *Rindb) Release(ctx context.Context, snap *Snapshot) error {
	ctx, span := tracer.Start(ctx, "Rindb.Release")
	defer span.End()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrDatabaseClosed
	}

	for i, seq := range r.activeSnapshots {
		if seq == snap.sequence {
			r.activeSnapshots = append(r.activeSnapshots[:i], r.activeSnapshots[i+1:]...)
			break
		}
	}
	return r.cleanupObsoleteLocked(ctx, r.sequenceNumber)
}
