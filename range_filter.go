package rindb

import "bytes"

// recordFilter coordinates record visibility and deduplication for range
// iteration. It tracks the last emitted key so duplicate physical versions of a
// user key are suppressed and applies snapshot and tombstone filtering.
type recordFilter struct {
	lastKey    Bytes
	lastKeySet bool

	lastDir Direction
	dirSet  bool

	snapshot    uint64
	hasSnapshot bool

	tombstoned map[uint64][]Bytes
}

// newRecordFilter constructs a recordFilter bound to the provided snapshot. A
// nil snapshot permits all sequence numbers.
func newRecordFilter(snapshot *uint64) *recordFilter {
	f := &recordFilter{tombstoned: make(map[uint64][]Bytes)}
	if snapshot != nil {
		f.snapshot = *snapshot
		f.hasSnapshot = true
	}
	return f
}

// Accept evaluates the supplied record for the given iteration direction. It
// returns the record when it should be surfaced to callers and false when it
// must be suppressed (duplicate key, tombstone, or hidden by the snapshot).
func (f *recordFilter) Accept(rec Record, dir Direction) (Record, bool) {
	if rec == nil {
		return nil, false
	}

	if !f.dirSet || dir != f.lastDir {
		f.resetKey()
		f.lastDir = dir
		f.dirSet = true
	}

	key := rec.GetKey()
	if f.isTombstoned(key) {
		return nil, false
	}

	if f.hasSnapshot && rec.GetSequenceNumber() > f.snapshot {
		return nil, false
	}

	if rec.GetType() == TypeDeletion {
		f.markTombstoned(key)
		f.remember(key)
		return nil, false
	}

	if f.lastKeySet {
		cmp := key.Compare(f.lastKey)
		switch dir {
		case DirForward:
			if cmp == CmpLess {
				return nil, false
			}
		case DirReverse:
			if cmp == CmpGreater {
				return nil, false
			}
		}
	}

	if f.lastKeySet && key.Compare(f.lastKey) == CmpEqual {
		return nil, false
	}

	f.remember(key)
	return rec, true
}

// MarkEmitted updates the deduplication state for a record that bypassed
// Accept, such as when replaying an anchor during a direction switch.
func (f *recordFilter) MarkEmitted(rec Record, dir Direction) {
	if rec == nil {
		return
	}
	f.remember(rec.GetKey())
	f.lastDir = dir
	f.dirSet = true
}

func (f *recordFilter) remember(key Bytes) {
	f.lastKey = append(f.lastKey[:0], key...)
	f.lastKeySet = true
}

func (f *recordFilter) resetKey() {
	f.lastKey = f.lastKey[:0]
	f.lastKeySet = false
}

func (f *recordFilter) markTombstoned(key Bytes) {
	hash := hashBytes(key)
	cloned := append(Bytes(nil), key...)
	f.tombstoned[hash] = append(f.tombstoned[hash], cloned)
}

func (f *recordFilter) isTombstoned(key Bytes) bool {
	hash := hashBytes(key)
	candidates := f.tombstoned[hash]
	for _, existing := range candidates {
		if bytes.Equal(existing, key) {
			return true
		}
	}
	return false
}

func hashBytes(b Bytes) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for _, by := range b {
		hash ^= uint64(by)
		hash *= prime64
	}
	return hash
}
