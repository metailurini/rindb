package rindb

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

	tombstoned map[string]struct{}
}

// newRecordFilter constructs a recordFilter bound to the provided snapshot. A
// nil snapshot permits all sequence numbers.
func newRecordFilter(snapshot *uint64) *recordFilter {
	f := &recordFilter{tombstoned: make(map[string]struct{})}
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

	keyStr := string(rec.GetKey())
	if _, tombstoned := f.tombstoned[keyStr]; tombstoned {
		return nil, false
	}

	if f.hasSnapshot && rec.GetSequenceNumber() > f.snapshot {
		return nil, false
	}

	if rec.GetType() == TypeDeletion {
		f.tombstoned[keyStr] = struct{}{}
		f.remember(rec.GetKey())
		return nil, false
	}

	if f.lastKeySet {
		cmp := rec.GetKey().Compare(f.lastKey)
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

	if f.lastKeySet && rec.GetKey().Compare(f.lastKey) == CmpEqual {
		return nil, false
	}

	f.remember(rec.GetKey())
	return rec, true
}

// Reset clears the stored key so the next Accept call treats the provided
// record as unseen regardless of the user key.
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
