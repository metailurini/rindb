package rindb

// recordFilter coordinates record visibility and deduplication for range
// iteration. It tracks the last emitted key so duplicate physical versions of a
// user key are suppressed and applies snapshot and tombstone filtering.
type recordFilter struct {
	recentKey Bytes
	haveKey   bool

	currentDir Direction
	haveDir    bool

	snapshot    uint64
	hasSnapshot bool
}

// newRecordFilter constructs a recordFilter bound to the provided snapshot. A
// nil snapshot permits all sequence numbers.
func newRecordFilter(snapshot *uint64) *recordFilter {
	f := &recordFilter{}
	if snapshot != nil {
		f.snapshot = *snapshot
		f.hasSnapshot = true
	}
	return f
}

// accept evaluates the supplied record for the given iteration direction. It
// returns the record when it should be surfaced to callers and false when it
// must be suppressed (duplicate key, tombstone, or hidden by the snapshot).
func (f *recordFilter) accept(rec Record, dir Direction) (Record, bool) {
	if rec == nil {
		return nil, false
	}

	if !f.haveDir || dir != f.currentDir {
		f.resetKey()
		f.currentDir = dir
		f.haveDir = true
	}

	if f.hasSnapshot && rec.GetSequenceNumber() > f.snapshot {
		return nil, false
	}

	if rec.GetType() == TypeDeletion {
		return nil, false
	}

	if f.haveKey && rec.GetKey().Compare(f.recentKey) == CmpEqual {
		return nil, false
	}

	f.remember(rec.GetKey())
	return rec, true
}

// reset clears the stored key so the next Accept call treats the provided
// record as unseen regardless of the user key.
func (f *recordFilter) reset() {
	f.resetKey()
	f.haveDir = false
}

// markEmitted updates the deduplication state for a record that bypassed
// Accept, such as when replaying an anchor during a direction switch.
func (f *recordFilter) markEmitted(rec Record, dir Direction) {
	if rec == nil {
		return
	}
	f.remember(rec.GetKey())
	f.currentDir = dir
	f.haveDir = true
}

func (f *recordFilter) remember(key Bytes) {
	f.recentKey = append(f.recentKey[:0], key...)
	f.haveKey = true
}

func (f *recordFilter) resetKey() {
	f.recentKey = f.recentKey[:0]
	f.haveKey = false
}

func (f *recordFilter) clone() *recordFilter {
	copy := *f
	copy.recentKey = append(Bytes(nil), f.recentKey...)
	return &copy
}
