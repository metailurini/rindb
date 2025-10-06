package rindb

// anchorState tracks the pending anchor record to replay after a direction
// switch. It stages the last emitted record when the traversal direction
// changes so oscillating iteration can surface the boundary element again.
type anchorState struct {
	pending    [2]Record
	pendingSet [2]bool

	currentDir Direction
	dirSet     bool
	recent     Record
}

// Reset clears all state from the anchor manager.
func (a *anchorState) Reset() {
	if a == nil {
		return
	}
	*a = anchorState{}
}

func dirIndex(dir Direction) int {
	if dir == DirReverse {
		return 1
	}
	return 0
}

// OnDirectionChange records the new direction and stages the last emitted
// record for replay when the traversal direction changes. It returns true when
// a direction change occurred.
func (a *anchorState) OnDirectionChange(next Direction) bool {
	if !a.dirSet {
		a.currentDir = next
		a.dirSet = true
		return false
	}

	if a.currentDir == next {
		return false
	}

	a.currentDir = next
	if a.recent != nil {
		a.stagePending(a.recent, next)
	}
	return true
}

// PopPending retrieves the staged anchor record for the provided direction, if
// any, clearing the pending state once consumed.
func (a *anchorState) PopPending(dir Direction) (Record, bool) {
	idx := dirIndex(dir)
	if !a.pendingSet[idx] {
		return nil, false
	}
	rec := a.pending[idx]
	a.pending[idx] = nil
	a.pendingSet[idx] = false
	return rec, true
}

// HasPending reports whether a staged record exists for the supplied
// direction.
func (a *anchorState) HasPending(dir Direction) bool {
	return a.pendingSet[dirIndex(dir)]
}

func (a *anchorState) stagePending(rec Record, dir Direction) {
	idx := dirIndex(dir)
	a.pending[idx] = cloneRecord(rec)
	a.pendingSet[idx] = true
}

// ClearPending removes any staged record for the provided direction.
func (a *anchorState) ClearPending(dir Direction) {
	idx := dirIndex(dir)
	a.pending[idx] = nil
	a.pendingSet[idx] = false
}

// MarkLastEmitted stores the most recently surfaced record so it can be replayed
// after a direction change.
func (a *anchorState) MarkLastEmitted(rec Record, dir Direction) {
	if rec == nil {
		a.recent = nil
	} else {
		a.recent = cloneRecord(rec)
	}
	a.currentDir = dir
	a.dirSet = true
}

// LastDirection returns the most recent traversal direction alongside a flag
// indicating whether a direction has been recorded.
func (a *anchorState) LastDirection() (Direction, bool) {
	if !a.dirSet {
		return DirForward, false
	}
	return a.currentDir, true
}
