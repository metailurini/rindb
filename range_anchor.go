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

// onDirectionChange records the new direction and stages the last emitted
// record for replay when the traversal direction changes. It returns true when
// a direction change occurred.
func (a *anchorState) onDirectionChange(next Direction) bool {
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

// popPending retrieves the staged anchor record for the provided direction, if
// any, clearing the pending state once consumed.
func (a *anchorState) popPending(dir Direction) (Record, bool) {
	idx := dirIndex(dir)
	if !a.pendingSet[idx] {
		return nil, false
	}
	rec := a.pending[idx]
	a.pending[idx] = nil
	a.pendingSet[idx] = false
	return rec, true
}

// hasPending reports whether a staged record exists for the supplied
// direction.
func (a *anchorState) hasPending(dir Direction) bool {
	return a.pendingSet[dirIndex(dir)]
}

// stagePending saves a record to be replayed on the next iteration in the given
// direction. This is used to resurface the anchor record when oscillation
// occurs (e.g., Next() -> Prev()).
func (a *anchorState) stagePending(rec Record, dir Direction) {
	idx := dirIndex(dir)
	a.pending[idx] = cloneRecord(rec)
	a.pendingSet[idx] = true
}

// clearPending removes any staged record for the provided direction.
func (a *anchorState) clearPending(dir Direction) {
	idx := dirIndex(dir)
	a.pending[idx] = nil
	a.pendingSet[idx] = false
}

// markLastEmitted stores the most recently surfaced record so it can be replayed
// after a direction change.
func (a *anchorState) markLastEmitted(rec Record, dir Direction) {
	if rec == nil {
		a.recent = nil
	} else {
		a.recent = cloneRecord(rec)
	}
	a.currentDir = dir
	a.dirSet = true
}

// lastDirection returns the most recent traversal direction alongside a flag
// indicating whether a direction has been recorded.
func (a *anchorState) lastDirection() (Direction, bool) {
	if !a.dirSet {
		return DirForward, false
	}
	return a.currentDir, true
}
