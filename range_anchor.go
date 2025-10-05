package rindb

// anchorState tracks the pending anchor record to replay after a direction
// switch. It stages the last emitted record when the traversal direction
// changes so oscillating iteration can surface the boundary element again.
type anchorState struct {
	pending Record
	lastDir Direction
	dirSet  bool
}

// OnDirectionChange records the new direction and stages the last emitted
// record for replay when the traversal direction changes. It returns true when
// a direction change occurred.
func (a *anchorState) OnDirectionChange(next Direction, lastEmitted Record) bool {
	if !a.dirSet {
		a.lastDir = next
		a.dirSet = true
		return false
	}
	if a.lastDir == next {
		return false
	}
	a.lastDir = next
	a.pending = cloneRecord(lastEmitted)
	return true
}

// popPending retrieves the staged anchor record, if any, clearing the pending
// state once consumed.
func (a *anchorState) popPending() (Record, bool) {
	if a.pending == nil {
		return nil, false
	}
	rec := a.pending
	a.pending = nil
	return rec, true
}
