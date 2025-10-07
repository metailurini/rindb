package rindb

import "testing"

func TestAnchorState_OnDirectionChangeStagesClone(t *testing.T) {
	t.Parallel()
	state := &anchorState{}
	rec := RecordImpl{Key: Bytes("key"), Value: Bytes("value"), SequenceNumber: 1}

	if changed := state.onDirectionChange(DirForward); changed {
		t.Fatalf("expected initial direction to report no change")
	}

	state.markLastEmitted(rec, DirForward)

	if changed := state.onDirectionChange(DirReverse); !changed {
		t.Fatalf("expected direction switch to report change")
	}

	staged, ok := state.popPending(DirReverse)
	if !ok {
		t.Fatalf("expected pending record after direction change")
	}
	if staged.GetSequenceNumber() != rec.GetSequenceNumber() || staged.GetType() != rec.GetType() {
		t.Fatalf("cloned record does not match original metadata")
	}
	if string(staged.GetKey()) != string(rec.GetKey()) || string(staged.GetValue()) != string(rec.GetValue()) {
		t.Fatalf("cloned record does not match original key/value")
	}

	// Mutate the original record to confirm the staged copy is independent.
	rec.Key[0] = 'x'
	rec.Value[0] = 'y'
	if staged.GetKey()[0] == rec.GetKey()[0] || staged.GetValue()[0] == rec.GetValue()[0] {
		t.Fatalf("expected staged record to be independent clone")
	}

	if _, ok := state.popPending(DirReverse); ok {
		t.Fatalf("expected pending record to be cleared after pop")
	}
}

func TestAnchorState_OnDirectionChangeIgnoresSameDirection(t *testing.T) {
	t.Parallel()
	state := &anchorState{}
	rec := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 2}

	state.markLastEmitted(rec, DirForward)
	state.onDirectionChange(DirForward)
	if changed := state.onDirectionChange(DirForward); changed {
		t.Fatalf("expected same direction to report no change")
	}
	if _, ok := state.popPending(DirForward); ok {
		t.Fatalf("did not expect pending record when direction unchanged")
	}
}

func TestAnchorState_MarkLastEmittedClearsWhenNil(t *testing.T) {
	t.Parallel()
	state := &anchorState{}
	rec := RecordImpl{Key: Bytes("key"), SequenceNumber: 1}
	state.markLastEmitted(rec, DirForward)
	state.markLastEmitted(nil, DirForward)
	if state.recent != nil {
		t.Fatalf("expected nil record to clear recent state")
	}
}

func TestAnchorState_LastDirectionUnset(t *testing.T) {
	t.Parallel()
	state := &anchorState{}
	dir, ok := state.lastDirection()
	if ok {
		t.Fatalf("expected unset direction to report false")
	}
	if dir != DirForward {
		t.Fatalf("expected zero value direction to be forward")
	}
}
