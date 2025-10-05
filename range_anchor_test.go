package rindb

import "testing"

func TestAnchorState_OnDirectionChangeStagesClone(t *testing.T) {
	state := &anchorState{}
	rec := RecordImpl{Key: Bytes("key"), Value: Bytes("value"), SequenceNumber: 1}

	if changed := state.OnDirectionChange(DirForward, nil); changed {
		t.Fatalf("expected initial direction to report no change")
	}

	if changed := state.OnDirectionChange(DirReverse, rec); !changed {
		t.Fatalf("expected direction switch to report change")
	}

	staged, ok := state.popPending()
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

	if _, ok := state.popPending(); ok {
		t.Fatalf("expected pending record to be cleared after pop")
	}
}

func TestAnchorState_OnDirectionChangeIgnoresSameDirection(t *testing.T) {
	state := &anchorState{}
	rec := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 2}

	state.OnDirectionChange(DirForward, rec)
	if changed := state.OnDirectionChange(DirForward, rec); changed {
		t.Fatalf("expected same direction to report no change")
	}
	if _, ok := state.popPending(); ok {
		t.Fatalf("did not expect pending record when direction unchanged")
	}
}
