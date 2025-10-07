package rindb

import "testing"

func TestPrefetchState_PeekAndPopEmpty(t *testing.T) {
	var p prefetchState
	if _, ok := p.peek(DirForward); ok {
		t.Fatalf("expected empty prefetch to report not ready")
	}
	if _, ok := p.pop(DirReverse); ok {
		t.Fatalf("expected empty prefetch pop to report not ready")
	}
}

func TestPrefetchState_StageNilClears(t *testing.T) {
	var p prefetchState
	p.stage(DirForward, rec("k", "v", 1, TypeValue))
	if _, ok := p.peek(DirForward); !ok {
		t.Fatalf("expected staged record to be available")
	}

	p.stage(DirForward, nil)
	if p.has(DirForward) {
		t.Fatalf("expected staging nil to clear readiness")
	}
	if _, ok := p.peek(DirForward); ok {
		t.Fatalf("expected cleared slot to report empty")
	}
}
