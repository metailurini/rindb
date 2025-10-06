package rindb

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRangeIterator_PrimesReverseError(t *testing.T) {
	boom := errors.New("boom")
	mi := &MergingIterator{
		fwd:           NewPriorityQueue(func(a, b pqItem) bool { return false }),
		rev:           NewPriorityQueue(func(a, b pqItem) bool { return false }),
		reversePrimed: true,
		reverseErr:    boom,
	}

	ri := NewRangeIterator(mi, RangeDesc)
	require.Equal(t, boom, ri.err)
}

func TestRangeIterator_PrimeNextEarlyReturn(t *testing.T) {
	mi := &MergingIterator{
		fwd: NewPriorityQueue(func(a, b pqItem) bool { return false }),
		rev: NewPriorityQueue(func(a, b pqItem) bool { return false }),
	}
	ri := &RangeIterator{
		mi:       mi,
		cursor:   newRangeCursor(mi),
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    RangeAsc,
	}

	staged := rec("k", "v", 1, TypeValue)
	ri.prefetch.Stage(DirForward, staged)
	ri.primeNext()

	peeked, ok := ri.prefetch.Peek(DirForward)
	require.True(t, ok)
	assert.Equal(t, staged, peeked)
}

func TestRangeIterator_PrimeNextHandlesReverseGap(t *testing.T) {
	iter := &errIterator{
		records: []Record{
			rec("a", "va", 1, TypeValue),
			rec("b", "vb", 1, TypeValue),
		},
		failIdx: 0,
	}
	mi, err := NewMergingIterator([]Iterator[Record]{iter}, nil, RangeDesc)
	require.NoError(t, err)
	ri := NewRangeIterator(mi, RangeDesc)
	ri.prefetch.ClearAll()

	ri.primeNext()

	_, ok := ri.prefetch.Peek(DirReverse)
	assert.False(t, ok)
	assert.NotNil(t, mi.err)
}

func TestRangeIterator_PrimeNextStagesDeletionForPrev(t *testing.T) {
	iter := &errIterator{
		records: []Record{
			rec("k", "", 2, TypeDeletion),
			rec("k", "v", 1, TypeValue),
		},
		failIdx: -1,
	}
	mi, err := NewMergingIterator([]Iterator[Record]{iter}, nil, RangeDesc)
	require.NoError(t, err)
	ri := NewRangeIterator(mi, RangeDesc)
	ri.anchors.MarkLastEmitted(rec("seed", "", 1, TypeValue), DirForward)

	ri.primeNext()

	staged, ok := ri.prefetch.Peek(DirReverse)
	require.True(t, ok, "next value should be staged after filtering tombstone")
	assert.Equal(t, Bytes("k"), staged.GetKey())
	assert.Equal(t, TypeValue, staged.GetType())
	assert.Greater(t, mi.fwd.Len(), 0, "tombstone candidate should be staged for future Prev calls")
}

func TestRangeIterator_ShouldCollapseForNextFalseAfterForward(t *testing.T) {
	ri := &RangeIterator{
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    RangeDesc,
	}
	ri.anchors.MarkLastEmitted(rec("k", "v", 1, TypeValue), DirForward)
	require.False(t, ri.shouldCollapseForNext())
}

func TestRangeIterator_PrimePrevWithOrderEarlyReturns(t *testing.T) {
	mi := &MergingIterator{
		fwd: NewPriorityQueue(func(a, b pqItem) bool { return false }),
		rev: NewPriorityQueue(func(a, b pqItem) bool { return false }),
	}
	ri := &RangeIterator{
		mi:       mi,
		cursor:   newRangeCursor(mi),
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    RangeAsc,
	}
	dir := oppositeDirection(directionFromOrder(RangeAsc))
	ri.anchors.pendingSet[dirIndex(dir)] = true

	ri.primePrevWithOrder(RangeAsc)

	ri.anchors.pendingSet[dirIndex(dir)] = false
	value := rec("x", "v", 1, TypeValue)
	ri.prefetch.Stage(dir, value)
	ri.primePrevWithOrder(RangeAsc)
	peeked, ok := ri.prefetch.Peek(dir)
	require.True(t, ok)
	assert.Equal(t, value, peeked)
}

func TestRangeIterator_PrimePrevWithOrderErrorPropagation(t *testing.T) {
	boom := errors.New("prime-prev")
	mi := &MergingIterator{
		fwd:          NewPriorityQueue(func(a, b pqItem) bool { return false }),
		rev:          NewPriorityQueue(func(a, b pqItem) bool { return false }),
		err:          boom,
		nextPrepared: false,
	}
	ri := &RangeIterator{
		mi:       mi,
		cursor:   newRangeCursor(mi),
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    RangeDesc,
	}

	ri.primePrevWithOrder(RangeDesc)
	assert.Equal(t, boom, ri.err)
}

func TestRangeIterator_PrimePrevWithOrderSkipsNilAndDeletion(t *testing.T) {
	mi := &MergingIterator{
		fwd:          NewPriorityQueue(func(a, b pqItem) bool { return false }),
		rev:          NewPriorityQueue(func(a, b pqItem) bool { return false }),
		prevPrepared: true,
		prevItem:     pqItem{},
	}
	ri := &RangeIterator{
		mi:       mi,
		cursor:   newRangeCursor(mi),
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    RangeAsc,
	}

	require.False(t, ri.stagePrevCandidate(nil, RangeAsc))
	require.False(t, ri.stagePrevCandidate(rec("k", "", 1, TypeDeletion), RangeAsc))

	ri.primePrevWithOrder(RangeAsc)
	assert.False(t, ri.prefetch.Has(oppositeDirection(directionFromOrder(RangeAsc))))
}

func TestRangeIterator_HasNextAnchorPending(t *testing.T) {
	ri := &RangeIterator{
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    RangeAsc,
	}
	dir := directionFromOrder(RangeAsc)
	ri.anchors.pending[dirIndex(dir)] = rec("k", "v", 1, TypeValue)
	ri.anchors.pendingSet[dirIndex(dir)] = true

	assert.True(t, ri.HasNext())
}

func TestRangeIterator_HasNextDropsRejectedPrefetch(t *testing.T) {
	ri := &RangeIterator{
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    RangeAsc,
	}
	dir := directionFromOrder(RangeAsc)
	value := rec("dup", "v", 1, TypeValue)
	ri.prefetch.Stage(dir, value)
	ri.filter.MarkEmitted(value, dir)
	ri.err = errors.New("stop")

	assert.False(t, ri.HasNext())
}

func TestRangeIterator_HasPrevAnchorPending(t *testing.T) {
	ri := &RangeIterator{
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    RangeAsc,
	}
	dir := oppositeDirection(directionFromOrder(RangeAsc))
	ri.anchors.pending[dirIndex(dir)] = rec("k", "v", 1, TypeValue)
	ri.anchors.pendingSet[dirIndex(dir)] = true

	assert.True(t, ri.HasPrev())
}

func TestRangeIterator_HasPrevDropsRejectedPrefetch(t *testing.T) {
	ri := &RangeIterator{
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    RangeAsc,
	}
	dir := oppositeDirection(directionFromOrder(RangeAsc))
	value := rec("dup", "v", 1, TypeValue)
	ri.prefetch.Stage(dir, value)
	ri.filter.MarkEmitted(value, dir)
	ri.err = errors.New("stop")

	assert.False(t, ri.HasPrev())
}

func TestRangeIterator_PullNextPreparedPropagatesError(t *testing.T) {
	ri := &RangeIterator{
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    RangeAsc,
	}
	boom := errors.New("pull-next")
	ri.err = boom

	_, ok, err := ri.pullNextPrepared(nil)
	assert.False(t, ok)
	assert.Equal(t, boom, err)
}

func TestRangeIterator_NextPropagatesStoredError(t *testing.T) {
	ri := &RangeIterator{
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    RangeAsc,
	}
	boom := errors.New("stored")
	ri.err = boom
	rec, err := ri.Next()
	assert.Nil(t, rec)
	assert.Equal(t, boom, err)
}

func TestRangeIterator_PrevPropagatesStoredError(t *testing.T) {
	mi := &MergingIterator{
		fwd: NewPriorityQueue(func(a, b pqItem) bool { return false }),
		rev: NewPriorityQueue(func(a, b pqItem) bool { return false }),
	}
	ri := &RangeIterator{
		mi:       mi,
		cursor:   newRangeCursor(mi),
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    RangeAsc,
	}
	boom := errors.New("prev stored")
	ri.err = boom
	rec, err := ri.Prev()
	assert.Nil(t, rec)
	assert.Equal(t, boom, err)
}

func TestRangeIterator_MakePrevPullerPropagatesError(t *testing.T) {
	mi := &MergingIterator{
		fwd: NewPriorityQueue(func(a, b pqItem) bool { return false }),
		rev: NewPriorityQueue(func(a, b pqItem) bool { return false }),
	}
	ri := &RangeIterator{
		mi:       mi,
		cursor:   newRangeCursor(mi),
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    RangeAsc,
	}
	boom := errors.New("puller")
	ri.err = boom

	puller := ri.makePrevPuller(RangeAsc)
	rec, ok, err := puller(nil)
	assert.Nil(t, rec)
	assert.False(t, ok)
	assert.Equal(t, boom, err)
}

func TestRangeIterator_LastErrorPaths(t *testing.T) {
	boom := errors.New("last")
	mi := &MergingIterator{
		fwd: NewPriorityQueue(func(a, b pqItem) bool { return false }),
		rev: NewPriorityQueue(func(a, b pqItem) bool { return false }),
		err: boom,
	}
	ri := &RangeIterator{
		mi:       mi,
		cursor:   newRangeCursor(mi),
		filter:   newRecordFilter(nil),
		anchors:  &anchorState{},
		prefetch: &prefetchState{},
		order:    RangeAsc,
	}

	_, err := ri.Last()
	assert.Equal(t, boom, err)
}
