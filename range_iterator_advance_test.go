package rindb

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRangeIteratorAdvance_ReplaysPendingAnchor(t *testing.T) {
	t.Parallel()

	ri := &RangeIterator{
		filter:  newRecordFilter(nil),
		anchors: &anchorState{},
		cursor:  &rangeCursor{},
	}

	last := rec("a", "va", 1, TypeValue)
	ri.anchors.markLastEmitted(last, DirForward)

	// Seed the anchor state with an initial forward traversal so the next
	// direction switch stages the last emitted record.
	changed := ri.anchors.onDirectionChange(DirForward)
	require.False(t, changed)
	ri.filter.markEmitted(last, DirForward)

	pullCalls := 0
	pull := func(*rangeCursor) (Record, bool, error) {
		pullCalls++
		return rec("b", "vb", 2, TypeValue), true, nil
	}

	replayed, ok, err := ri.advance(DirReverse, pull)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, Bytes("a"), replayed.GetKey())
	assert.Equal(t, Bytes("va"), replayed.GetValue())
	assert.Equal(t, 0, pullCalls, "anchor replay should bypass cursor pulls")
	assert.Equal(t, DirReverse, ri.anchors.currentDir)
	assert.Equal(t, Bytes("a"), ri.anchors.recent.GetKey())
}

func TestRangeIteratorAdvance_FiltersUntilAccept(t *testing.T) {
	t.Parallel()

	ri := &RangeIterator{
		filter:  newRecordFilter(nil),
		anchors: &anchorState{},
		cursor:  &rangeCursor{},
	}

	records := []Record{
		rec("a", "", 3, TypeDeletion),
		rec("a", "va", 2, TypeValue),
		rec("a", "old", 1, TypeValue),
		rec("b", "vb", 1, TypeValue),
	}

	pull := func(*rangeCursor) (Record, bool, error) {
		if len(records) == 0 {
			return nil, false, nil
		}
		r := records[0]
		records = records[1:]
		return r, true, nil
	}

	first, ok, err := ri.advance(DirForward, pull)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, Bytes("a"), first.GetKey())
	assert.Equal(t, Bytes("va"), first.GetValue())

	second, ok, err := ri.advance(DirForward, pull)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, Bytes("b"), second.GetKey())

	_, ok, err = ri.advance(DirForward, pull)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestRangeIteratorAdvance_PropagatesPullError(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	ri := &RangeIterator{
		filter:  newRecordFilter(nil),
		anchors: &anchorState{},
		cursor:  &rangeCursor{},
	}

	pull := func(*rangeCursor) (Record, bool, error) {
		return nil, false, boom
	}

	_, _, err := ri.advance(DirForward, pull)
	assert.ErrorIs(t, err, boom)
}
