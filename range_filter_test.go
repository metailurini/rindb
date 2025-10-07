package rindb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecordFilter_AcceptsVisibleValues(t *testing.T) {
	t.Parallel()
	filter := newRecordFilter(nil)
	rec := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 4, Type: TypeValue}

	accepted, ok := filter.accept(rec, DirForward)

	require.True(t, ok)
	require.Equal(t, rec, accepted)
}

func TestRecordFilter_RejectsTombstones(t *testing.T) {
	t.Parallel()
	filter := newRecordFilter(nil)
	rec := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 4, Type: TypeDeletion}

	accepted, ok := filter.accept(rec, DirForward)

	require.False(t, ok)
	require.Nil(t, accepted)
}

func TestRecordFilter_RejectsSequenceBeyondSnapshot(t *testing.T) {
	t.Parallel()
	snapshot := uint64(5)
	filter := newRecordFilter(&snapshot)
	rec := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 6, Type: TypeValue}

	accepted, ok := filter.accept(rec, DirForward)

	require.False(t, ok)
	require.Nil(t, accepted)
}

func TestRecordFilter_DeduplicatesKeysPerDirection(t *testing.T) {
	t.Parallel()
	filter := newRecordFilter(nil)
	rec := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 6, Type: TypeValue}
	older := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 3, Type: TypeValue}

	accepted, ok := filter.accept(rec, DirForward)
	require.True(t, ok)
	require.Equal(t, rec, accepted)

	second, ok := filter.accept(older, DirForward)
	require.False(t, ok)
	require.Nil(t, second)

	third, ok := filter.accept(older, DirReverse)
	require.True(t, ok)
	require.Equal(t, older, third)
}

func TestRecordFilter_ResetClearsSeenKey(t *testing.T) {
	t.Parallel()
	filter := newRecordFilter(nil)
	rec := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 6, Type: TypeValue}
	older := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 3, Type: TypeValue}

	accepted, ok := filter.accept(rec, DirForward)
	require.True(t, ok)
	require.Equal(t, rec, accepted)

	filter.reset()

	replay, ok := filter.accept(older, DirForward)
	require.True(t, ok)
	require.Equal(t, older, replay)
}

func TestRecordFilter_MarkEmittedUpdatesState(t *testing.T) {
	t.Parallel()
	filter := newRecordFilter(nil)
	emitted := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 6, Type: TypeValue}
	older := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 3, Type: TypeValue}

	filter.markEmitted(emitted, DirForward)

	second, ok := filter.accept(older, DirForward)
	require.False(t, ok)
	require.Nil(t, second)
}

func TestRecordFilter_MarkEmittedMaintainsDirection(t *testing.T) {
	t.Parallel()
	filter := newRecordFilter(nil)
	emitted := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 6, Type: TypeValue}
	older := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 3, Type: TypeValue}

	filter.markEmitted(emitted, DirReverse)

	suppressed, ok := filter.accept(older, DirReverse)
	require.False(t, ok)
	require.Nil(t, suppressed)

	replay, ok := filter.accept(older, DirForward)
	require.True(t, ok)
	require.Equal(t, older, replay)
}

func TestRecordFilter_MarkEmittedIgnoresNilRecord(t *testing.T) {
	t.Parallel()
	filter := newRecordFilter(nil)
	other := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 6, Type: TypeValue}

	filter.markEmitted(nil, DirForward)

	accepted, ok := filter.accept(other, DirForward)
	require.True(t, ok)
	require.Equal(t, other, accepted)
}

func TestRecordFilter_AcceptHandlesNilRecord(t *testing.T) {
	t.Parallel()
	filter := newRecordFilter(nil)

	rec, ok := filter.accept(nil, DirForward)

	require.False(t, ok)
	require.Nil(t, rec)
}
