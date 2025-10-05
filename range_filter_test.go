package rindb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecordFilterAcceptsVisibleValues(t *testing.T) {
	filter := newRecordFilter(nil)
	rec := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 4, Type: TypeValue}

	accepted, ok := filter.Accept(rec, DirForward)

	require.True(t, ok)
	require.Equal(t, rec, accepted)
}

func TestRecordFilterRejectsTombstones(t *testing.T) {
	filter := newRecordFilter(nil)
	rec := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 4, Type: TypeDeletion}

	accepted, ok := filter.Accept(rec, DirForward)

	require.False(t, ok)
	require.Nil(t, accepted)
}

func TestRecordFilterRejectsSequenceBeyondSnapshot(t *testing.T) {
	snapshot := uint64(5)
	filter := newRecordFilter(&snapshot)
	rec := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 6, Type: TypeValue}

	accepted, ok := filter.Accept(rec, DirForward)

	require.False(t, ok)
	require.Nil(t, accepted)
}

func TestRecordFilterDeduplicatesKeysPerDirection(t *testing.T) {
	filter := newRecordFilter(nil)
	rec := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 6, Type: TypeValue}
	older := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 3, Type: TypeValue}

	accepted, ok := filter.Accept(rec, DirForward)
	require.True(t, ok)
	require.Equal(t, rec, accepted)

	second, ok := filter.Accept(older, DirForward)
	require.False(t, ok)
	require.Nil(t, second)

	third, ok := filter.Accept(older, DirReverse)
	require.True(t, ok)
	require.Equal(t, older, third)
}

func TestRecordFilterResetClearsSeenKey(t *testing.T) {
	filter := newRecordFilter(nil)
	rec := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 6, Type: TypeValue}
	older := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 3, Type: TypeValue}

	accepted, ok := filter.Accept(rec, DirForward)
	require.True(t, ok)
	require.Equal(t, rec, accepted)

	filter.Reset()

	replay, ok := filter.Accept(older, DirForward)
	require.True(t, ok)
	require.Equal(t, older, replay)
}

func TestRecordFilterMarkEmittedUpdatesState(t *testing.T) {
	filter := newRecordFilter(nil)
	emitted := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 6, Type: TypeValue}
	older := RecordImpl{Key: Bytes("alpha"), SequenceNumber: 3, Type: TypeValue}

	filter.MarkEmitted(emitted)

	second, ok := filter.Accept(older, DirForward)
	require.False(t, ok)
	require.Nil(t, second)
}

func TestRecordFilterAcceptHandlesNilRecord(t *testing.T) {
	filter := newRecordFilter(nil)

	rec, ok := filter.Accept(nil, DirForward)

	require.False(t, ok)
	require.Nil(t, rec)
}
