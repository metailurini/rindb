package rindb

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errIterator injects an error at a specified index.
type errIterator struct {
	records []Record
	idx     int
	failIdx int
}

func (e *errIterator) HasNext() bool { return e.idx < len(e.records) }

func (e *errIterator) Next() (Record, error) {
	if e.idx == e.failIdx {
		e.idx++
		var empty Record
		return empty, errors.New("boom")
	}
	if e.idx >= len(e.records) {
		var empty Record
		return empty, EOI
	}
	rec := e.records[e.idx]
	e.idx++
	return rec, nil
}

func (e *errIterator) HasPrev() bool { return e.idx > 0 }

func (e *errIterator) Prev() (Record, error) {
	if !e.HasPrev() {
		var empty Record
		return empty, EOI
	}
	e.idx--
	if e.idx == e.failIdx {
		var empty Record
		return empty, errors.New("boom")
	}
	return e.records[e.idx], nil
}

func (e *errIterator) Last() (Record, error) {
	var empty Record
	if len(e.records) == 0 {
		return empty, EOI
	}
	e.idx = len(e.records) - 1
	return e.records[e.idx], nil
}

type lastErrorIterator struct {
	err error
}

func (l *lastErrorIterator) HasNext() bool { return false }

func (l *lastErrorIterator) Next() (Record, error) {
	var empty Record
	return empty, EOI
}

func (l *lastErrorIterator) HasPrev() bool { return false }

func (l *lastErrorIterator) Prev() (Record, error) {
	var empty Record
	return empty, EOI
}

func (l *lastErrorIterator) Last() (Record, error) {
	var empty Record
	return empty, l.err
}

func mkRec(k, v string, seq uint64, typ RecordType) Record {
	var nv Bytes
	if v != "" {
		nv = Bytes(v)
	}
	return RecordImpl{Key: Bytes(k), Value: nv, SequenceNumber: seq, Type: typ}
}

func TestMergingIterator_IncludesDuplicatesAndTombstones(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		iters [][]Record
		want  []Record
	}{
		{
			name: "includes duplicates and tombstones",
			iters: [][]Record{
				{mkRec("a", "v2", 2, TypeValue), mkRec("b", "vb", 1, TypeValue)},
				{mkRec("a", "", 3, TypeDeletion), mkRec("a", "v1", 1, TypeValue)},
			},
			want: []Record{
				mkRec("a", "", 3, TypeDeletion),
				mkRec("a", "v2", 2, TypeValue),
				mkRec("a", "v1", 1, TypeValue),
				mkRec("b", "vb", 1, TypeValue),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var iterators []Iterator[Record]
			for _, recs := range tt.iters {
				iterators = append(iterators, &errIterator{records: recs, failIdx: -1})
			}

			mi, err := NewMergingIterator(iterators, nil, RangeAsc)
			assert.NoError(t, err)

			var got []Record
			for mi.HasNext() {
				r, err := mi.Next()
				assert.NoError(t, err)
				got = append(got, r)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMergingIterator_LastErrorSurfaced(t *testing.T) {
	t.Parallel()
	boom := errors.New("tail boom")
	_, err := NewMergingIterator([]Iterator[Record]{&lastErrorIterator{err: boom}}, nil, RangeDesc)
	assert.EqualError(t, err, "tail boom")
}

func TestMergingIterator_LastReturnsMaxAndPrimesPrev(t *testing.T) {
	t.Parallel()
	iterators := []Iterator[Record]{
		&errIterator{records: []Record{mkRec("a", "va", 1, TypeValue), mkRec("c", "vc1", 2, TypeValue)}, failIdx: -1},
		&errIterator{records: []Record{mkRec("b", "vb", 1, TypeValue), mkRec("c", "vc2", 3, TypeValue)}, failIdx: -1},
	}
	mi, err := NewMergingIterator(iterators, nil, RangeAsc)
	require.NoError(t, err)

	last, err := mi.Last()
	require.NoError(t, err)
	assert.Equal(t, mkRec("c", "vc2", 3, TypeValue), last)

	prev, err := mi.Prev()
	require.NoError(t, err)
	assert.Equal(t, mkRec("c", "vc1", 2, TypeValue), prev)

	prev2, err := mi.Prev()
	require.NoError(t, err)
	assert.Equal(t, mkRec("b", "vb", 1, TypeValue), prev2)
}

func TestMergingIterator_LastEmpty(t *testing.T) {
	t.Parallel()
	iterators := []Iterator[Record]{&errIterator{records: nil, failIdx: -1}}
	mi, err := NewMergingIterator(iterators, nil, RangeAsc)
	require.NoError(t, err)

	_, err = mi.Last()
	assert.ErrorIs(t, err, EOI)
}

func TestMergingIterator_MergesRecords(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		iters [][]Record
		want  []Record
	}{
		{
			name: "merges records across iterators",
			iters: [][]Record{
				{mkRec("a", "v2", 2, TypeValue), mkRec("b", "vb", 1, TypeValue)},
				{mkRec("a", "", 3, TypeDeletion), mkRec("a", "v1", 1, TypeValue)},
			},
			want: []Record{
				mkRec("a", "", 3, TypeDeletion),
				mkRec("a", "v2", 2, TypeValue),
				mkRec("a", "v1", 1, TypeValue),
				mkRec("b", "vb", 1, TypeValue),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var iterators []Iterator[Record]
			for _, recs := range tt.iters {
				iterators = append(iterators, &errIterator{records: recs, failIdx: -1})
			}

			mi, err := NewMergingIterator(iterators, nil, RangeAsc)
			assert.NoError(t, err)

			var got []Record
			for mi.HasNext() {
				r, err := mi.Next()
				assert.NoError(t, err)
				got = append(got, r)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMergingIterator_PrevMovesBackward(t *testing.T) {
	t.Parallel()
	iterators := []Iterator[Record]{
		&errIterator{records: []Record{mkRec("a", "va", 1, TypeValue), mkRec("c", "vc", 1, TypeValue)}, failIdx: -1},
		&errIterator{records: []Record{mkRec("b", "vb", 1, TypeValue)}, failIdx: -1},
	}
	mi, err := NewMergingIterator(iterators, nil, RangeAsc)
	assert.NoError(t, err)

	_, err = mi.Next()
	assert.NoError(t, err)
	assert.True(t, mi.HasPrev())

	r2, err := mi.Next()
	assert.NoError(t, err)
	assert.True(t, mi.HasPrev())

	back, err := mi.Prev()
	assert.NoError(t, err)
	assert.Equal(t, r2, back)

	nextRec, err := mi.Next()
	assert.NoError(t, err)
	assert.Equal(t, r2, nextRec)
}

func TestMergingIterator_PrevBeforeNextReturnsEOI(t *testing.T) {
	t.Parallel()
	iterators := []Iterator[Record]{
		&errIterator{records: []Record{mkRec("a", "va", 1, TypeValue)}, failIdx: -1},
		&errIterator{records: []Record{mkRec("b", "vb", 1, TypeValue)}, failIdx: -1},
	}
	mi, err := NewMergingIterator(iterators, nil, RangeAsc)
	assert.NoError(t, err)

	assert.Equal(t, 2, mi.fwd.Len())
	assert.Equal(t, 0, mi.rev.Len())

	_, err = mi.Prev()
	assert.ErrorIs(t, err, EOI)

	assert.Equal(t, 2, mi.fwd.Len())
	assert.Equal(t, 0, mi.rev.Len())
}

func TestMergingIterator_AlternatingNextPrev(t *testing.T) {
	t.Parallel()
	iterators := []Iterator[Record]{
		&errIterator{records: []Record{mkRec("a", "va", 1, TypeValue)}, failIdx: -1},
		&errIterator{records: []Record{mkRec("b", "vb", 1, TypeValue)}, failIdx: -1},
	}
	mi, err := NewMergingIterator(iterators, nil, RangeAsc)
	assert.NoError(t, err)

	assert.Equal(t, 2, mi.fwd.Len())
	assert.Equal(t, 0, mi.rev.Len())

	r1, err := mi.Next()
	assert.NoError(t, err)
	assert.Equal(t, mkRec("a", "va", 1, TypeValue), r1)
	assert.Equal(t, 1, mi.fwd.Len())
	assert.Equal(t, 1, mi.rev.Len())

	r2, err := mi.Next()
	assert.NoError(t, err)
	assert.Equal(t, mkRec("b", "vb", 1, TypeValue), r2)
	assert.Equal(t, 0, mi.fwd.Len())
	assert.Equal(t, 2, mi.rev.Len())

	back, err := mi.Prev()
	assert.NoError(t, err)
	assert.Equal(t, r2, back)
	assert.Equal(t, 1, mi.fwd.Len())
	assert.Equal(t, 1, mi.rev.Len())

	nextRec, err := mi.Next()
	assert.NoError(t, err)
	assert.Equal(t, r2, nextRec)
	assert.Equal(t, 1, mi.fwd.Len())
	assert.Equal(t, 2, mi.rev.Len())
}

type iteratorOp int

const (
	opNext iteratorOp = iota
	opPrev
)

type expectedOp struct {
	op   iteratorOp
	want Record
}

func runIteratorSequence(t *testing.T, mi *MergingIterator, ops []expectedOp) {
	t.Helper()
	for i, expected := range ops {
		var rec Record
		var err error
		switch expected.op {
		case opNext:
			rec, err = mi.Next()
		case opPrev:
			rec, err = mi.Prev()
		}
		require.NoError(t, err, "operation %d failed", i)
		assert.Equal(t, expected.want, rec, "operation %d result mismatch", i)
	}
}

func TestMergingIterator_NextPrevSequences(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		records []Record
		ops     []expectedOp
	}{
		{
			name: "PrevTwiceThenForwardMovesCorrectly",
			records: []Record{
				mkRec("a", "va", 1, TypeValue),
				mkRec("b", "vb", 1, TypeValue),
				mkRec("c", "vc", 1, TypeValue),
			},
			ops: []expectedOp{
				{opNext, mkRec("a", "va", 1, TypeValue)}, // Consume two elements so that both appear in the reverse queue.
				{opNext, mkRec("b", "vb", 1, TypeValue)},
				{opPrev, mkRec("b", "vb", 1, TypeValue)}, // Walk backwards twice to the beginning.
				{opPrev, mkRec("a", "va", 1, TypeValue)},
				{opNext, mkRec("a", "va", 1, TypeValue)}, // Moving forward should yield each element exactly once in order.
				{opNext, mkRec("b", "vb", 1, TypeValue)},
				{opNext, mkRec("c", "vc", 1, TypeValue)},
			},
		},
		{
			name: "ComplexNextPrevSequence",
			records: []Record{
				mkRec("a", "va", 1, TypeValue),
				mkRec("b", "vb", 1, TypeValue),
			},
			ops: []expectedOp{
				{opNext, mkRec("a", "va", 1, TypeValue)},
				{opPrev, mkRec("a", "va", 1, TypeValue)},
				{opNext, mkRec("a", "va", 1, TypeValue)},
				{opPrev, mkRec("a", "va", 1, TypeValue)},
				{opNext, mkRec("a", "va", 1, TypeValue)},
				{opNext, mkRec("b", "vb", 1, TypeValue)},
				{opPrev, mkRec("b", "vb", 1, TypeValue)},
				{opNext, mkRec("b", "vb", 1, TypeValue)},
				{opPrev, mkRec("b", "vb", 1, TypeValue)},
				{opPrev, mkRec("a", "va", 1, TypeValue)},
				{opNext, mkRec("a", "va", 1, TypeValue)},
				{opPrev, mkRec("a", "va", 1, TypeValue)},
				{opNext, mkRec("a", "va", 1, TypeValue)},
				{opNext, mkRec("b", "vb", 1, TypeValue)},
				{opPrev, mkRec("b", "vb", 1, TypeValue)},
				{opNext, mkRec("b", "vb", 1, TypeValue)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iterators := []Iterator[Record]{&errIterator{records: tt.records, failIdx: -1}}

			mi, err := NewMergingIterator(iterators, nil, RangeAsc)
			require.NoError(t, err)

			runIteratorSequence(t, mi, tt.ops)
		})
	}
}

func TestMergingIterator_ErrorPropagation(t *testing.T) {
	t.Parallel()
	r1 := newRecord(Bytes("a"), Bytes("1"), 1)
	r2 := newRecord(Bytes("b"), Bytes("2"), 2)
	cases := []struct {
		name       string
		records    []Record
		failIdx    int
		expectInit bool
	}{
		{name: "initial error", records: []Record{r1}, failIdx: 0, expectInit: true},
		{name: "error on next", records: []Record{r1, r2}, failIdx: 1, expectInit: false},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			it := &errIterator{records: tt.records, failIdx: tt.failIdx}
			mi, err := NewMergingIterator([]Iterator[Record]{it}, nil, RangeAsc)
			if tt.expectInit {
				assert.Nil(t, mi)
				assert.EqualError(t, err, "boom")
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, mi)
			_, err = mi.Next()
			require.NoError(t, err)
			_, err = mi.Next()
			assert.EqualError(t, err, "boom")
		})
	}
}

func TestNewMergingIterator_CleanupOnInitError(t *testing.T) {
	t.Parallel()
	iter := &errIterator{records: []Record{mkRec("a", "va", 1, TypeValue)}, failIdx: 0}
	cleaned := false
	mi, err := NewMergingIterator([]Iterator[Record]{iter}, func() { cleaned = true }, RangeAsc)
	assert.Nil(t, mi)
	assert.EqualError(t, err, "boom")
	assert.True(t, cleaned)
}

func TestMergingIterator_PrepareNextNoopWhenPrepared(t *testing.T) {
	t.Parallel()
	iter := &errIterator{records: []Record{mkRec("a", "va", 1, TypeValue)}, failIdx: -1}
	mi, err := NewMergingIterator([]Iterator[Record]{iter}, nil, RangeAsc)
	require.NoError(t, err)

	require.True(t, mi.HasNext())

	prepared := mi.nextItem
	fwdLen := mi.fwd.Len()
	revLen := mi.rev.Len()

	mi.prepareNext()

	assert.True(t, mi.nextPrepared)
	assert.Equal(t, prepared, mi.nextItem)
	assert.Equal(t, fwdLen, mi.fwd.Len())
	assert.Equal(t, revLen, mi.rev.Len())
}

func TestMergingIterator_HasNextAfterIteratorError(t *testing.T) {
	t.Parallel()
	iter := &errIterator{
		records: []Record{
			mkRec("a", "v1", 1, TypeValue),
			mkRec("b", "v2", 1, TypeValue),
		},
		failIdx: 1,
	}

	mi, err := NewMergingIterator([]Iterator[Record]{iter}, nil, RangeAsc)
	require.NoError(t, err)

	first, err := mi.Next()
	require.NoError(t, err)
	assert.Equal(t, mkRec("a", "v1", 1, TypeValue), first)

	assert.False(t, mi.HasNext())

	_, err = mi.Next()
	assert.EqualError(t, err, "boom")
	assert.EqualError(t, mi.err, "boom")
}

func TestMergingIterator_PrevErrorPropagation(t *testing.T) {
	t.Parallel()
	iter := &errIterator{
		records: []Record{mkRec("a", "va", 1, TypeValue)},
		failIdx: -1,
	}

	mi, err := NewMergingIterator([]Iterator[Record]{iter}, nil, RangeAsc)
	require.NoError(t, err)

	require.True(t, mi.HasNext())
	first, err := mi.Next()
	require.NoError(t, err)

	iter.failIdx = 0

	assert.True(t, mi.HasPrev())
	back, err := mi.Prev()
	assert.Equal(t, first, back)
	assert.EqualError(t, err, "boom")
	assert.EqualError(t, mi.err, "boom")

	mi.prepareNext()
	assert.False(t, mi.nextPrepared)
}
