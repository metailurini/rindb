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

func mkRec(k, v string, seq uint64, typ RecordType) Record {
	var nv Bytes
	if v != "" {
		nv = Bytes(v)
	}
	return RecordImpl{Key: Bytes(k), Value: nv, SequenceNumber: seq, Type: typ}
}

func TestMergingIterator(t *testing.T) {
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

			mi, err := NewMergingIterator(iterators, nil)
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

func TestMergingIterator_MergesRecords(t *testing.T) {
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

            mi, err := NewMergingIterator(iterators, nil)
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

func TestMergingIteratorPrev(t *testing.T) {
	iterators := []Iterator[Record]{
		&errIterator{records: []Record{mkRec("a", "va", 1, TypeValue), mkRec("c", "vc", 1, TypeValue)}, failIdx: -1},
		&errIterator{records: []Record{mkRec("b", "vb", 1, TypeValue)}, failIdx: -1},
	}
	mi, err := NewMergingIterator(iterators, nil)
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

func TestMergingIteratorPrevBeforeNext(t *testing.T) {
	iterators := []Iterator[Record]{
		&errIterator{records: []Record{mkRec("a", "va", 1, TypeValue)}, failIdx: -1},
		&errIterator{records: []Record{mkRec("b", "vb", 1, TypeValue)}, failIdx: -1},
	}
	mi, err := NewMergingIterator(iterators, nil)
	assert.NoError(t, err)

	assert.Equal(t, 2, mi.fwd.Len())
	assert.Equal(t, 0, mi.rev.Len())

	_, err = mi.Prev()
	assert.ErrorIs(t, err, EOI)

	assert.Equal(t, 2, mi.fwd.Len())
	assert.Equal(t, 0, mi.rev.Len())
}

func TestMergingIteratorAlternating(t *testing.T) {
	iterators := []Iterator[Record]{
		&errIterator{records: []Record{mkRec("a", "va", 1, TypeValue)}, failIdx: -1},
		&errIterator{records: []Record{mkRec("b", "vb", 1, TypeValue)}, failIdx: -1},
	}
	mi, err := NewMergingIterator(iterators, nil)
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

func TestMergingIterator_ErrorPropagation(t *testing.T) {
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
            mi, err := NewMergingIterator([]Iterator[Record]{it}, nil)
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
