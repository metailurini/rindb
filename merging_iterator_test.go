package rindb

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestMergingIterator(t *testing.T) {
	rec := func(k, v string, seq uint64, typ RecordType) Record {
		var nv Bytes = nil
		if v != "" {
			nv = Bytes(v)
		}
		return RecordImpl{Key: Bytes(k), Value: nv, SequenceNumber: seq, Type: typ}
	}

	tests := []struct {
		name  string
		iters [][]Record
		want  []Record
	}{
		{
			name: "includes duplicates and tombstones",
			iters: [][]Record{
				{rec("a", "v2", 2, TypeValue), rec("b", "vb", 1, TypeValue)},
				{rec("a", "", 3, TypeDeletion), rec("a", "v1", 1, TypeValue)},
			},
			want: []Record{
				rec("a", "", 3, TypeDeletion),
				rec("a", "v2", 2, TypeValue),
				rec("a", "v1", 1, TypeValue),
				rec("b", "vb", 1, TypeValue),
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
	rec := func(k, v string, seq uint64, typ RecordType) Record {
		var nv Bytes = nil
		if v != "" {
			nv = Bytes(v)
		}
		return RecordImpl{Key: Bytes(k), Value: nv, SequenceNumber: seq, Type: typ}
	}
	iterators := []Iterator[Record]{
		&errIterator{records: []Record{rec("a", "va", 1, TypeValue), rec("c", "vc", 1, TypeValue)}, failIdx: -1},
		&errIterator{records: []Record{rec("b", "vb", 1, TypeValue)}, failIdx: -1},
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

	fwd, err := mi.Next()
	assert.NoError(t, err)
	assert.Equal(t, r2, fwd)
}

func TestMergingIteratorRewindSkipsTombstones(t *testing.T) {
	rec := func(k, v string, seq uint64, typ RecordType) Record {
		var nv Bytes = nil
		if v != "" {
			nv = Bytes(v)
		}
		return RecordImpl{Key: Bytes(k), Value: nv, SequenceNumber: seq, Type: typ}
	}
	iterators := []Iterator[Record]{
		&errIterator{records: []Record{rec("a", "", 3, TypeDeletion), rec("a", "v1", 2, TypeValue)}, failIdx: -1},
	}
	mi, err := NewMergingIterator(iterators, nil)
	assert.NoError(t, err)

	_, err = mi.Next() // tombstone
	assert.NoError(t, err)
	r, err := mi.Next() // value
	assert.NoError(t, err)

	back, err := mi.Prev()
	assert.NoError(t, err)
	assert.Equal(t, r, back)

	fwd, err := mi.Next()
	assert.NoError(t, err)
	assert.Equal(t, r, fwd)
}

func TestMergingIteratorSeedsReverseHeap(t *testing.T) {
	rec := func(k string) Record {
		return RecordImpl{Key: Bytes(k), Value: Bytes("v"), SequenceNumber: 1, Type: TypeValue}
	}
	iterators := []Iterator[Record]{
		&errIterator{records: []Record{rec("a")}, failIdx: -1},
		&errIterator{records: []Record{rec("b")}, failIdx: -1},
	}
	mi, err := NewMergingIterator(iterators, nil)
	assert.NoError(t, err)

	assert.True(t, mi.HasPrev())
	r, err := mi.Prev()
	assert.NoError(t, err)
	assert.Equal(t, Bytes("b"), r.GetKey())
}

func TestMergingIteratorInitialError(t *testing.T) {
	r := newRecord(Bytes("a"), Bytes("1"), 1)
	it := &errIterator{records: []Record{r}, failIdx: 0}
	mi, err := NewMergingIterator([]Iterator[Record]{it}, nil)
	assert.Nil(t, mi)
	assert.EqualError(t, err, "boom")
}
