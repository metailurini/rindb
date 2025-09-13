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
	rec := e.records[e.idx]
	e.idx++
	return rec, nil
}

func (e *errIterator) HasPrev() bool { return e.idx > 0 }

func (e *errIterator) Prev() (Record, error) {
	if e.idx == 0 {
		var empty Record
		return empty, EOI
	}
	e.idx--
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
			var iterators []BiIterator[Record]
			for _, recs := range tt.iters {
				iterators = append(iterators, &errIterator{records: recs, failIdx: -1})
			}

			mi, err := NewMergingIterator(iterators, false, nil)
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

func TestMergingIteratorReverse(t *testing.T) {
	rec := func(k string) Record { return newRecord(Bytes(k), nil, 1) }

	it1 := &errIterator{records: []Record{rec("a"), rec("c")}, idx: 2, failIdx: -1}
	it2 := &errIterator{records: []Record{rec("b")}, idx: 1, failIdx: -1}

	mi, err := NewMergingIterator([]BiIterator[Record]{it1, it2}, true, nil)
	assert.NoError(t, err)

	var got []string
	for mi.HasNext() {
		r, err := mi.Next()
		assert.NoError(t, err)
		got = append(got, string(r.GetKey()))
	}
	assert.Equal(t, []string{"c", "b", "a"}, got)
}

func TestMergingIteratorInitialError(t *testing.T) {
	r := newRecord(Bytes("a"), Bytes("1"), 1)
	it := &errIterator{records: []Record{r}, failIdx: 0}
	mi, err := NewMergingIterator([]BiIterator[Record]{it}, false, nil)
	assert.Nil(t, mi)
	assert.EqualError(t, err, "boom")
}
