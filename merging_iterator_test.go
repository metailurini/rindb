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
	rec := e.records[e.idx]
	e.idx++
	return rec, nil
}

func TestMergingIterator_MergesRecords(t *testing.T) {
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
