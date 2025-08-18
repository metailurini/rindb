package rindb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// sliceIterator iterates over a predefined slice of records.
type sliceIterator struct {
	records []Record
	idx     int
}

func (s *sliceIterator) HasNext() bool { return s.idx < len(s.records) }

func (s *sliceIterator) Next() (Record, error) {
	if !s.HasNext() {
		var empty Record
		return empty, EOI
	}
	rec := s.records[s.idx]
	s.idx++
	return rec, nil
}

func TestRangeIteratorPrepare(t *testing.T) {
	r1 := NewRecord(Bytes("a"), Bytes("1"), 1)
	r2 := NewRecord(Bytes("b"), Bytes("2"), 2)
	tomb := NewRecord(Bytes("a"), nil, 3)
	newer := NewRecord(Bytes("a"), Bytes("3"), 4)

	tests := []struct {
		name             string
		iterators        []Iterator[Record]
		expectedPrepared bool
		expectedNext     Record
		expectedErr      string
		expectClosed     bool
	}{
		{
			name: "returns smallest key",
			iterators: []Iterator[Record]{
				&sliceIterator{records: []Record{r1}},
				&sliceIterator{records: []Record{r2}},
			},
			expectedPrepared: true,
			expectedNext:     r1,
			expectClosed:     false,
		},
		{
			name: "skips tombstones and closes when empty",
			iterators: []Iterator[Record]{
				&sliceIterator{records: []Record{r1}},
				&sliceIterator{records: []Record{tomb}},
			},
			expectedPrepared: false,
			expectedNext:     nil,
			expectClosed:     true,
		},
		{
			name: "chooses highest sequence for duplicate keys",
			iterators: []Iterator[Record]{
				&sliceIterator{records: []Record{r1}},
				&sliceIterator{records: []Record{newer}},
			},
			expectedPrepared: true,
			expectedNext:     newer,
			expectClosed:     false,
		},
		{
			name: "propagates iterator error",
			iterators: []Iterator[Record]{
				&errIterator{records: []Record{r1, r2}},
			},
			expectedPrepared: true,
			expectedNext:     r1,
			expectedErr:      "boom",
			expectClosed:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pq := buildRangePQ(tt.iterators)
			closed := false
			ri := RangeIterator{pq: pq, cleanup: func() { closed = true }}

			ri.prepare()

			assert.Equal(t, tt.expectedPrepared, ri.prepared)
			if tt.expectedNext != nil {
				assert.Equal(t, tt.expectedNext.GetKey(), ri.next.GetKey())
				assert.Equal(t, tt.expectedNext.GetSequenceNumber(), ri.next.GetSequenceNumber())
			}
			if tt.expectedErr != "" {
				assert.EqualError(t, ri.err, tt.expectedErr)
			} else {
				assert.NoError(t, ri.err)
			}
			assert.Equal(t, tt.expectClosed, closed)
		})
	}
}
