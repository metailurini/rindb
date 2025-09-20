package rindb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergingIterator_HasNext_PrepareIdempotent(t *testing.T) {
	t.Parallel()
	iterators := []Iterator[Record]{
		&errIterator{records: []Record{mkRec("a", "va", 1, TypeValue)}, failIdx: -1},
		&errIterator{records: []Record{mkRec("b", "vb", 1, TypeValue)}, failIdx: -1},
	}
	mi, err := NewMergingIterator(iterators, nil)
	assert.NoError(t, err)

	// Call HasNext multiple times; it should not advance.
	assert.True(t, mi.HasNext())
	assert.True(t, mi.HasNext())

	r1, err := mi.Next()
	assert.NoError(t, err)
	assert.Equal(t, mkRec("a", "va", 1, TypeValue), r1)

	r2, err := mi.Next()
	assert.NoError(t, err)
	assert.Equal(t, mkRec("b", "vb", 1, TypeValue), r2)
}

func TestMergingIterator_HasPrev_PrepareIdempotent(t *testing.T) {
	t.Parallel()
	iterators := []Iterator[Record]{
		&errIterator{records: []Record{mkRec("a", "va", 1, TypeValue)}, failIdx: -1},
		&errIterator{records: []Record{mkRec("b", "vb", 1, TypeValue)}, failIdx: -1},
	}
	mi, err := NewMergingIterator(iterators, nil)
	assert.NoError(t, err)

	// Move forward once to establish a current element.
	r1, err := mi.Next()
	assert.NoError(t, err)
	assert.Equal(t, mkRec("a", "va", 1, TypeValue), r1)

	// Prepare previous multiple times; should not over-advance or corrupt state.
	assert.True(t, mi.HasPrev())
	assert.True(t, mi.HasPrev())

	back, err := mi.Prev()
	assert.Equal(t, mkRec("a", "va", 1, TypeValue), back)
	assert.NoError(t, err)

	// Next again should return the same element per iterator contract.
	rAgain, err := mi.Next()
	assert.NoError(t, err)
	assert.Equal(t, mkRec("a", "va", 1, TypeValue), rAgain)
}

func TestMergingIterator_HasNext_PrefetchErrorSurfacedAfterConsume(t *testing.T) {
	t.Parallel()
	r1 := mkRec("a", "1", 1, TypeValue)
	r2 := mkRec("b", "2", 2, TypeValue)
	it := &errIterator{records: []Record{r1, r2}, failIdx: 1}
	mi, err := NewMergingIterator([]Iterator[Record]{it}, nil)
	assert.NoError(t, err)

	// Prepare next; this will attempt to prefetch and encounter an error,
	// but the first record should still be available.
	assert.True(t, mi.HasNext())
	rec, err := mi.Next()
	assert.NoError(t, err)
	assert.Equal(t, r1, rec)

	// The next call should surface the error set during preparation.
	_, err = mi.Next()
	assert.EqualError(t, err, "boom")
}
