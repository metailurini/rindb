//go:build integration && smoke

package rindb_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

type rangeIterDirection string

const (
	iterNext rangeIterDirection = "next"
	iterPrev rangeIterDirection = "prev"
)

type rangeIterStep struct {
	dir   rangeIterDirection
	key   string
	value string
	eoi   bool
}

func runRangeIterSequence(t *testing.T, iter *rindb.RangeIterator, steps []rangeIterStep) {
	t.Helper()
	for i, step := range steps {
		switch step.dir {
		case iterNext:
			rec, err := iter.Next()
			if step.eoi {
				require.ErrorIs(t, err, rindb.EOI, "step %d: expected EOI on Next", i)
				continue
			}
			require.NoError(t, err, "step %d: Next error", i)
			require.Equal(t, step.key, string(rec.GetKey()), "step %d: Next key mismatch", i)
			require.Equal(t, step.value, string(rec.GetValue()), "step %d: Next value mismatch", i)
		case iterPrev:
			rec, err := iter.Prev()
			if step.eoi {
				require.ErrorIs(t, err, rindb.EOI, "step %d: expected EOI on Prev", i)
				continue
			}
			require.NoError(t, err, "step %d: Prev error", i)
			require.Equal(t, step.key, string(rec.GetKey()), "step %d: Prev key mismatch", i)
			require.Equal(t, step.value, string(rec.GetValue()), "step %d: Prev value mismatch", i)
		default:
			t.Fatalf("unsupported iterator direction %q", step.dir)
		}
	}
}

func TestRangeIterator_AlternatingNextPrevAcrossLevels(t *testing.T) {
	// Limit history to ensure Prev() hits EOI after exceeding bounds.
	db, cleanup := initTestDB(t, rindb.WithMaxMemtableSize(100), rindb.WithSSTableIterMaxHistory(1))
	t.Cleanup(cleanup)
	ctx := context.Background()

	large := strings.Repeat("x", 40)

	// These entries are flushed to an SSTable.
	require.NoError(t, db.Put(ctx, rindb.Bytes("a"), rindb.Bytes(large)))
	require.NoError(t, db.Put(ctx, rindb.Bytes("b"), rindb.Bytes(large)))
	require.NoError(t, db.Put(ctx, rindb.Bytes("c"), rindb.Bytes(large)))

	// Memtable operations.
	require.NoError(t, db.Remove(ctx, rindb.Bytes("b"))) // tombstone for b
	require.NoError(t, db.Put(ctx, rindb.Bytes("d"), rindb.Bytes("1")))
	require.NoError(t, db.Put(ctx, rindb.Bytes("e"), rindb.Bytes("2")))

	iter, err := db.IRange(ctx, rindb.Bytes("a"), rindb.Bytes("z"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, iter.Close()) })

	assertNext := func(expectedKey string) {
		t.Helper()
		rec, err := iter.Next()
		require.NoError(t, err)
		require.Equal(t, expectedKey, string(rec.GetKey()))
	}
	assertPrev := func(expectedKey string) {
		t.Helper()
		rec, err := iter.Prev()
		require.NoError(t, err)
		require.Equal(t, expectedKey, string(rec.GetKey()))
	}

	assertNext("a")
	assertNext("c")

	assertPrev("c")
	assertPrev("a")

	// History exceeded: cannot go back further.
	_, err = iter.Prev()
	require.ErrorIs(t, err, rindb.EOI)

	// Rewind forward again.
	assertNext("a")
	assertNext("c")
	assertNext("d")

	assertPrev("d")
	assertNext("d")
	assertNext("e")

	assertPrev("e")
	assertPrev("d")
	assertPrev("c")
}

func TestRangeIterator_AlternatingNextPrevSequences(t *testing.T) {
	t.Run("single-key-latest-version-only", func(t *testing.T) {
		db, cleanup := initTestDB(t, rindb.WithMaxMemtableSize(256), rindb.WithSSTableIterMaxHistory(8))
		t.Cleanup(cleanup)
		ctx := context.Background()

		const key = "alpha"
		valueOld := strings.Repeat("v1", 8)
		valueMid := strings.Repeat("v2", 8)
		valueLatest := "v3"

		require.NoError(t, db.Put(ctx, rindb.Bytes(key), rindb.Bytes(valueOld)))
		require.NoError(t, db.Put(ctx, rindb.Bytes(key), rindb.Bytes(valueMid)))
		require.NoError(t, db.Put(ctx, rindb.Bytes(key), rindb.Bytes(valueLatest)))

		iter, err := db.IRange(ctx, rindb.Bytes(key), rindb.Bytes(key))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, iter.Close()) })

		runRangeIterSequence(t, iter, []rangeIterStep{
			{dir: iterNext, key: key, value: valueLatest},
			{dir: iterPrev, key: key, value: valueLatest},
			{dir: iterPrev, eoi: true},
			{dir: iterNext, key: key, value: valueLatest},
			{dir: iterNext, eoi: true},
			{dir: iterPrev, key: key, value: valueLatest},
		})
	})

	t.Run("multi-key-span", func(t *testing.T) {
		db, cleanup := initTestDB(t, rindb.WithMaxMemtableSize(300), rindb.WithSSTableIterMaxHistory(8))
		t.Cleanup(cleanup)
		ctx := context.Background()

		const (
			keyA = "a"
			keyB = "b"
			keyC = "c"
		)

		valueA1 := strings.Repeat("a1", 90)
		valueA2 := strings.Repeat("a2", 90)
		valueB1 := strings.Repeat("b1", 90)
		valueB2 := strings.Repeat("b2", 90)
		valueC1 := strings.Repeat("c1", 90)
		valueC2 := strings.Repeat("c2", 90)
		valueA3 := "a3"
		valueB3 := "b3"
		valueC3 := "c3"

		require.NoError(t, db.Put(ctx, rindb.Bytes(keyA), rindb.Bytes(valueA1)))
		require.NoError(t, db.Put(ctx, rindb.Bytes(keyA), rindb.Bytes(valueA2)))

		require.NoError(t, db.Put(ctx, rindb.Bytes(keyB), rindb.Bytes(valueB1)))
		require.NoError(t, db.Put(ctx, rindb.Bytes(keyB), rindb.Bytes(valueB2)))

		require.NoError(t, db.Put(ctx, rindb.Bytes(keyC), rindb.Bytes(valueC1)))
		require.NoError(t, db.Put(ctx, rindb.Bytes(keyC), rindb.Bytes(valueC2)))

		require.NoError(t, db.Put(ctx, rindb.Bytes(keyA), rindb.Bytes(valueA3)))
		require.NoError(t, db.Put(ctx, rindb.Bytes(keyB), rindb.Bytes(valueB3)))
		require.NoError(t, db.Put(ctx, rindb.Bytes(keyC), rindb.Bytes(valueC3)))

		iter, err := db.IRange(ctx, rindb.Bytes(keyA), rindb.Bytes("z"))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, iter.Close()) })

		runRangeIterSequence(t, iter, []rangeIterStep{
			{dir: iterNext, key: keyA, value: valueA3},
			{dir: iterNext, key: keyB, value: valueB3},
			{dir: iterPrev, key: keyB, value: valueB3},
			{dir: iterNext, key: keyB, value: valueB3},
			{dir: iterNext, key: keyC, value: valueC3},
			{dir: iterPrev, key: keyC, value: valueC3},
			{dir: iterNext, key: keyC, value: valueC3},
			{dir: iterNext, eoi: true},
			{dir: iterPrev, key: keyC, value: valueC3},
			{dir: iterNext, key: keyC, value: valueC3},
			{dir: iterNext, eoi: true},
		})
	})
}
