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
		var (
			rec rindb.Record
			err error
		)
		opName := string(step.dir)

		switch step.dir {
		case iterNext:
			rec, err = iter.Next()
		case iterPrev:
			rec, err = iter.Prev()
		default:
			t.Fatalf("unsupported iterator direction %q", step.dir)
		}

		if step.eoi {
			require.ErrorIs(t, err, rindb.EOI, "step %d: expected EOI on %s", i, opName)
			continue
		}
		require.NoError(t, err, "step %d: %s error", i, opName)
		require.Equal(t, step.key, string(rec.GetKey()), "step %d: %s key mismatch", i, opName)
		require.Equal(t, step.value, string(rec.GetValue()), "step %d: %s value mismatch", i, opName)
	}
}

func TestRangeIterator_AlternatingNextPrevAcrossLevels(t *testing.T) {
	t.Parallel()
	db, cleanup := initTestDB(t, rindb.WithMaxMemtableSize(100))
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

	// Beginning of iteration: cannot go back further.
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
	assertPrev("a")

	_, err = iter.Prev()
	require.ErrorIs(t, err, rindb.EOI)
}

func TestRangeIterator_AlternatingNextPrevSequences(t *testing.T) {
	t.Parallel()
	t.Run("single-key-latest-version-only", func(t *testing.T) {
		db, cleanup := initTestDB(t, rindb.WithMaxMemtableSize(256))
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
		db, cleanup := initTestDB(t, rindb.WithMaxMemtableSize(300))
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

	t.Run("alternating-next-prev-with-duplicate-keys", func(t *testing.T) {
		db, cleanup := initTestDB(t, rindb.WithMaxMemtableSize(256))
		t.Cleanup(cleanup)
		ctx := context.Background()

		const key = "testkey"
		valueV1 := "valueV1"
		valueV2 := "valueV2"
		valueV3 := "valueV3"

		// Put in reverse order of sequence number to simulate different versions
		// being added over time. The latest version will have the highest sequence number.
		require.NoError(t, db.Put(ctx, rindb.Bytes(key), rindb.Bytes(valueV1))) // seq 1
		require.NoError(t, db.Put(ctx, rindb.Bytes(key), rindb.Bytes(valueV2))) // seq 2
		require.NoError(t, db.Put(ctx, rindb.Bytes(key), rindb.Bytes(valueV3))) // seq 3

		iter, err := db.IRange(ctx, rindb.Bytes(key), rindb.Bytes(key))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, iter.Close()) })

		runRangeIterSequence(t, iter, []rangeIterStep{
			{dir: iterNext, key: key, value: valueV3}, // Current: V3
			{dir: iterPrev, key: key, value: valueV3}, // Should return V3 (last yielded)
			{dir: iterNext, key: key, value: valueV3}, // Current: V3
			{dir: iterNext, eoi: true},                // No more next
			{dir: iterPrev, key: key, value: valueV3}, // Should return V3
			{dir: iterPrev, eoi: true},                // No more prev
		})

		// Re-initialize iterator to test a different sequence
		iter, err = db.IRange(ctx, rindb.Bytes(key), rindb.Bytes(key))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, iter.Close()) })

		runRangeIterSequence(t, iter, []rangeIterStep{
			{dir: iterNext, key: key, value: valueV3}, // Current: V3
			{dir: iterNext, eoi: true},                // No more next
			{dir: iterPrev, key: key, value: valueV3}, // Should return V3
			{dir: iterPrev, eoi: true},                // No more prev
		})

		// Re-initialize iterator to test the problematic sequence
		iter, err = db.IRange(ctx, rindb.Bytes(key), rindb.Bytes(key))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, iter.Close()) })

		runRangeIterSequence(t, iter, []rangeIterStep{
			{dir: iterNext, key: key, value: valueV3}, // Yields V3. rev: [V3]
			{dir: iterNext, eoi: true},                // No more next.
			{dir: iterPrev, key: key, value: valueV3}, // Prev should return V3. rev: []
			{dir: iterPrev, eoi: true},                // No more prev.
		})
	})
}
