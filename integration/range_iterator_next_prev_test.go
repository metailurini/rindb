//go:build integration && smoke

package rindb_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

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
