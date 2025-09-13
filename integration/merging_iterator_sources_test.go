//go:build integration && smoke

package rindb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestIRangeAlternatingAcrossMemtableAndSSTable(t *testing.T) {
	db, cleanup := initTestDB(t, rindb.WithMaxMemtableSize(175))
	t.Cleanup(cleanup)
	ctx := context.Background()

	// Initial records that get flushed to an SSTable.
	require.NoError(t, db.Put(ctx, rindb.Bytes("a"), rindb.Bytes("va")))
	require.NoError(t, db.Put(ctx, rindb.Bytes("c"), rindb.Bytes("vc")))
	require.NoError(t, db.Put(ctx, rindb.Bytes("e"), rindb.Bytes("ve")))

	// Records that remain in the memtable.
	require.NoError(t, db.Put(ctx, rindb.Bytes("b"), rindb.Bytes("vb")))
	require.NoError(t, db.Put(ctx, rindb.Bytes("d"), rindb.Bytes("vd")))
	require.NoError(t, db.Put(ctx, rindb.Bytes("f"), rindb.Bytes("vf")))

	iter, err := db.IRange(ctx, rindb.Bytes("a"), rindb.Bytes("z"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, iter.Close()) })

	type op struct {
		next bool
		want string
	}
	ops := []op{
		{true, "a"},
		{true, "b"},
		{false, "b"},
		{true, "b"},
		{true, "c"},
		{false, "c"},
		{true, "c"},
		{true, "d"},
		{false, "d"},
		{true, "d"},
		{true, "e"},
		{false, "e"},
		{true, "e"},
		{true, "f"},
	}

	for i, op := range ops {
		var rec rindb.Record
		if op.next {
			rec, err = iter.Next()
		} else {
			rec, err = iter.Prev()
		}
		require.NoError(t, err, "step %d", i)
		require.Equal(t, op.want, string(rec.GetKey()), "step %d", i)
	}

	_, err = iter.Next()
	require.ErrorIs(t, err, rindb.EOI)

	rec, err := iter.Prev()
	require.NoError(t, err)
	require.Equal(t, "f", string(rec.GetKey()))

	rec, err = iter.Prev()
	require.NoError(t, err)
	require.Equal(t, "e", string(rec.GetKey()))
}

func TestIRangeExhaustsSources(t *testing.T) {
	db, cleanup := initTestDB(t, rindb.WithMaxMemtableSize(175))
	t.Cleanup(cleanup)
	ctx := context.Background()

	// Records flushed to an SSTable.
	require.NoError(t, db.Put(ctx, rindb.Bytes("c"), rindb.Bytes("vc")))
	require.NoError(t, db.Put(ctx, rindb.Bytes("d"), rindb.Bytes("vd")))
	require.NoError(t, db.Put(ctx, rindb.Bytes("e"), rindb.Bytes("ve")))

	// Records that remain in the memtable (lower keys).
	require.NoError(t, db.Put(ctx, rindb.Bytes("a"), rindb.Bytes("va")))
	require.NoError(t, db.Put(ctx, rindb.Bytes("b"), rindb.Bytes("vb")))

	iter, err := db.IRange(ctx, rindb.Bytes("a"), rindb.Bytes("z"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, iter.Close()) })

	// Exhaust the memtable first.
	rec, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, "a", string(rec.GetKey()))

	rec, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, "b", string(rec.GetKey()))

	// Now iterate over SSTable records after the memtable is exhausted.
	rec, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, "c", string(rec.GetKey()))

	rec, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, "d", string(rec.GetKey()))

	rec, err = iter.Next()
	require.NoError(t, err)
	require.Equal(t, "e", string(rec.GetKey()))

	_, err = iter.Next()
	require.ErrorIs(t, err, rindb.EOI)

	rec, err = iter.Prev()
	require.NoError(t, err)
	require.Equal(t, "e", string(rec.GetKey()))

	rec, err = iter.Prev()
	require.NoError(t, err)
	require.Equal(t, "d", string(rec.GetKey()))

	rec, err = iter.Prev()
	require.NoError(t, err)
	require.Equal(t, "c", string(rec.GetKey()))

	rec, err = iter.Prev()
	require.NoError(t, err)
	require.Equal(t, "b", string(rec.GetKey()))

	rec, err = iter.Prev()
	require.NoError(t, err)
	require.Equal(t, "a", string(rec.GetKey()))

	_, err = iter.Prev()
	require.ErrorIs(t, err, rindb.EOI)
}
