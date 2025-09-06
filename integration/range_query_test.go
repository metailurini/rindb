//go:build integration && smoke

package rindb_test

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestIRangeRangeQuery(t *testing.T) {
	// Increase maxMemtableSize so some records remain unflushed.
	db, cleanup := initTestDB(t, rindb.WithMaxMemtableSize(300))
	ctx := context.Background()

	large1 := strings.Repeat("a", 30)
	large2 := strings.Repeat("b", 30)

	// These entries will be flushed to an SSTable.
	require.NoError(t, db.Put(ctx, rindb.Bytes("old"), rindb.Bytes(large1)))
	require.NoError(t, db.Put(ctx, rindb.Bytes("sst"), rindb.Bytes(large2)))
	// Write outside the query range to trigger the flush.
	require.NoError(t, db.Put(ctx, rindb.Bytes("0_trigger_flush"), rindb.Bytes("d")))

	// These operations remain in the memtable.
	require.NoError(t, db.Remove(ctx, rindb.Bytes("old")))
	require.NoError(t, db.Put(ctx, rindb.Bytes("mem1"), rindb.Bytes("1")))
	require.NoError(t, db.Put(ctx, rindb.Bytes("mem2"), rindb.Bytes("2")))

	filesBefore := countNumericEntriesInFDDirectory(t)

	iter, err := db.IRange(ctx, rindb.Bytes("a"), rindb.Bytes("z"))
	require.NoError(t, err)

	var keys []string
	for iter.HasNext() {
		rec, err := iter.Next()
		require.NoError(t, err)
		if rec.GetValue() == nil {
			continue
		}
		keys = append(keys, string(rec.GetKey()))
	}

	require.True(t, sort.StringsAreSorted(keys))
	require.Equal(t, []string{"mem1", "mem2", "sst"}, keys)
	require.NotContains(t, keys, "old")

	require.NoError(t, iter.Close())
	cleanup()
	filesAfter := countNumericEntriesInFDDirectory(t)
	require.LessOrEqual(t, filesAfter, filesBefore)
}
