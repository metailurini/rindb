//go:build integration

package rindb_test

import (
	"context"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func countOpenFiles(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(entries)
}

func TestIRangeRangeQuery(t *testing.T) {
	db, cleanup := initTestDB(t, rindb.WithMaxMemtableSize(50))
	t.Cleanup(cleanup)
	ctx := context.Background()

	large1 := strings.Repeat("a", 30)
	large2 := strings.Repeat("b", 30)

	require.NoError(t, db.Put(ctx, rindb.Bytes("old"), rindb.Bytes(large1)))
	require.NoError(t, db.Put(ctx, rindb.Bytes("sst"), rindb.Bytes(large2)))

	require.NoError(t, db.Remove(ctx, rindb.Bytes("old")))

	require.NoError(t, db.Put(ctx, rindb.Bytes("mem1"), rindb.Bytes("1")))
	require.NoError(t, db.Put(ctx, rindb.Bytes("mem2"), rindb.Bytes("2")))

	filesBefore := countOpenFiles(t)

	iter, err := db.IRange(ctx, rindb.Bytes("a"), rindb.Bytes("z"))
	require.NoError(t, err)

	var keys []string
	for iter.HasNext() {
		rec, err := iter.Next()
		require.NoError(t, err)
		keys = append(keys, string(rec.GetKey()))
	}

	require.True(t, sort.StringsAreSorted(keys))
	require.Equal(t, []string{"mem1", "mem2", "sst"}, keys)
	require.NotContains(t, keys, "old")

	require.NoError(t, iter.Close())
	filesAfter := countOpenFiles(t)
	require.Less(t, filesAfter, filesBefore)
}
