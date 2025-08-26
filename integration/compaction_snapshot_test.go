//go:build integration

package rindb_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestCompactionRespectsSnapshot(t *testing.T) {
	db, cleanup := initTestDB(t,
		rindb.WithLevel0CompactionThreshold(2),
		rindb.WithMaxMemtableSize(175),
	)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, db.Put(ctx, rindb.Bytes("k1"), rindb.Bytes("v1")))
	snap, err := db.NewSnapshot(ctx)
	require.NoError(t, err)

	// Two puts trigger a flush due to the small memtable size
	require.NoError(t, db.Put(ctx, rindb.Bytes("k1"), rindb.Bytes("v2")))
	// Second flush to create another L0 table and trigger compaction
	require.NoError(t, db.Put(ctx, rindb.Bytes("k2"), rindb.Bytes("v2")))
	require.NoError(t, db.Put(ctx, rindb.Bytes("k3"), rindb.Bytes("v3")))

	require.Eventually(t, func() bool {
		st := db.Stats()
		if len(st.SSTablesPerLevel) < 2 {
			return false
		}
		return st.SSTablesPerLevel[0] == 0 && st.SSTablesPerLevel[1] > 0
	}, 5*time.Second, 100*time.Millisecond)

	got, err := db.Get(ctx, rindb.Bytes("k1"))
	require.NoError(t, err)
	require.Equal(t, rindb.Bytes("v2"), got)

	snapVal, err := db.Get(ctx, rindb.Bytes("k1"), snap.Sequence())
	require.NoError(t, err)
	require.Equal(t, rindb.Bytes("v1"), snapVal)
}
