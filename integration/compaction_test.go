//go:build integration

package rindb_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestCompactionMovesSSTablesToNextLevel(t *testing.T) {
	db, cleanup := initTestDB(t,
		rindb.WithLevel0CompactionThreshold(2),
		rindb.WithMaxMemtableSize(1),
	)
	defer cleanup()
	ctx := context.Background()

	kvs := []struct{ key, val string }{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k4", "v4"},
	}
	for _, kv := range kvs {
		require.NoError(t, db.Put(ctx, rindb.Bytes(kv.key), rindb.Bytes(kv.val)))
	}

	require.Eventually(t, func() bool {
		st := db.Stats()
		if len(st.SSTablesPerLevel) < 2 {
			return false
		}
		return st.SSTablesPerLevel[0] == 0 && st.SSTablesPerLevel[1] > 0
	}, 5*time.Second, 100*time.Millisecond, "compaction did not move SSTables to next level")

	for _, kv := range kvs {
		got, err := db.Get(ctx, rindb.Bytes(kv.key))
		require.NoError(t, err)
		require.Equal(t, rindb.Bytes(kv.val), got)
	}
}
