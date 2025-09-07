//go:build integration

package rindb_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestTableCacheMissAfterGetFollowingCompaction(t *testing.T) {
	db, cleanup := initTestDB(t,
		rindb.WithLevel0CompactionThreshold(2),
		rindb.WithMaxMemtableSize(1),
	)
	defer cleanup()
	ctx := context.Background()

	kvs := []struct{ key, val string }{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k1", "v1_new"},
		{"k3", "v3"},
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
	}, 5*time.Second, 100*time.Millisecond)

	v, err := db.Get(ctx, rindb.Bytes("k1"))
	require.NoError(t, err)
	require.Equal(t, rindb.Bytes("v1_new"), v)
	mid := db.TableCacheStats()

	v, err = db.Get(ctx, rindb.Bytes("k1"))
	require.NoError(t, err)
	require.Equal(t, rindb.Bytes("v1_new"), v)

	after := db.TableCacheStats()
	require.Equal(t, mid.Hits+1, after.Hits)
}
