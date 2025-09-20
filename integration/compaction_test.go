//go:build integration

package rindb_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestCompaction_MovesSSTablesToNextLevel(t *testing.T) {
	t.Parallel()
	db, cleanup := initTestDB(t,
		rindb.WithLevel0CompactionThreshold(2),
		rindb.WithMaxMemtableSize(1),
	)
	defer cleanup()
	ctx := context.Background()

	kvs := []struct{ key, val string }{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k1", "v1_new"}, // update k1 to create multiple versions
		{"k3", "v3"},
	}
	for _, kv := range kvs {
		require.NoError(t, db.Put(ctx, rindb.Bytes(kv.key), rindb.Bytes(kv.val)))
	}

	// Compaction runs asynchronously; allow extra time for slower environments.
	require.Eventually(t, func() bool {
		st := db.Stats()
		if len(st.SSTablesPerLevel) < 2 {
			return false
		}
		return st.SSTablesPerLevel[0] == 0 && st.SSTablesPerLevel[1] > 0
	}, 10*time.Second, 100*time.Millisecond, "compaction did not move SSTables to next level")

	expectedKVs := map[string]string{
		"k1": "v1_new",
		"k2": "v2",
		"k3": "v3",
	}
	for key, val := range expectedKVs {
		got, err := db.Get(ctx, rindb.Bytes(key))
		require.NoError(t, err)
		require.Equal(t, rindb.Bytes(val), got, "key: %s", key)
	}
}
