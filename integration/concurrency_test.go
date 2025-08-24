//go:build integration

package rindb_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestConcurrentPutGet(t *testing.T) {
	db, cleanup := initTestDB(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	const goroutines = 50

	for i := 0; i < goroutines; i++ {
		i := i
		t.Run(fmt.Sprintf("put-get-%d", i), func(t *testing.T) {
			t.Parallel()
			key := rindb.Bytes(fmt.Sprintf("key-%d", i))
			val := rindb.Bytes(fmt.Sprintf("value-%d", i))
			require.NoError(t, db.Put(ctx, key, val))
			got, err := db.Get(ctx, key)
			require.NoError(t, err)
			require.Equal(t, val, got)
		})
	}
}
