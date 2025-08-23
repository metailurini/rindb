//go:build integration

package rindb_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestConcurrentPutGet(t *testing.T) {
	db, cleanup := initTestDB(t)
	defer cleanup()
	ctx := context.Background()

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			key := rindb.Bytes(fmt.Sprintf("key-%d", i))
			val := rindb.Bytes(fmt.Sprintf("value-%d", i))
			require.NoError(t, db.Put(ctx, key, val))
			got, err := db.Get(ctx, key)
			require.NoError(t, err)
			require.Equal(t, val, got)
		}()
	}

	wg.Wait()

	for i := 0; i < goroutines; i++ {
		key := rindb.Bytes(fmt.Sprintf("key-%d", i))
		val := rindb.Bytes(fmt.Sprintf("value-%d", i))
		got, err := db.Get(ctx, key)
		require.NoError(t, err)
		require.Equal(t, val, got)
	}
}
