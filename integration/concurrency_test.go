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
	ctx := context.Background()

	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			key := rindb.Bytes(fmt.Sprintf("key-%d", i))
			val := rindb.Bytes(fmt.Sprintf("value-%d", i))
			require.NoError(t, db.Put(ctx, key, val), "Put failed for i=%d", i)
			got, err := db.Get(ctx, key)
			require.NoError(t, err, "Get failed for i=%d", i)
			require.Equal(t, val, got, "value mismatch for i=%d", i)
		}()
	}

	wg.Wait()
	cleanup()
}
