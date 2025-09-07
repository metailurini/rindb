package rindb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSemaphoreFDLimiterConcurrentGet(t *testing.T) {
	ctx := context.Background()
	limiter := NewSemaphoreFDLimiter(1)

	var current atomic.Int32
	var max atomic.Int32

	cache := newTestCache(t, tableCacheOptions{
		FDLimiter: limiter,
		Open: func(ctx context.Context, k tableKey) (*SStable, error) {
			c := current.Add(1)
			for {
				old := max.Load()
				if c <= old || max.CompareAndSwap(old, c) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			current.Add(-1)
			return &SStable{}, nil
		},
	})

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h, err := cache.get(ctx, tableKey{FileNum: uint64(i)})
			require.NoError(t, err)
			h.unref()
		}(i)
	}
	wg.Wait()

	require.EqualValues(t, 1, max.Load(), "concurrent opens should be limited to 1")
}

func TestSemaphoreFDLimiterAcquireCancelled(t *testing.T) {
	limiter := NewSemaphoreFDLimiter(1)

	require.NoError(t, limiter.Acquire(context.Background()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := limiter.Acquire(ctx)
	require.ErrorIs(t, err, context.Canceled)

	limiter.Release()
}
