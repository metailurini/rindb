package rindb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSemaphoreFDLimiter_ConcurrentGet(t *testing.T) {
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

func TestSemaphoreFDLimiter_Acquire(t *testing.T) {
	tests := []struct {
		name    string
		ctx     func() context.Context
		wantErr error
	}{
		{
			name:    "success",
			ctx:     func() context.Context { return context.Background() },
			wantErr: nil,
		},
		{
			name: "canceled",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			wantErr: context.Canceled,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			limiter := NewSemaphoreFDLimiter(1)
			err := limiter.Acquire(tt.ctx())
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				limiter.Release()
			}
		})
	}
}
