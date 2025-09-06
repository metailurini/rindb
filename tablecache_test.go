package rindb

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestCache(t *testing.T, opt tableCacheOptions) *tableCache {
	t.Helper()
	if opt.Open == nil {
		opt.Open = func(ctx context.Context, k tableKey) (*SStable, error) { return &SStable{}, nil }
	}
	if opt.Close == nil {
		opt.Close = func(*SStable) error { return nil }
	}
	if opt.Measure == nil {
		opt.Measure = func(*SStable) (int64, int64) { return 0, 1 }
	}
	if opt.Shards == 0 {
		opt.Shards = 1
	}
	return newTableCache(opt)
}

func TestTableCacheHitMiss(t *testing.T) {
	ctx := context.Background()
	var opens atomic.Int32
	cache := newTestCache(t, tableCacheOptions{
		Open: func(ctx context.Context, k tableKey) (*SStable, error) {
			opens.Add(1)
			return &SStable{}, nil
		},
	})

	// tests use default DBID 0; override when multi-DB is supported
	key := tableKey{FileNum: 1}
	h, err := cache.Get(ctx, key)
	require.NoError(t, err)
	h.Unref()

	h, err = cache.Get(ctx, key)
	require.NoError(t, err)
	h.Unref()

	require.EqualValues(t, 1, opens.Load())
	st := cache.Stats()
	require.EqualValues(t, 1, st.Misses)
	require.EqualValues(t, 1, st.Hits)
}

func TestTableCacheEviction(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, tableCacheOptions{CapBytes: 1})

	k1 := tableKey{FileNum: 1}
	h1, err := cache.Get(ctx, k1)
	require.NoError(t, err)
	h1.Unref()

	k2 := tableKey{FileNum: 2}
	h2, err := cache.Get(ctx, k2)
	require.NoError(t, err)
	h2.Unref()

	_, ok := cache.TryGet(k1)
	require.False(t, ok, "k1 should be evicted")
	_, ok = cache.TryGet(k2)
	require.True(t, ok, "k2 should remain in cache")
}

func TestTableCachePinning(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, tableCacheOptions{CapBytes: 1})

	k1 := tableKey{FileNum: 1}
	h1, err := cache.Get(ctx, k1)
	require.NoError(t, err)
	h1.Unref()
	require.True(t, cache.PinKey(k1))

	k2 := tableKey{FileNum: 2}
	h2, err := cache.Get(ctx, k2)
	require.NoError(t, err)
	h2.Unref()

	_, ok := cache.TryGet(k1)
	require.True(t, ok, "pinned k1 should stay resident")
	_, ok = cache.TryGet(k2)
	require.False(t, ok, "unpinned k2 should be evicted")
}

func TestTableCacheCorruptionQuarantine(t *testing.T) {
	ctx := context.Background()
	var opens atomic.Int32
	cache := newTestCache(t, tableCacheOptions{
		Open: func(ctx context.Context, k tableKey) (*SStable, error) {
			opens.Add(1)
			return &SStable{}, nil
		},
		Verify:     func(*SStable) error { return ErrCorruption },
		CorruptTTL: time.Minute,
	})

	k := tableKey{FileNum: 1}
	_, err := cache.Get(ctx, k)
	require.ErrorIs(t, err, ErrCorruption)
	require.EqualValues(t, 1, opens.Load())

	_, err = cache.Get(ctx, k)
	require.ErrorIs(t, err, ErrCorruption)
	require.EqualValues(t, 1, opens.Load(), "should not reopen during quarantine")
}

func TestTableCacheCloseDrainsBusyHandles(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, tableCacheOptions{CapBytes: 2})

	k1 := tableKey{FileNum: 1}
	h1a, err := cache.Get(ctx, k1)
	require.NoError(t, err)
	h1b, err := cache.Get(ctx, k1)
	require.NoError(t, err)
	h1b.Unref()

	k2 := tableKey{FileNum: 2}
	h2, err := cache.Get(ctx, k2)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		time.Sleep(100 * time.Millisecond)
		h1a.Unref()
		h2.Unref()
		close(done)
	}()

	start := time.Now()
	err = cache.Close(ctx, 500*time.Millisecond)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
	require.Less(t, elapsed, 500*time.Millisecond)
	<-done

	st := cache.Stats()
	require.EqualValues(t, 1, st.Hits)
	require.EqualValues(t, 2, st.Misses)
	require.EqualValues(t, 0, st.Evicts)
	require.EqualValues(t, 2, st.Closes)
}

func TestTableCachePinnedOverCapacity(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, tableCacheOptions{CapBytes: 2})

	k1 := tableKey{FileNum: 1}
	h1, err := cache.Get(ctx, k1)
	require.NoError(t, err)
	h1.Unref()
	require.True(t, cache.PinKey(k1))

	k2 := tableKey{FileNum: 2}
	h2, err := cache.Get(ctx, k2)
	require.NoError(t, err)
	h2.Unref()
	require.True(t, cache.PinKey(k2))

	k3 := tableKey{FileNum: 3}
	h3, err := cache.Get(ctx, k3)
	require.NoError(t, err)
	h3.Unref()

	h, ok := cache.TryGet(k1)
	require.True(t, ok)
	h.Unref()
	h, ok = cache.TryGet(k2)
	require.True(t, ok)
	h.Unref()
	_, ok = cache.TryGet(k3)
	require.False(t, ok)

	st := cache.Stats()
	require.EqualValues(t, 2, st.Hits)
	require.EqualValues(t, 3, st.Misses)
	require.EqualValues(t, 1, st.Evicts)
	require.EqualValues(t, 1, st.Closes)
}
