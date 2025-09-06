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

func TestTableCacheTombstoneExpiry(t *testing.T) {
	ctx := context.Background()
	var opens atomic.Int32
	ttl := 10 * time.Millisecond
	cache := newTestCache(t, tableCacheOptions{
		Open: func(ctx context.Context, k tableKey) (*SStable, error) {
			opens.Add(1)
			return &SStable{}, nil
		},
		TombstoneTTL: ttl,
	})

	k := tableKey{FileNum: 1}
	h, err := cache.Get(ctx, k)
	require.NoError(t, err)
	h.Unref()

	cache.Delete(ctx, k)

	_, err = cache.Get(ctx, k)
	require.ErrorIs(t, err, ErrObsolete)
	require.EqualValues(t, 1, opens.Load())

	time.Sleep(ttl + time.Millisecond)

	h, err = cache.Get(ctx, k)
	require.NoError(t, err)
	h.Unref()
	require.EqualValues(t, 2, opens.Load())
}
