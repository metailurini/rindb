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

	_, ok := cache.TryGet(ctx, k1)
	require.False(t, ok, "k1 should be evicted")
	_, ok = cache.TryGet(ctx, k2)
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

	_, ok := cache.TryGet(ctx, k1)
	require.True(t, ok, "pinned k1 should stay resident")
	_, ok = cache.TryGet(ctx, k2)
	require.False(t, ok, "unpinned k2 should be evicted")
}

func TestTableCachePinnedOverCapacity(t *testing.T) {
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

	_, ok := cache.TryGet(k2)
	require.False(t, ok, "k2 should not be cached when capacity is pinned")

	h2, err = cache.Get(ctx, k2)
	require.NoError(t, err)
	h2.Unref()
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

func TestTableCacheDelete(t *testing.T) {
	ctx := context.Background()
	var opens atomic.Int32
	var closes atomic.Int32
	cache := newTestCache(t, tableCacheOptions{
		CapBytes: 1,
		Open: func(ctx context.Context, k tableKey) (*SStable, error) {
			opens.Add(1)
			return &SStable{}, nil
		},
		Close: func(*SStable) error {
			closes.Add(1)
			return nil
		},
	})

	k := tableKey{FileNum: 1}
	h, err := cache.Get(ctx, k)
	require.NoError(t, err)
	h.Unref()

	// Entry should be resident prior to deletion.
	h2, ok := cache.TryGet(k)
	require.True(t, ok)
	h2.Unref()
	require.EqualValues(t, 0, closes.Load())

	cache.Delete(ctx, k)
	require.EqualValues(t, 1, closes.Load(), "delete should close handle when refs==0")

	_, ok = cache.TryGet(k)
	require.False(t, ok, "entry should be removed")

	_, err = cache.Get(ctx, k)
	require.ErrorIs(t, err, ErrObsolete, "tombstone should block re-admission")
	require.EqualValues(t, 1, opens.Load(), "open should not be called again")
}

func TestTableCacheClose(t *testing.T) {
	t.Run("drain", func(t *testing.T) {
		ctx := context.Background()
		var opens atomic.Int32
		var closes atomic.Int32
		cache := newTestCache(t, tableCacheOptions{
			CapBytes: 1,
			Open: func(ctx context.Context, k tableKey) (*SStable, error) {
				opens.Add(1)
				return &SStable{}, nil
			},
			Close: func(*SStable) error {
				closes.Add(1)
				return nil
			},
		})

		k1 := tableKey{FileNum: 1}
		h, err := cache.Get(ctx, k1)
		require.NoError(t, err)

		done := make(chan error)
		go func() { done <- cache.Close(ctx, 100*time.Millisecond) }()

		time.Sleep(10 * time.Millisecond) // allow Close to stop admission

		// New admissions should be rejected.
		_, err = cache.Get(ctx, tableKey{FileNum: 2})
		require.ErrorIs(t, err, ErrClosed)
		require.EqualValues(t, 1, opens.Load())
		require.EqualValues(t, 0, closes.Load())

		// Release the outstanding handle; Close should return and close it.
		h.Unref()
		require.NoError(t, <-done)
		require.EqualValues(t, 1, closes.Load())

		// Further admissions are rejected after Close.
		_, err = cache.Get(ctx, tableKey{FileNum: 3})
		require.ErrorIs(t, err, ErrClosed)
	})

	t.Run("timeout", func(t *testing.T) {
		ctx := context.Background()
		var closes atomic.Int32
		cache := newTestCache(t, tableCacheOptions{
			CapBytes: 1,
			Close: func(*SStable) error {
				closes.Add(1)
				return nil
			},
		})

		k := tableKey{FileNum: 1}
		h, err := cache.Get(ctx, k)
		require.NoError(t, err)

		err = cache.Close(ctx, 10*time.Millisecond)
		require.Error(t, err)
		require.Contains(t, err.Error(), "handles still busy")
		require.EqualValues(t, 0, closes.Load(), "handle should remain open on timeout")

		h.Unref()
		require.EqualValues(t, 1, closes.Load(), "handle should close after late Unref")
	})
}
