package rindb

import (
	"context"
	"runtime"
	"sync"
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

	require.True(t, cache.TryRef(ctx, key))
	st := cache.Stats()
	require.EqualValues(t, 1, st.Misses)
	require.EqualValues(t, 0, st.Hits)

	h, err = cache.Get(ctx, key)
	require.NoError(t, err)
	h.Unref()

	require.EqualValues(t, 1, opens.Load())
	st = cache.Stats()
	require.EqualValues(t, 1, st.Misses)
	require.EqualValues(t, 1, st.Hits)
}

func TestTableCacheTryGetStats(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, tableCacheOptions{})

	k1 := tableKey{FileNum: 1}
	h, err := cache.Get(ctx, k1)
	require.NoError(t, err)
	h.Unref()

	st := cache.Stats()
	require.EqualValues(t, 1, st.Misses)
	require.EqualValues(t, 0, st.Hits)

	h, ok := cache.TryGet(ctx, k1)
	require.True(t, ok)
	h.Unref()

	st = cache.Stats()
	require.EqualValues(t, 1, st.Misses)
	require.EqualValues(t, 1, st.Hits)

	_, ok = cache.TryGet(ctx, tableKey{FileNum: 2})
	require.False(t, ok)

	st = cache.Stats()
	require.EqualValues(t, 2, st.Misses)
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

func TestTableCacheGetCanceledWhileSingleflight(t *testing.T) {
	ctx := context.Background()
	openStart := make(chan struct{})
	release := make(chan struct{})
	var opens atomic.Int32
	cache := newTestCache(t, tableCacheOptions{
		Open: func(ctx context.Context, k tableKey) (*SStable, error) {
			opens.Add(1)
			close(openStart)
			<-release
			return &SStable{}, nil
		},
	})

	key := tableKey{FileNum: 1}

	// Hold the singleflight with the first call.
	done := make(chan struct{})
	go func() {
		h, err := cache.Get(ctx, key)
		if err == nil {
			h.Unref()
		}
		close(done)
	}()

	<-openStart // ensure first open started and is blocking

	ctx2, cancel := context.WithCancel(ctx)
	errCh := make(chan error)
	go func() {
		_, err := cache.Get(ctx2, key)
		errCh <- err
	}()

	time.Sleep(10 * time.Millisecond) // let second call join the flight
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Get did not return after context cancellation")
	}

	// Release first call and wait for it to finish.
	close(release)
	<-done

	require.EqualValues(t, 1, opens.Load())
}

func TestTableCacheCloseDrainsBusyEntries(t *testing.T) {
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

	h, ok := cache.TryGet(ctx, k1)
	require.True(t, ok)
	h.Unref()
	h, ok = cache.TryGet(ctx, k2)
	require.True(t, ok)
	h.Unref()
	_, ok = cache.TryGet(ctx, k3)
	require.False(t, ok)

	st := cache.Stats()
	require.EqualValues(t, 2, st.Hits)
	require.EqualValues(t, 4, st.Misses)
	require.EqualValues(t, 1, st.Evicts)
	require.EqualValues(t, 1, st.Closes)
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
	h2, ok := cache.TryGet(ctx, k)
	require.True(t, ok)
	h2.Unref()
	require.EqualValues(t, 0, closes.Load())

	cache.Delete(ctx, k)
	require.EqualValues(t, 1, closes.Load(), "delete should close handle when refs==0")

	_, ok = cache.TryGet(ctx, k)
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

		// New admissions should be rejected.
		// Poll until cache.Close() has started and rejects new admissions, which is more robust than a fixed sleep.
		require.Eventually(t, func() bool {
			if !cache.shards[0].stopAdmission.Load() {
				return false
			}
			_, err := cache.Get(ctx, tableKey{FileNum: 2})
			return err == ErrClosed
		}, 50*time.Millisecond, 5*time.Millisecond)
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
		require.Contains(t, err.Error(), "entries still busy")
		require.EqualValues(t, 0, closes.Load(), "entry should remain open on timeout")

		h.Unref()
		require.EqualValues(t, 1, closes.Load(), "entry should close after late Unref")
	})
}

func TestTableCacheCloseUnrefRace(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, tableCacheOptions{CapBytes: 1})

	k := tableKey{FileNum: 1}
	h, err := cache.Get(ctx, k)
	require.NoError(t, err)

	startG := runtime.NumGoroutine()
	var unrefWG sync.WaitGroup
	unrefWG.Add(1)
	go func() {
		defer unrefWG.Done()
		time.Sleep(10 * time.Millisecond)
		h.Unref()
	}()

	start := time.Now()
	err = cache.Close(ctx, time.Second)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Less(t, elapsed, time.Second)

	unrefWG.Wait()

	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= startG+1
	}, time.Second, 10*time.Millisecond)
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

func TestTableCacheDeleteClearsCorruptQuarantine(t *testing.T) {
	ctx := context.Background()
	var opens atomic.Int32
	var verifies atomic.Int32
	ttl := 100 * time.Millisecond
	cache := newTestCache(t, tableCacheOptions{
		Open: func(ctx context.Context, k tableKey) (*SStable, error) {
			opens.Add(1)
			return &SStable{}, nil
		},
		Verify: func(*SStable) error {
			if verifies.Add(1) == 1 {
				return ErrCorruption
			}
			return nil
		},
		CorruptTTL:   time.Hour,
		TombstoneTTL: ttl,
	})

	k := tableKey{FileNum: 1}
	_, err := cache.Get(ctx, k)
	require.ErrorIs(t, err, ErrCorruption)
	require.EqualValues(t, 1, opens.Load())

	cache.Delete(ctx, k)

	// Admission is blocked by tombstone.
	_, err = cache.Get(ctx, k)
	require.ErrorIs(t, err, ErrObsolete)
	require.EqualValues(t, 1, opens.Load())

	time.Sleep(ttl + time.Millisecond)

	h, err := cache.Get(ctx, k)
	require.NoError(t, err)
	h.Unref()
	require.EqualValues(t, 2, opens.Load())
}

func TestTableCacheMeasureAndByteAccounting(t *testing.T) {
	ctx := context.Background()
	actuals := []int64{0, 5, -7}
	var idx atomic.Int32
	cache := newTestCache(t, tableCacheOptions{
		CapBytes: 20,
		Measure: func(*SStable) (int64, int64) {
			i := int(idx.Add(1) - 1)
			return 0, actuals[i]
		},
	})

	for i := 1; i <= len(actuals); i++ {
		k := tableKey{FileNum: uint64(i)}
		h, err := cache.Get(ctx, k)
		require.NoError(t, err)
		h.Unref()
	}

	st := cache.Stats()
	require.EqualValues(t, 7, st.UsedBytes)
	require.EqualValues(t, 7, cache.shards[0].usedBytes)
	require.EqualValues(t, 7, cache.totalBytes.Load())
}

func TestTableCacheDeleteUpdatesByteAccounting(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, tableCacheOptions{CapBytes: 10})

	keys := []tableKey{{FileNum: 1}, {FileNum: 2}}
	for _, k := range keys {
		h, err := cache.Get(ctx, k)
		require.NoError(t, err)
		h.Unref()
	}

	st := cache.Stats()
	require.EqualValues(t, 2, st.UsedBytes)
	require.EqualValues(t, 2, cache.shards[0].usedBytes)
	require.EqualValues(t, 2, cache.totalBytes.Load())

	cache.Delete(ctx, keys[0])

	st = cache.Stats()
	require.EqualValues(t, 1, st.UsedBytes)
	require.EqualValues(t, 1, cache.shards[0].usedBytes)
	require.EqualValues(t, 1, cache.totalBytes.Load())
}

func TestTableCacheEvictOversizedEntry(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, tableCacheOptions{
		CapBytes: 5,
		Measure:  func(*SStable) (int64, int64) { return 0, 7 },
	})

	k := tableKey{FileNum: 1}
	h, err := cache.Get(ctx, k)
	require.NoError(t, err)
	h.Unref()

	st := cache.Stats()
	require.EqualValues(t, 0, st.UsedBytes)
	require.EqualValues(t, 0, cache.shards[0].usedBytes)
	require.EqualValues(t, 0, cache.totalBytes.Load())
	require.EqualValues(t, 1, st.Evicts)
	require.EqualValues(t, 1, st.Closes)

	_, ok := cache.TryGet(ctx, k)
	require.False(t, ok)
}

func TestTableCacheMultiShardByteAccounting(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, tableCacheOptions{CapBytes: 8, Shards: 4})

	var keys []tableKey
	seen := make(map[*shard]struct{})
	for i := 1; len(keys) < len(cache.shards); i++ {
		k := tableKey{FileNum: uint64(i)}
		s := cache.shardFor(k)
		if _, ok := seen[s]; ok {
			continue
		}
		h, err := cache.Get(ctx, k)
		require.NoError(t, err)
		h.Unref()
		keys = append(keys, k)
		seen[s] = struct{}{}
	}

	st := cache.Stats()
	require.Equal(t, len(cache.shards), int(st.Shards))
	require.EqualValues(t, 4, st.Shards)
	require.EqualValues(t, 8, st.CapBytes)
	require.EqualValues(t, 4, st.UsedBytes)
	for _, s := range cache.shards {
		require.EqualValues(t, 2, s.capBytes)
		require.EqualValues(t, 1, s.usedBytes)
	}
	require.EqualValues(t, 4, cache.totalBytes.Load())
}

func TestTableCachePinnedByteAccounting(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, tableCacheOptions{CapBytes: 1})

	k1 := tableKey{FileNum: 1}
	h, err := cache.Get(ctx, k1)
	require.NoError(t, err)
	h.Unref()
	require.True(t, cache.PinKey(k1))

	st := cache.Stats()
	require.EqualValues(t, 1, st.UsedBytes)
	require.EqualValues(t, 1, cache.shards[0].usedBytes)
	require.EqualValues(t, 1, cache.totalBytes.Load())

	k2 := tableKey{FileNum: 2}
	h, err = cache.Get(ctx, k2)
	require.NoError(t, err)
	h.Unref()

	st = cache.Stats()
	require.EqualValues(t, 1, st.UsedBytes)
	_, ok := cache.TryGet(ctx, k2)
	require.False(t, ok)

	cache.UnpinKey(ctx, k1)
	h, err = cache.Get(ctx, k2)
	require.NoError(t, err)
	h.Unref()

	st = cache.Stats()
	require.EqualValues(t, 1, st.UsedBytes)
	_, ok = cache.TryGet(ctx, k1)
	require.False(t, ok)
	_, ok = cache.TryGet(ctx, k2)
	require.True(t, ok)
}

func TestTableCacheUnpinTriggersEviction(t *testing.T) {
	ctx := context.Background()
	cache := newTestCache(t, tableCacheOptions{CapBytes: 2})

	k1 := tableKey{FileNum: 1}
	h, err := cache.Get(ctx, k1)
	require.NoError(t, err)
	h.Unref()
	require.True(t, cache.PinKey(k1))

	k2 := tableKey{FileNum: 2}
	h, err = cache.Get(ctx, k2)
	require.NoError(t, err)
	h.Unref()
	require.True(t, cache.PinKey(k2))

	// Drop the capacity so the shard exceeds its budget while entries are pinned.
	cache.shards[0].capBytes = 1

	st := cache.Stats()
	require.EqualValues(t, 2, st.UsedBytes)

	cache.UnpinKey(ctx, k1)

	st = cache.Stats()
	require.EqualValues(t, 1, st.UsedBytes)

	_, ok := cache.TryGet(ctx, k1)
	require.False(t, ok, "unpinned key should be evicted")

	_, ok = cache.TryGet(ctx, k2)
	require.True(t, ok, "still pinned key stays")
}
