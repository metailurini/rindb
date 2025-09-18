package rindb

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot_MemtableCleanupAfterRelease(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, WithDatabaseDir(t.TempDir()), WithMaxMemtableSize(1<<20))
	defer cleanup()

	key := Bytes("k")
	require.NoError(t, rin.Put(ctx, key, Bytes("v1")))
	snap, err := rin.NewSnapshot(ctx)
	require.NoError(t, err)

	require.NoError(t, rin.Put(ctx, key, Bytes("v2")))

	v, err := rin.Memtable.GetAt(key, snap.Sequence())
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v1"), v)

	assert.NoError(t, snap.Release(ctx))

	_, err = rin.Memtable.GetAt(key, snap.Sequence())
	assert.Error(t, err)

	val, err := rin.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v2"), val)
}

func TestSnapshot_CleanupRetainsSnapshotVisibleVersion(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, WithDatabaseDir(t.TempDir()), WithMaxMemtableSize(1<<20))
	defer cleanup()

	key := Bytes("primary")
	require.NoError(t, rin.Put(ctx, key, Bytes("v1")))

	snap, err := rin.NewSnapshot(ctx)
	require.NoError(t, err)

	require.NoError(t, rin.Put(ctx, key, Bytes("v2")))

	newerSnap, err := rin.NewSnapshot(ctx)
	require.NoError(t, err)

	require.NoError(t, newerSnap.Release(ctx))

	current, err := rin.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, Bytes("v2"), current)

	snapVal, err := snap.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, Bytes("v1"), snapVal)

	iter, err := snap.IRange(ctx, key, key)
	require.NoError(t, err)
	require.True(t, iter.HasNext(), "expected snapshot-visible version to remain after cleanup")
	rec, err := iter.Next()
	require.NoError(t, err)
	assert.Equal(t, Bytes("v1"), rec.GetValue())
	require.NoError(t, iter.Close())

	require.NoError(t, snap.Release(ctx))
}

func TestSnapshot_WALSegmentsRespectSnapshots(t *testing.T) {
	ctx := context.Background()
	const maxSize = uint(128)
	rin, cleanup := initRinDBWithCleanup(t, WithDatabaseDir(t.TempDir()), WithMaxMemtableSize(maxSize))
	defer cleanup()

	require.NoError(t, rin.Put(ctx, Bytes("k"), Bytes("v1")))
	snap, err := rin.NewSnapshot(ctx)
	require.NoError(t, err)

	require.NoError(t, rin.Put(ctx, Bytes("k"), Bytes("v2")))

	big := make(Bytes, maxSize)
	require.NoError(t, rin.Put(ctx, Bytes("big"), big))

	mem, err := rin.WAL.Load(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint(3), mem.data.Len())

	assert.NoError(t, snap.Release(ctx))

	mem, err = rin.WAL.Load(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint(1), mem.data.Len())
}

func TestSnapshot_MinSequenceUpdatesOnlyOnFirst(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, WithDatabaseDir(t.TempDir()))
	defer cleanup()

	require.NoError(t, rin.Put(ctx, Bytes("k1"), Bytes("v1")))
	snap1, err := rin.NewSnapshot(ctx)
	require.NoError(t, err)
	assert.Equal(t, snap1.Sequence(), rin.SSTableManager.minSnapshotSeq)

	require.NoError(t, rin.Put(ctx, Bytes("k2"), Bytes("v2")))
	snap2, err := rin.NewSnapshot(ctx)
	require.NoError(t, err)
	assert.Equal(t, snap1.Sequence(), rin.SSTableManager.minSnapshotSeq)

	assert.NoError(t, snap1.Release(ctx))
	assert.Equal(t, snap2.Sequence(), rin.SSTableManager.minSnapshotSeq)

	assert.NoError(t, snap2.Release(ctx))
	assert.Equal(t, uint64(math.MaxUint64), rin.SSTableManager.minSnapshotSeq)
}

func TestSnapshot_TombstoneRemovedAfterRelease(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, WithDatabaseDir(t.TempDir()), WithMaxMemtableSize(200), WithLevel0CompactionThreshold(1))
	defer cleanup()

	require.NoError(t, rin.Put(ctx, Bytes("k"), Bytes("v1")))
	snap, err := rin.NewSnapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, rin.Remove(ctx, Bytes("k")))
	large := Bytes(strings.Repeat("x", 200))
	require.NoError(t, rin.Put(ctx, Bytes("pad"), large))

	require.Eventually(t, func() bool {
		st := rin.Stats()
		return len(st.SSTablesPerLevel) >= 2 && st.SSTablesPerLevel[0] == 0 && st.SSTablesPerLevel[1] > 0
	}, 5*time.Second, 100*time.Millisecond)

	require.NoError(t, snap.Release(ctx))
	require.NoError(t, rin.Put(ctx, Bytes("k2"), large))
	require.NoError(t, rin.Put(ctx, Bytes("k3"), large))
	require.Eventually(t, func() bool {
		ssts, err := rin.SSTableManager.GetRelevantSSTables(ctx, Bytes("k"), Bytes("k"))
		if err != nil {
			return false
		}
		for _, h := range ssts {
			h.unref()
		}
		return len(ssts) == 0
	}, 5*time.Second, 100*time.Millisecond)
}

func TestRindb_Snapshot(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, WithDatabaseDir(t.TempDir()))
	defer cleanup()

	key := Bytes("key")
	assert.NoError(t, rin.Put(ctx, key, Bytes("v1")))
	snap, err := rin.NewSnapshot(ctx)
	assert.NoError(t, err)

	assert.NoError(t, rin.Put(ctx, key, Bytes("v2")))

	val, err := snap.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v1"), val)

	assert.NoError(t, snap.Release(ctx))
	rin.mu.RLock()
	assert.Len(t, rin.activeSnapshots, 0)
	rin.mu.RUnlock()
}

func TestSnapshot_IRange(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, WithDatabaseDir(t.TempDir()))
	defer cleanup()

	assert.NoError(t, rin.Put(ctx, Bytes("a"), Bytes("v1")))
	assert.NoError(t, rin.Put(ctx, Bytes("b"), Bytes("v2")))
	snap, err := rin.NewSnapshot(ctx)
	assert.NoError(t, err)

	assert.NoError(t, rin.Put(ctx, Bytes("a"), Bytes("v3")))
	assert.NoError(t, rin.Remove(ctx, Bytes("b")))

	iter, err := snap.IRange(ctx, Bytes("a"), Bytes("z"))
	assert.NoError(t, err)
	expected := []Record{
		newRecord(Bytes("a"), Bytes("v1"), 1),
		newRecord(Bytes("b"), Bytes("v2"), 2),
	}
	assertIteratorRecords(t, iter, expected)

	assert.NoError(t, snap.Release(ctx))
}

func TestSnapshot_ReleaseConcurrent(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, WithDatabaseDir(t.TempDir()))
	defer cleanup()

	snap, err := rin.NewSnapshot(ctx)
	require.NoError(t, err)

	var wg sync.WaitGroup
	const goroutines = 10
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, snap.Release(ctx))
		}()
	}
	wg.Wait()

	rin.mu.RLock()
	assert.Len(t, rin.activeSnapshots, 0)
	rin.mu.RUnlock()
}

func BenchmarkRindbRelease(b *testing.B) {
	ctx := context.Background()
	rin, err := InitRinDB(ctx, WithDatabaseDir(b.TempDir()), WithMaxMemtableSize(1<<20))
	require.NoError(b, err)
	defer func() { _ = rin.Close() }()

	const snapshots = 10000
	snaps := make([]*Snapshot, snapshots)
	for i := 0; i < snapshots; i++ {
		require.NoError(b, rin.Put(ctx, Bytes(fmt.Sprintf("k-%d", i)), Bytes("v")))
		snap, err := rin.NewSnapshot(ctx)
		require.NoError(b, err)
		snaps[i] = snap
	}

	next := snapshots
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % snapshots
		require.NoError(b, snaps[idx].Release(ctx))
		require.NoError(b, rin.Put(ctx, Bytes(fmt.Sprintf("k-%d", next)), Bytes("v")))
		next++
		newSnap, err := rin.NewSnapshot(ctx)
		require.NoError(b, err)
		snaps[idx] = newSnap
	}
}
