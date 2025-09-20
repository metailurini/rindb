package rindb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRindb_Stats(t *testing.T) {
	t.Parallel()
	t.Run("NoFlush", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, nil)
		defer ts.Cleanup()

		st := ts.RinDB.Stats()
		assert.Zero(t, st.MemtableBytes)
		assert.Zero(t, st.SequenceNumber)
		assert.Zero(t, st.ActiveSnapshots)
		assert.Zero(t, st.WALBytes)
		assert.Zero(t, st.WALRecords)
		assert.Zero(t, st.GetCalls)
		assert.Zero(t, st.PutCalls)
		assert.Zero(t, st.RemoveCalls)
		assert.Zero(t, st.IRangeCalls)
		assert.Zero(t, st.Flushes)
		for _, c := range st.SSTablesPerLevel {
			assert.Zero(t, c)
		}

		assert.NoError(t, ts.RinDB.Put(ctx, Bytes("a"), Bytes("1")))
		_, err := ts.RinDB.Get(ctx, Bytes("a"))
		assert.NoError(t, err)
		assert.NoError(t, ts.RinDB.Remove(ctx, Bytes("a")))
		iter, err := ts.RinDB.IRange(ctx, Bytes("a"), Bytes("z"))
		assert.NoError(t, err)
		for iter.HasNext() {
			_, err = iter.Next()
			assert.NoError(t, err)
		}
		assert.NoError(t, iter.Close())

		st = ts.RinDB.Stats()
		assert.Equal(t, uint64(2), st.SequenceNumber)
		assert.Greater(t, st.MemtableBytes, 0)
		assert.Zero(t, st.ActiveSnapshots)
		assert.Equal(t, uint64(1), st.GetCalls)
		assert.Equal(t, uint64(1), st.PutCalls)
		assert.Equal(t, uint64(1), st.RemoveCalls)
		assert.Equal(t, uint64(1), st.IRangeCalls)
		assert.Equal(t, uint64(0), st.Flushes)
		assert.Equal(t, uint64(2), st.WALRecords)
		assert.Greater(t, st.WALBytes, uint64(0))
		for _, c := range st.SSTablesPerLevel {
			assert.Zero(t, c)
		}
	})

	t.Run("Flush", func(t *testing.T) {
		ctx := context.Background()
		cfg := testConfig(t)
		cfg.maxMemtableSize = 1
		ts := newTestRindbSetup(t, ctx, &cfg)
		defer ts.Cleanup()

		assert.NoError(t, ts.RinDB.Put(ctx, Bytes("a"), Bytes("1")))

		st := ts.RinDB.Stats()
		assert.Equal(t, uint64(1), st.SequenceNumber)
		assert.Equal(t, uint64(1), st.PutCalls)
		assert.Equal(t, uint64(1), st.Flushes)
		assert.Equal(t, 0, st.ActiveSnapshots)
		assert.Equal(t, 0, st.MemtableBytes)
		assert.Equal(t, uint64(0), st.WALRecords)
		assert.Equal(t, uint64(0), st.WALBytes)
		if assert.NotEmpty(t, st.SSTablesPerLevel) {
			assert.Equal(t, 1, st.SSTablesPerLevel[0])
		}
	})

	t.Run("Snapshot", func(t *testing.T) {
		ctx := context.Background()
		ts := newTestRindbSetup(t, ctx, nil)
		defer ts.Cleanup()

		st := ts.RinDB.Stats()
		assert.Zero(t, st.ActiveSnapshots)

		snap, err := ts.RinDB.NewSnapshot(ctx)
		require.NoError(t, err)

		st = ts.RinDB.Stats()
		assert.Equal(t, 1, st.ActiveSnapshots)

		require.NoError(t, snap.Release(ctx))

		st = ts.RinDB.Stats()
		assert.Zero(t, st.ActiveSnapshots)
	})
}

func TestRindb_TableCacheStats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig(t)
	cfg.cacheBytes = 1024
	cfg.cacheShards = 4
	ts := newTestRindbSetup(t, ctx, &cfg)
	defer ts.Cleanup()

	st := ts.RinDB.TableCacheStats()
	assert.Equal(t, 4, st.Shards)
	assert.Equal(t, int64(1024), st.CapBytes)
}
