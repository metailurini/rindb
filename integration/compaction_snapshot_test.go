//go:build integration

package rindb_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/metailurini/rindb"
)

func TestCompactionRespectsSnapshot(t *testing.T) {
	db, cleanup := initTestDB(t,
		rindb.WithLevel0CompactionThreshold(2),
		rindb.WithMaxMemtableSize(175),
	)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, db.Put(ctx, rindb.Bytes("k1"), rindb.Bytes("v1")))
	snap, err := db.NewSnapshot(ctx)
	require.NoError(t, err)
	st := db.Stats()
	require.Equal(t, 1, st.ActiveSnapshots)

	// Two puts trigger a flush due to the small memtable size
	require.NoError(t, db.Put(ctx, rindb.Bytes("k1"), rindb.Bytes("v2")))
	// Second flush to create another L0 table and trigger compaction
	require.NoError(t, db.Put(ctx, rindb.Bytes("k2"), rindb.Bytes("v2")))
	require.NoError(t, db.Put(ctx, rindb.Bytes("k3"), rindb.Bytes("v3")))

	require.Eventually(t, func() bool {
		st := db.Stats()
		if len(st.SSTablesPerLevel) < 2 {
			return false
		}
		return st.SSTablesPerLevel[0] == 0 && st.SSTablesPerLevel[1] > 0
	}, 5*time.Second, 100*time.Millisecond)

	got, err := db.Get(ctx, rindb.Bytes("k1"))
	require.NoError(t, err)
	require.Equal(t, rindb.Bytes("v2"), got)

	snapVal, err := snap.Get(ctx, rindb.Bytes("k1"))
	require.ErrorIs(t, err, rindb.ErrKeyNotFound)
	require.Nil(t, snapVal)

	require.NoError(t, snap.Release(ctx))
	st = db.Stats()
	require.Zero(t, st.ActiveSnapshots)
}

func TestCompactionPreservesTombstoneForSnapshot(t *testing.T) {
	db, cleanup := initTestDB(t,
		rindb.WithLevel0CompactionThreshold(1),
		rindb.WithMaxMemtableSize(200),
	)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, db.Put(ctx, rindb.Bytes("k1"), rindb.Bytes("v1")))
	snap, err := db.NewSnapshot(ctx)
	require.NoError(t, err)
	st := db.Stats()
	require.Equal(t, 1, st.ActiveSnapshots)

	require.NoError(t, db.Remove(ctx, rindb.Bytes("k1")))

	largeVal := rindb.Bytes(strings.Repeat("x", 200))
	require.NoError(t, db.Put(ctx, rindb.Bytes("k2"), largeVal))

	require.Eventually(t, func() bool {
		st := db.Stats()
		if len(st.SSTablesPerLevel) < 2 {
			return false
		}
		return st.SSTablesPerLevel[0] == 0 && st.SSTablesPerLevel[1] > 0
	}, 5*time.Second, 100*time.Millisecond)

	st = db.Stats()
	require.Equal(t, uint64(1), st.Flushes)
	require.GreaterOrEqual(t, len(st.SSTablesPerLevel), 2)
	require.Equal(t, 0, st.SSTablesPerLevel[0])
	require.Equal(t, 1, st.SSTablesPerLevel[1])

	_, err = db.Get(ctx, rindb.Bytes("k1"))
	require.ErrorIs(t, err, rindb.ErrKeyNotFound)

	snapVal, err := db.Get(ctx, rindb.Bytes("k1"), snap.Sequence())
	require.ErrorIs(t, err, rindb.ErrKeyNotFound)
	require.Nil(t, snapVal)
}
