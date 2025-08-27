package rindb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot_MemtableCleanupAfterRelease(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, WithDatabaseDir(t.TempDir()), WithMaxMemtableSize(1<<20))
	defer cleanup()

	key := Bytes("k")
	assert.NoError(t, rin.Put(ctx, key, Bytes("v1")))
	snap, err := rin.NewSnapshot(ctx)
	assert.NoError(t, err)

	assert.NoError(t, rin.Put(ctx, key, Bytes("v2")))

	v, err := rin.memtable.GetAt(key, snap.Sequence())
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v1"), v)

	assert.NoError(t, rin.Release(ctx, snap))

	_, err = rin.memtable.GetAt(key, snap.Sequence())
	assert.Error(t, err)

	val, err := rin.Get(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, Bytes("v2"), val)
}

func TestSnapshot_WALSegmentsRespectSnapshots(t *testing.T) {
	ctx := context.Background()
	const maxSize = uint(128)
	rin, cleanup := initRinDBWithCleanup(t, WithDatabaseDir(t.TempDir()), WithMaxMemtableSize(maxSize))
	defer cleanup()

	assert.NoError(t, rin.Put(ctx, Bytes("k"), Bytes("v1")))
	snap, err := rin.NewSnapshot(ctx)
	assert.NoError(t, err)

	assert.NoError(t, rin.Put(ctx, Bytes("k"), Bytes("v2")))

	big := make(Bytes, maxSize)
	assert.NoError(t, rin.Put(ctx, Bytes("big"), big))

	mem, err := rin.wal.Load(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint(3), mem.data.Len())

	assert.NoError(t, rin.Release(ctx, snap))

	mem, err = rin.wal.Load(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint(1), mem.data.Len())
}

func TestSnapshot_MinSequenceUpdatesOnlyOnFirst(t *testing.T) {
	ctx := context.Background()
	rin, cleanup := initRinDBWithCleanup(t, WithDatabaseDir(t.TempDir()))
	defer cleanup()

	assert.NoError(t, rin.Put(ctx, Bytes("k1"), Bytes("v1")))
	snap1, err := rin.NewSnapshot(ctx)
	assert.NoError(t, err)
	assert.Equal(t, snap1.Sequence(), rin.ssTableManager.minSnapshotSeq)

	assert.NoError(t, rin.Put(ctx, Bytes("k2"), Bytes("v2")))
	snap2, err := rin.NewSnapshot(ctx)
	assert.NoError(t, err)
	assert.Equal(t, snap1.Sequence(), rin.ssTableManager.minSnapshotSeq)

	assert.NoError(t, rin.Release(ctx, snap1))
	assert.Equal(t, snap2.Sequence(), rin.ssTableManager.minSnapshotSeq)

	assert.NoError(t, rin.Release(ctx, snap2))
	assert.Equal(t, rin.sequenceNumber, rin.ssTableManager.minSnapshotSeq)
}
