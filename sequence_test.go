package rindb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test_getMaxSequenceNumber verifies that the helper returns the larger
// sequence between the manifest and WAL and provides the loaded memtable.
func Test_getMaxSequenceNumber(t *testing.T) {
	t.Run("WAL higher than manifest", func(t *testing.T) {
		ctx := context.Background()
		cfg := testConfig()
		cfg.databaseDir = t.TempDir()

		wal, err := DefaultNewWALFunc(ctx, cfg)
		require.NoError(t, err)
		defer wal.Close()

		require.NoError(t, wal.Append(ctx, newRecord(Bytes("k1"), Bytes("v1"), 1)))
		require.NoError(t, wal.Append(ctx, newRecord(Bytes("k2"), Bytes("v2"), 5)))

		vs := &versionSet{LastSequence: 3}

		seq, mem, err := getMaxSequenceNumber(ctx, vs, wal)
		require.NoError(t, err)
		require.Equal(t, uint64(5), seq)
		require.Greater(t, mem.ByteSize(), 0)
	})

	t.Run("Manifest higher than WAL", func(t *testing.T) {
		ctx := context.Background()
		cfg := testConfig()
		cfg.databaseDir = t.TempDir()

		wal, err := DefaultNewWALFunc(ctx, cfg)
		require.NoError(t, err)
		defer wal.Close()

		require.NoError(t, wal.Append(ctx, newRecord(Bytes("k1"), Bytes("v1"), 1)))

		vs := &versionSet{LastSequence: 10}

		seq, _, err := getMaxSequenceNumber(ctx, vs, wal)
		require.NoError(t, err)
		require.Equal(t, uint64(10), seq)
	})
}
