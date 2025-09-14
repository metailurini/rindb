package rindb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSequence_GetMaxSequenceNumber verifies that the helper returns the
// larger sequence between the manifest and WAL and provides the loaded memtable.
func TestSequence_GetMaxSequenceNumber(t *testing.T) {
	tests := []struct {
		name            string
		walRecords      []Record
		lastManifestSeq uint64
		wantSeq         uint64
		wantMemLoaded   bool
	}{
		{
			name: "WAL higher than manifest",
			walRecords: []Record{
				newRecord(Bytes("k1"), Bytes("v1"), 1),
				newRecord(Bytes("k2"), Bytes("v2"), 5),
			},
			lastManifestSeq: 3,
			wantSeq:         5,
			wantMemLoaded:   true,
		},
		{
			name: "Manifest higher than WAL",
			walRecords: []Record{
				newRecord(Bytes("k1"), Bytes("v1"), 1),
			},
			lastManifestSeq: 10,
			wantSeq:         10,
			wantMemLoaded:   false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := testConfig()
			cfg.databaseDir = t.TempDir()

			wal, err := DefaultNewWALFunc(ctx, cfg)
			require.NoError(t, err)
			defer wal.Close()

			for _, rec := range tt.walRecords {
				require.NoError(t, wal.Append(ctx, rec))
			}

			vs := &versionSet{LastSequence: tt.lastManifestSeq}

			seq, mem, err := getMaxSequenceNumber(ctx, vs, wal)
			require.NoError(t, err)
			require.Equal(t, tt.wantSeq, seq)
			if tt.wantMemLoaded {
				require.Greater(t, mem.ByteSize(), 0)
			}
		})
	}
}
