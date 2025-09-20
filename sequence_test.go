package rindb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSequence_GetMaxSequenceNumber verifies that the helper returns the
// larger sequence between the manifest and WAL and provides the loaded memtable.
func TestSequence_GetMaxSequenceNumber(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		walRecords      []Record
		lastManifestSeq uint64
		wantSeq         uint64
		checkMemtable   bool
	}{
		{
			name: "WAL higher than manifest",
			walRecords: []Record{
				newRecord(Bytes("k1"), Bytes("v1"), 1),
				newRecord(Bytes("k2"), Bytes("v2"), 5),
			},
			lastManifestSeq: 3,
			wantSeq:         5,
			checkMemtable:   true,
		},
		{
			name: "Manifest higher than WAL",
			walRecords: []Record{
				newRecord(Bytes("k1"), Bytes("v1"), 1),
			},
			lastManifestSeq: 10,
			wantSeq:         10,
			checkMemtable:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := testConfig(t)
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
			if tt.checkMemtable {
				iter := mem.Iterator()

				var recordCount int
				for iter.HasNext() {
					_, err := iter.Next()
					require.NoError(t, err)
					recordCount++
				}
				require.Equal(t, len(tt.walRecords), recordCount, "memtable should have all records from the WAL")
			}
		})
	}
}
