package rindb

import (
	"context"
	"math"
)

func getMaxSeq(seq ...uint64) uint64 {
	if len(seq) > 0 {
		return seq[0]
	}
	return math.MaxUint64
}

// getMaxSequenceNumber loads the WAL and compares its highest sequence with
// the manifest's LastSequence. It returns the larger of the two along with the
// loaded memtable.
func getMaxSequenceNumber(ctx context.Context, vs *versionSet, wal *wal) (uint64, memtable, error) {
	mem, err := wal.Load(ctx)
	if err != nil {
		return 0, memtable{}, err
	}

	walSeq, err := getMaxSequenceNumberFromMemtable(mem)
	if err != nil {
		return 0, memtable{}, err
	}

	return max(vs.LastSequence, walSeq), mem, nil
}
