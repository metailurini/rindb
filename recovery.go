package rindb

import "context"

// getMaxSequenceNumber loads the WAL and compares its highest sequence with
// the manifest's LastSequence. It returns the larger of the two along with the
// loaded memtable.
func getMaxSequenceNumber(ctx context.Context, vs *VersionSet, wal *WAL) (uint64, Memtable, error) {
	maxSeq := vs.LastSequence

	mem, err := wal.Load(ctx)
	if err != nil {
		return 0, Memtable{}, err
	}

	walSeq, err := getMaxSequenceNumberFromMemtable(mem)
	if err != nil {
		return 0, Memtable{}, err
	}

	if walSeq > maxSeq {
		maxSeq = walSeq
	}
	return maxSeq, mem, nil
}
