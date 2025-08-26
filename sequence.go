package rindb

import "math"

func getMaxSeq(seq ...uint64) uint64 {
	if len(seq) > 0 {
		return seq[0]
	}
	return math.MaxUint64
}
