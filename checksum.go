package rindb

import "hash/crc32"

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func checksum(parts ...[]byte) uint32 {
	var sum uint32
	for _, p := range parts {
		sum = crc32.Update(sum, crc32cTable, p)
	}
	return sum
}
