package rindb

import "fmt"

func walPath(num uint64) string { return fmt.Sprintf("%06d.wal", num) }
func sstPath(num uint64) string { return fmt.Sprintf("%06d.sst", num) }
