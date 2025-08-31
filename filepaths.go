package rindb

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

func walPath(num uint64) string { return fmt.Sprintf("%06d.wal", num) }
func sstPath(num uint64) string { return fmt.Sprintf("%06d.sst", num) }

// fileNum extracts the numeric identifier from a WAL or SSTable path.
func fileNum(p string) (uint64, error) {
	base := filepath.Base(p)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	return strconv.ParseUint(name, 10, 64)
}
