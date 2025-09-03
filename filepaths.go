package rindb

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	walExt              = ".wal"
	sstExt              = ".sst"
	manifestFmt         = "MANIFEST-%06d"
	currentFile         = "CURRENT"
	currentTmp          = "CURRENT.tmp"
	defaultManifestFile = "MANIFEST-000001"
)

func walPath(num uint64) string { return fmt.Sprintf("%06d%s", num, walExt) }
func sstPath(num uint64) string { return fmt.Sprintf("%06d%s", num, sstExt) }

func manifestPath(num int) string { return fmt.Sprintf(manifestFmt, num) }

func manifestNum(base string) (int, error) {
	if !strings.HasPrefix(base, "MANIFEST-") {
		return 0, fmt.Errorf("invalid manifest name %s", base)
	}
	return strconv.Atoi(strings.TrimPrefix(base, "MANIFEST-"))
}

// fileNum extracts the numeric identifier from a WAL or SSTable path.
func fileNum(p string) (uint64, error) {
	base := filepath.Base(p)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	return strconv.ParseUint(name, 10, 64)
}
