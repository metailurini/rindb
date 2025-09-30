package rindb

import "errors"

var errMmapUnsupported = errors.New("sstable mmap: unsupported platform")
