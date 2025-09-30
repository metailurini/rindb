//go:build !darwin && !linux && !windows

package rindb

import (
	"errors"
	"os"
)

type mmapHandle struct{}

var errMmapUnsupported = newMmapUnsupportedError(errors.New("sstable mmap: unsupported platform"))

func mapFile(*os.File) (*mmapHandle, error) { return nil, errMmapUnsupported }
func (h *mmapHandle) Close() error          { return nil }
func (h *mmapHandle) Bytes() []byte         { return nil }
