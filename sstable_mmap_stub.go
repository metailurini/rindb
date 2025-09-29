//go:build !darwin && !linux && !windows

package rindb

import "os"

type mmapHandle struct{}

func mapFile(*os.File) (*mmapHandle, error) { return nil, errMmapUnsupported }
func (h *mmapHandle) Close() error          { return nil }
func (h *mmapHandle) Bytes() []byte         { return nil }
