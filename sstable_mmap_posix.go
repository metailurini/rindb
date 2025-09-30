//go:build darwin || linux

package rindb

import (
	"fmt"
	"math"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

type mmapHandle struct {
	data []byte
}

func mapFile(f *os.File) (*mmapHandle, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size == 0 {
		return nil, nil
	}
	if unsafe.Sizeof(uintptr(0)) == 4 && size > math.MaxInt32 {
		return nil, fmt.Errorf("sstable mmap: file too large for 32-bit build (%d bytes)", size)
	}
	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &mmapHandle{data: data}, nil
}

func (h *mmapHandle) Close() error {
	if h == nil || h.data == nil {
		return nil
	}
	data := h.data
	h.data = nil
	return unix.Munmap(data)
}

func (h *mmapHandle) Bytes() []byte {
	if h == nil {
		return nil
	}
	return h.data
}
