//go:build windows

package rindb

import (
	"fmt"
	"math"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

type mmapHandle struct {
	data   []byte
	handle windows.Handle
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
	h, err := windows.CreateFileMapping(windows.Handle(f.Fd()), nil, windows.PAGE_READONLY, 0, 0, nil)
	if err != nil {
		return nil, err
	}
	ptr, err := windows.MapViewOfFile(h, windows.FILE_MAP_READ, 0, 0, uintptr(size))
	if err != nil {
		windows.CloseHandle(h)
		return nil, err
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(ptr)), int(size))
	return &mmapHandle{data: data, handle: h}, nil
}

func (h *mmapHandle) Close() error {
	if h == nil || h.data == nil {
		return nil
	}
	data := h.data
	h.data = nil
	errUnmap := windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&data[0])))

	var errClose error
	if h.handle != 0 {
		errClose = windows.CloseHandle(h.handle)
		h.handle = 0
	}

	if errUnmap != nil {
		if errClose != nil {
			return fmt.Errorf("unmap view failed: %w (and close handle failed: %v)", errUnmap, errClose)
		}
		return errUnmap
	}
	return errClose
}

func (h *mmapHandle) Bytes() []byte {
	if h == nil {
		return nil
	}
	return h.data
}
