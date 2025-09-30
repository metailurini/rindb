package rindb

import (
	"fmt"
	"io"
)

type offsetReader struct {
	fs     *FileSystem
	offset int64
}

func newOffsetReader(fs *FileSystem, off int64) *offsetReader {
	return &offsetReader{fs: fs, offset: off}
}

func (r *offsetReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if data, ok := r.peekSlice(len(p)); ok {
		copy(p, data)
		return len(p), nil
	}
	n, err := r.fs.ReadAt(p, r.offset)
	if n > 0 {
		r.offset += int64(n)
	}
	return n, err
}

func (r *offsetReader) Offset() int64 {
	return r.offset
}

func (r *offsetReader) peekSlice(length int) ([]byte, bool) {
	if length < 0 {
		return nil, false
	}
	data := r.fs.mmapBytes(r.offset, length)
	if data == nil {
		return nil, false
	}
	if length == 0 {
		return data[:0], true
	}
	if len(data) < length {
		return nil, false
	}
	r.offset += int64(length)
	return data, true
}

func (r *offsetReader) PrevOffset() (int64, error) {
	if r.offset == 0 {
		return 0, io.EOF
	}
	if r.offset < mdByteSize {
		return 0, fmt.Errorf("offset %d smaller than trailer size %d", r.offset, mdByteSize)
	}

	var trailer [mdByteSize]byte
	start := r.offset - mdByteSize
	if _, err := r.fs.ReadAt(trailer[:], start); err != nil {
		return 0, err
	}

	size := int64(byteOrder.Uint64(trailer[:]))
	if size <= 0 {
		return 0, fmt.Errorf("invalid record size trailer %d at offset %d", size, r.offset)
	}

	prev := r.offset - size
	if prev < 0 {
		return 0, fmt.Errorf("record size %d exceeds current offset %d", size, r.offset)
	}

	return prev, nil
}
