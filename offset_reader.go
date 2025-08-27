package rindb

type offsetReader struct {
	fs     *FileSystem
	offset int64
}

func newOffsetReader(fs *FileSystem, off int64) *offsetReader {
	return &offsetReader{fs: fs, offset: off}
}

func (r *offsetReader) Read(p []byte) (int, error) {
	n, err := r.fs.ReadAt(p, r.offset)
	r.offset += int64(n)
	return n, err
}

func (r *offsetReader) Offset() int64 {
	return r.offset
}
