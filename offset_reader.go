package rindb

type offsetReader struct {
	fs     *FileSystem
	offset int64
}

func newOffsetReader(fs *FileSystem, off int64) *offsetReader {
	return &offsetReader{fs: fs, offset: off}
}

func (r *offsetReader) Read(p []byte) (int, error) {
	orig := r.offset
	n, err := r.fs.ReadAt(p, orig)
	if err != nil {
		r.offset = orig
		return n, err
	}
	if n > 0 {
		r.offset = orig + int64(n)
	}
	return n, nil
}

func (r *offsetReader) Offset() int64 {
	return r.offset
}
