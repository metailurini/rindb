package rindb

import "errors"

type mmapUnsupported interface {
	error
	isMmapUnsupported()
}

type mmapUnsupportedWrapper struct{ error }

func (m mmapUnsupportedWrapper) isMmapUnsupported() {}

func newMmapUnsupportedError(err error) error {
	if err == nil {
		err = errors.New("sstable mmap: unsupported platform")
	}
	return mmapUnsupportedWrapper{error: err}
}

func isMmapUnsupported(err error) bool {
	_, ok := err.(mmapUnsupported)
	return ok
}
