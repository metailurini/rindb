package rindb

import (
	"errors"
	"os"
	"path/filepath"
)

// removeFiles deletes SSTable files corresponding to the given metadata.
// It ignores missing files and returns the first encountered error.
func removeFiles(dir string, files []FileMeta) error {
	var first error
	for _, f := range files {
		p := filepath.Join(dir, sstPath(f.Number))
		if err := os.Remove(p); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if first == nil {
				first = err
			}
		}
	}
	return first
}
