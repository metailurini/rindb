package rindb

import (
	"encoding/binary"
	"fmt"
)

// footer layout: |8 index offset|8 index size|24 padding|8 magic|
type footer struct {
	indexOffset uint64
	indexSize   uint64
	_           [24]byte
	magic       uint64
}

func writeFooter(tx *transaction, f footer) error {
	var buf [footerSize]byte
	binary.BigEndian.PutUint64(buf[0:8], f.indexOffset)
	binary.BigEndian.PutUint64(buf[8:16], f.indexSize)
	binary.BigEndian.PutUint64(buf[40:48], f.magic)
	if _, err := tx.write(buf[:]); err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}
	return nil
}

func readFooter(fs *FileSystem, off int64) (footer, error) {
	var f footer
	var buf [footerSize]byte
	if _, err := fs.ReadAt(buf[:], off); err != nil {
		return f, err
	}

	f.indexOffset = binary.BigEndian.Uint64(buf[0:8])
	f.indexSize = binary.BigEndian.Uint64(buf[8:16])
	for i := 16; i < 40; i += 8 {
		if binary.BigEndian.Uint64(buf[i:i+8]) != 0 {
			return f, ErrMalFormedSSTable
		}
	}
	f.magic = binary.BigEndian.Uint64(buf[40:48])
	if f.magic != magicNumber {
		return f, ErrMalFormedSSTable
	}
	return f, nil
}
