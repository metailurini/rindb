package rindb

import (
	"bytes"
	"io"
	"log"
	"os"

	"github.com/pkg/errors"
)

type (
	KeyOffset struct {
		key    Bytes
		offset int64
	}

	SparseIndex []KeyOffset
)

var _ Record = KeyOffset{}

func NewKeyOffset(key, offset Bytes) KeyOffset {
	return KeyOffset{
		key:    key,
		offset: int64(byteOrder.Uint64(offset)),
	}
}

// GetSize implements Record.
func (k KeyOffset) GetSize() int {
	return len(k.GetKey()) + mdByteSize
}

// GetKey implements Record.
func (k KeyOffset) GetKey() Bytes {
	return k.key
}

// GetValue implements Record.
func (k KeyOffset) GetValue() Bytes {
	valueLenBytes := make(Bytes, mdByteSize)
	byteOrder.PutUint64(valueLenBytes, uint64(k.offset))
	return valueLenBytes
}

func (s SparseIndex) GetOffset(key Bytes) (int64, error) {
	headIdx := 0
	tailIdx := len(s) - 1

	for {
		if Compare(key, s[headIdx].key) == CmpEqual {
			return s[headIdx].offset, nil
		}

		if Compare(key, s[tailIdx].key) == CmpEqual {
			return s[tailIdx].offset, nil
		}

		const half = 2
		midIdx := (headIdx + tailIdx) / half
		if headIdx == midIdx || tailIdx == midIdx {
			break
		}

		if Compare(key, s[midIdx].key) == CmpLess {
			tailIdx = midIdx
		} else {
			headIdx = midIdx
		}
	}

	return 0, ErrKeyNotFound
}

var ErrMalFormedSSTable = errors.New("malformed sstable")

type SStable struct {
	*FileSystem
	SparseIndex SparseIndex
}

func (s SStable) GetValue(key Bytes) (Bytes, error) {
	offset, err := s.SparseIndex.GetOffset(key)
	if err != nil {
		return nil, err
	}

	if _, err := s.file.Seek(offset, io.SeekStart); err != nil {
		return nil, errors.Wrap(err, "failed to seek to offset")
	}

	record, err := ReadRecord(s)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read record")
	}
	return record.GetValue(), nil
}

func NewSSTable(fs *FileSystem) (SStable, error) {
	fileInfo, err := os.Stat(fs.Path())
	if err != nil {
		return SStable{}, errors.Wrap(err, "failed to load file info")
	}
	if fileInfo.Size() < mdByteSize {
		ERROR("Failed to load file %s: %v", fs.Path(), ErrMalFormedSSTable)
		return SStable{}, ErrMalFormedSSTable
	}

	sparseIndex, err := loadSparseIndex(fs)
	if err != nil {
		return SStable{}, errors.Wrap(err, "failed to load sparse index")
	}
	return SStable{fs, sparseIndex}, nil
}

func readTailSSTable(fs *FileSystem) (int64, error) {
	tailSSTableOffset, err := fs.file.Seek(-1*mdByteSize, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	return tailSSTableOffset, nil
}

func loadSparseIndex(fs *FileSystem) (SparseIndex, error) {
	tailSSTableOffset, err := readTailSSTable(fs)
	if err != nil {
		return SparseIndex{}, errors.Wrap(err, "failed to seek tail of sstable")
	}

	sparseIndexOffset, err := ReadNumber(fs)
	if err != nil {
		return SparseIndex{}, errors.Wrap(err, "failed to read offset sparse index")
	}

	ret, err := fs.file.Seek(int64(sparseIndexOffset), io.SeekStart)
	if err != nil {
		return SparseIndex{}, errors.Wrap(err, "failed to seek offset of sparse index")
	}

	sparseIndex := SparseIndex{}
	for {
		if !(ret < tailSSTableOffset) {
			break
		}

		record, err := ReadRecord(fs)
		if err != nil {
			return SparseIndex{}, errors.Wrap(err, "failed to read record")
		}
		sparseIndex = append(sparseIndex, NewKeyOffset(record.GetKey(), record.GetValue()))

		ret, err = fs.CursorPos()
		if err != nil {
			return SparseIndex{}, errors.Wrap(err, "failed to read current cursor position")
		}
	}
	return sparseIndex, nil
}

func Flush(mem Memtable, fs *FileSystem) (SStable, error) {
	if mem.data.Len() == 0 {
		WARN("Flushing empty memtable!")
		log.Panic("empty memtable!")
	}

	// txBuf is a buffer for making sure that once
	// content wrote to a disk it must be full content
	txBuf := bytes.NewBufferString("")

	r := mem.data.Head().Next()
	for r != nil {
		err := WriteRecord(txBuf, RecordImpl{r.Key, r.Value})
		if err != nil {
			return SStable{}, errors.Wrap(err, "failed to write record to sstable")
		}

		r = r.Next()
	}

	// this sparseIndexOffset is standing for
	// end of data and offset sparse index
	sparseIndexOffset := uint64(txBuf.Len())

	sparseIndex := genSparseIndex(mem)
	for _, v := range sparseIndex {
		if err := WriteRecord(txBuf, v); err != nil {
			return SStable{}, errors.Wrap(err, "failed to write index to sstable")
		}
	}

	if err := WriteNumber(txBuf, sparseIndexOffset); err != nil {
		return SStable{}, errors.Wrap(err, "failed to write offset index to sstable")
	}

	if _, err := fs.Write(txBuf.Bytes()); err != nil {
		return SStable{}, err
	}

	if err := fs.Sync(); err != nil {
		return SStable{}, errors.Wrap(err, "failed to sync file system")
	}

	// after flushing memtable to file system successfully.
	// memtable is supposed to be purged
	mem.Clear()

	return SStable{fs, sparseIndex}, nil
}

func genSparseIndex(mem Memtable) SparseIndex {
	sparseIndex := make(SparseIndex, 0, mem.data.Len())

	cursor := int64(0)
	runNode := mem.data.Head().Next()
	for runNode != nil {
		sparseIndex = append(sparseIndex, KeyOffset{runNode.Key, cursor})
		cursor += int64(CalOnDiskSize(toRecord(runNode)))
		runNode = runNode.Next()
	}
	return sparseIndex
}

var _ Iterator[Record] = (*sstableIterator)(nil)

type sstableIterator struct {
	*FileSystem
	currentIdx int
	maxIdx     int
}

// HasNext implements Iterator.
func (s *sstableIterator) HasNext() bool {
	return s.currentIdx < s.maxIdx
}

// Next implements Iterator.
func (s *sstableIterator) Next() (Record, error) {
	if s.HasNext() {
		record, err := ReadRecord(s)
		if err != nil {
			return nil, err
		}
		s.currentIdx += 1
		return record, nil
	}
	return nil, EOI
}

func (s SStable) Iterator() (Iterator[Record], error) {
	_, err := s.FileSystem.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	return &sstableIterator{
		currentIdx: 0,
		maxIdx:     len(s.SparseIndex),
		FileSystem: s.FileSystem,
	}, nil
}
