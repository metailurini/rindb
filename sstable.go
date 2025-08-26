package rindb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"time"
)

var (
	ErrKeyNotFound      = errors.New("key not found")
	ErrTombstoneFound   = errors.New("tombstone found")
	ErrMalFormedSSTable = errors.New("malformed sstable")
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

// GetSequenceNumber implements Record.
func (k KeyOffset) GetSequenceNumber() uint64 {
	return 0
}

// GetType implements Record.
func (k KeyOffset) GetType() RecordType {
	return TypeValue
}

func (s SparseIndex) GetOffset(key Bytes) (int64, error) {
	idx := sort.Search(len(s), func(i int) bool {
		return Compare(s[i].key, key) >= 0
	})

	if idx < len(s) && Compare(s[idx].key, key) == CmpEqual {
		return s[idx].offset, nil
	}

	return 0, ErrKeyNotFound
}

type SStable struct {
	*FileSystem
	SparseIndex SparseIndex
	Bloom       *BloomFilter
}

func (s SStable) GetValue(ctx context.Context, key Bytes, seq ...uint64) (Bytes, error) {
	ctx, span := sstableTracer.Start(ctx, "SStable.GetValue")
	start := time.Now()
	var bytesRead int
	defer func() {
		span.End()
		getValueLatency.Record(ctx, float64(time.Since(start).Milliseconds()))
		if bytesRead > 0 {
			getValueIOSize.Add(ctx, int64(bytesRead))
		}
	}()

	maxSeq := getMaxSeq(seq...)

	if !s.Bloom.Lookup(key) {
		return nil, ErrKeyNotFound
	}

	offset, err := s.SparseIndex.GetOffset(key)
	if err != nil {
		return nil, err
	}

	if _, err := s.file.Seek(offset, io.SeekStart); err != nil {
		ERROR(ctx, "Failed to seek to offset %d in %s: %v", offset, s.Path(), err)
		return nil, fmt.Errorf("failed to seek to offset %d: %w", offset, err)
	}

	for {
		record, err := ReadRecord(s)
		if err != nil {
			if errors.Is(err, io.EOF) {
				ERROR(ctx, "Unexpected EOF after seeking to offset %d in %s", offset, s.Path())
				return nil, fmt.Errorf("unexpected EOF after seeking to offset %d: %w", offset, ErrMalFormedSSTable)
			}
			ERROR(ctx, "Failed to read record at offset %d in %s: %v", offset, s.Path(), err)
			return nil, fmt.Errorf("failed to read record at offset %d: %w", offset, err)
		}
		bytesRead += CalOnDiskSize(record)
		if record.GetKey().Compare(key) != CmpEqual {
			return nil, ErrKeyNotFound
		}
		if record.GetSequenceNumber() <= maxSeq {
			if record.GetType() == TypeDeletion {
				return nil, ErrTombstoneFound
			}
			return record.GetValue(), nil
		}
	}
}

// GetKeyRange returns the minimum and maximum keys in the SStable.
func (s SStable) GetKeyRange() (Bytes, Bytes) {
	if len(s.SparseIndex) == 0 {
		return nil, nil // Or handle as an error, depending on desired behavior
	}
	minKey := s.SparseIndex[0].key
	maxKey := s.SparseIndex[len(s.SparseIndex)-1].key
	return minKey, maxKey
}

// Overlaps checks if the SSTable’s key range overlaps with the given range [min, max].
// Overlap occurs if sstable.min <= max AND sstable.max >= min.
func (s SStable) Overlaps(min, max Bytes) bool {
	sstMin, sstMax := s.GetKeyRange()
	if sstMin == nil || sstMax == nil {
		return false // Empty sstable cannot overlap
	}

	// Check if sstable range is entirely before the given range
	if sstMax.Compare(min) < 0 {
		return false
	}
	// Check if sstable range is entirely after the given range
	if sstMin.Compare(max) > 0 {
		return false
	}

	// Otherwise, there is an overlap
	return true
}

func NewSSTable(ctx context.Context, config Config, fs *FileSystem) (SStable, error) {
	fileInfo, err := os.Stat(fs.Path())
	if err != nil {
		return SStable{}, fmt.Errorf("failed to get file info for %s: %w", fs.Path(), err)
	}
	if fileInfo.Size() < mdByteSize {
		ERROR(ctx, "File %s is too small (%d bytes) to be a valid SSTable", fs.Path(), fileInfo.Size())
		return SStable{}, ErrMalFormedSSTable
	}

	sparseIndex, err := loadSparseIndex(fs)
	if err != nil {
		return SStable{}, fmt.Errorf("failed to load sparse index from %s: %w", fs.Path(), err)
	}

	bloom := NewBloomFilter(
		SetN(uint64(len(sparseIndex))),
		SetP(config.bloomFalsePositiveRate),
		WithCalculatedM(),
		WithCalculatedK(),
	)
	for _, ko := range sparseIndex {
		bloom.Insert(ko.key)
	}

	// Seek to the beginning of the file
	// Support testing assertions
	fs.file.Seek(0, io.SeekStart)
	INFO(ctx, "Successfully created SSTable at %s with %d sparse index entries", fs.Path(), len(sparseIndex))
	return SStable{FileSystem: fs, SparseIndex: sparseIndex, Bloom: bloom}, nil
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
		return SparseIndex{}, fmt.Errorf("failed to seek to tail of sstable %s: %w", fs.Path(), err)
	}

	sparseIndexOffset, err := ReadNumber(fs)
	if err != nil {
		return SparseIndex{}, fmt.Errorf("failed to read sparse index offset from %s: %w", fs.Path(), err)
	}

	ret, err := fs.file.Seek(int64(sparseIndexOffset), io.SeekStart)
	if err != nil {
		return SparseIndex{}, fmt.Errorf("failed to seek to sparse index offset %d in %s: %w", sparseIndexOffset, fs.Path(), err)
	}

	sparseIndex := SparseIndex{}
	// Read records until the calculated end of the sparse index data
	for ret < tailSSTableOffset {
		record, err := ReadRecord(fs)
		if err != nil {
			// Check for EOF specifically, might indicate corruption
			if errors.Is(err, io.EOF) {
				return SparseIndex{}, fmt.Errorf("unexpected EOF while reading sparse index in %s: %w", fs.Path(), ErrMalFormedSSTable)
			}
			return SparseIndex{}, fmt.Errorf("failed to read sparse index record in %s: %w", fs.Path(), err)
		}
		sparseIndex = append(sparseIndex, NewKeyOffset(record.GetKey(), record.GetValue()))

		ret, err = fs.CursorPos()
		if err != nil {
			return SparseIndex{}, fmt.Errorf("failed to get cursor position in %s: %w", fs.Path(), err)
		}
	}
	// Verify that the final cursor position matches the expected end of the sparse index data.
	if ret != tailSSTableOffset {
		return SparseIndex{}, fmt.Errorf("mismatched sparse index size in %s: expected end at %d, but read until %d: %w",
			fs.Path(), tailSSTableOffset, ret, ErrMalFormedSSTable)
	}
	return sparseIndex, nil
}

func (s SStable) MaxSequenceNumber() (uint64, error) {
	iterator, err := s.Iterator()
	if err != nil {
		return 0, err
	}

	var maxSeqNum uint64
	for iterator.HasNext() {
		record, err := iterator.Next()
		if err != nil {
			return 0, err
		}
		if record.GetSequenceNumber() > maxSeqNum {
			maxSeqNum = record.GetSequenceNumber()
		}
	}
	return maxSeqNum, nil
}

func flush(ctx context.Context, config Config, mem Memtable, fs *FileSystem) (SStable, error) {
	ctx, span := sstableTracer.Start(ctx, "flush")
	start := time.Now()
	var written int
	defer func() {
		span.End()
		flushLatency.Record(ctx, float64(time.Since(start).Milliseconds()))
		if written > 0 {
			flushIOSize.Add(ctx, int64(written))
		}
	}()

	if mem.data.Len() == 0 {
		ERROR(ctx, "Flushing empty memtable! It's a bug!")
		log.Panic("empty memtable!")
	}

	tm := NewTransactionManager()
	tx := tm.Begin()
	defer tx.Rollback(ctx)

	r := mem.data.Head().Next()
	for r != nil {
		if err := WriteRecord(tx, r.Value); err != nil {
			return SStable{}, fmt.Errorf("failed to write record to transaction buffer: %w", err)
		}
		r = r.Next()
	}

	sparseIndexOffset := uint64(tx.buffer.Len())

	sparseIndex := genSparseIndex(mem)
	for _, v := range sparseIndex {
		if err := WriteRecord(tx, v); err != nil {
			return SStable{}, fmt.Errorf("failed to write sparse index entry to transaction buffer: %w", err)
		}
	}

	if err := WriteNumber(tx, sparseIndexOffset); err != nil {
		return SStable{}, fmt.Errorf("failed to write sparse index offset to transaction buffer: %w", err)
	}

	written = tx.buffer.Len()
	if err := tx.Commit(ctx, fs); err != nil {
		return SStable{}, fmt.Errorf("failed to commit transaction to file system %s: %w", fs.Path(), err)
	}
	INFO(ctx, "Flushed memtable to SSTable at %s with %d entries", fs.Path(), mem.data.Len())

	// after flushing memtable to file system successfully.
	// memtable is supposed to be purged
	mem.Clear()

	return NewSSTable(ctx, config, fs)
}

func genSparseIndex(mem Memtable) SparseIndex {
	sparseIndex := make(SparseIndex, 0, mem.data.Len())

	cursor := int64(0)
	runNode := mem.data.Head().Next()
	for runNode != nil {
		sparseIndex = append(sparseIndex, KeyOffset{runNode.Key.UserKey, cursor})
		cursor += int64(CalOnDiskSize(runNode.Value))
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
	_, err := s.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	return &sstableIterator{
		currentIdx: 0,
		maxIdx:     len(s.SparseIndex),
		FileSystem: s.FileSystem,
	}, nil
}

// sstableIRange iterates over a range of keys in an SSTable.
type sstableIRange struct {
	s       *SStable
	current int
	endKey  Bytes
	seq     uint64
}

// HasNext implements Iterator.
func (sri *sstableIRange) HasNext() bool {
	return sri.current < len(sri.s.SparseIndex) && sri.s.SparseIndex[sri.current].key.Compare(sri.endKey) <= 0
}

// Next implements Iterator.
func (sri *sstableIRange) Next() (Record, error) {
	for sri.HasNext() {
		rec, err := ReadRecord(sri.s)
		if err != nil {
			return nil, err
		}
		sri.current++
		if rec.GetSequenceNumber() > sri.seq {
			continue
		}
		return rec, nil
	}
	return nil, EOI
}

// IRange returns an iterator over records with keys in [start, end] and sequence
// numbers less than or equal to seq.
func (s SStable) IRange(start, end Bytes, seq ...uint64) (Iterator[Record], error) {
	maxSeq := getMaxSeq(seq...)
	startIdx := sort.Search(len(s.SparseIndex), func(i int) bool {
		return s.SparseIndex[i].key.Compare(start) >= 0
	})
	if startIdx >= len(s.SparseIndex) {
		return &sstableIRange{s: &s, current: startIdx, endKey: end, seq: maxSeq}, nil
	}
	if _, err := s.file.Seek(s.SparseIndex[startIdx].offset, io.SeekStart); err != nil {
		return nil, err
	}
	return &sstableIRange{s: &s, current: startIdx, endKey: end, seq: maxSeq}, nil
}
