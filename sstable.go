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

func writeKeyOffset(tx *Transaction, ko KeyOffset) error {
	if err := WriteNumber(tx, uint64(len(ko.key))); err != nil {
		return fmt.Errorf("failed to write key length: %w", err)
	}
	if _, err := tx.Write(ko.key); err != nil {
		return fmt.Errorf("failed to write key bytes: %w", err)
	}
	if err := WriteNumber(tx, uint64(ko.offset)); err != nil {
		return fmt.Errorf("failed to write offset: %w", err)
	}
	return nil
}

func readKeyOffset(r io.Reader) (KeyOffset, error) {
	keyLen, err := ReadNumber(r)
	if err != nil {
		return KeyOffset{}, fmt.Errorf("failed to read key length: %w", err)
	}

	key := make(Bytes, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return KeyOffset{}, fmt.Errorf("failed to read key bytes: %w", err)
	}

	off, err := ReadNumber(r)
	if err != nil {
		return KeyOffset{}, fmt.Errorf("failed to read offset: %w", err)
	}

	return KeyOffset{key: key, offset: int64(off)}, nil
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

	if !s.IsOpened() {
		if err := s.Open(ctx); err != nil {
			return nil, fmt.Errorf("failed to open sstable %s: %w", s.Path(), err)
		}
	}

	if !s.Bloom.Lookup(key) {
		return nil, ErrKeyNotFound
	}

	offset, err := s.SparseIndex.GetOffset(key)
	if err != nil {
		offset = 0
	}

	reader := newOffsetReader(s.FileSystem, offset)
	for {
		record, err := ReadRecord(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				ERROR(ctx, "Unexpected EOF after reading at offset %d in %s", offset, s.Path())
				return nil, fmt.Errorf("unexpected EOF after reading at offset %d: %w", offset, ErrMalFormedSSTable)
			}
			if errors.Is(err, ErrChecksumMismatch) {
				ERROR(ctx, "Checksum mismatch at offset %d in %s", reader.Offset(), s.Path())
				return nil, fmt.Errorf("checksum mismatch at offset %d: %w", reader.Offset(), err)
			}
			if errors.Is(err, ErrFileNotOpened) {
				if err := s.Open(ctx); err != nil {
					return nil, fmt.Errorf("failed to reopen sstable %s: %w", s.Path(), err)
				}
				reader = newOffsetReader(s.FileSystem, reader.Offset())
				continue
			}
			ERROR(ctx, "Failed to read record at offset %d in %s: %v", reader.Offset(), s.Path(), err)
			return nil, fmt.Errorf("failed to read record at offset %d: %w", reader.Offset(), err)
		}
		bytesRead += CalOnDiskSize(record)
		cmp := record.GetKey().Compare(key)
		if cmp == CmpLess {
			continue
		}
		if cmp != CmpEqual {
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

	INFO(ctx, "Successfully created SSTable at %s with %d sparse index entries", fs.Path(), len(sparseIndex))
	return SStable{FileSystem: fs, SparseIndex: sparseIndex, Bloom: bloom}, nil
}

func readTailSSTable(fs *FileSystem) (int64, error) {
	info, err := os.Stat(fs.Path())
	if err != nil {
		return 0, err
	}
	return info.Size() - mdByteSize, nil
}

func loadSparseIndex(fs *FileSystem) (SparseIndex, error) {
	if !fs.IsOpened() {
		return SparseIndex{}, ErrFileNotOpened
	}

	tailOffset, err := readTailSSTable(fs)
	if err != nil {
		return SparseIndex{}, fmt.Errorf("failed to get sstable tail offset from %s: %w", fs.Path(), err)
	}

	buf := make([]byte, mdByteSize)
	if _, err := fs.ReadAt(buf, tailOffset); err != nil {
		return SparseIndex{}, fmt.Errorf("failed to read sparse index offset from %s: %w", fs.Path(), err)
	}
	sparseIndexOffset := byteOrder.Uint64(buf)

	offset := int64(sparseIndexOffset)
	sparseIndex := SparseIndex{}
	for offset < tailOffset {
		reader := newOffsetReader(fs, offset)
		ko, err := readKeyOffset(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return SparseIndex{}, fmt.Errorf("unexpected EOF while reading sparse index in %s: %w", fs.Path(), ErrMalFormedSSTable)
			}
			return SparseIndex{}, fmt.Errorf("failed to read sparse index entry in %s: %w", fs.Path(), err)
		}
		sparseIndex = append(sparseIndex, ko)
		offset = reader.Offset()
	}

	if offset != tailOffset {
		return SparseIndex{}, fmt.Errorf("mismatched sparse index size in %s: expected end at %d, but read until %d: %w",
			fs.Path(), tailOffset, offset, ErrMalFormedSSTable)
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

func flush(ctx context.Context, config Config, mem Memtable, fs *FileSystem) (SStable, FileMeta, error) {
	ctx, span := sstableTracer.Start(ctx, "flush")
	start := time.Now()
	var written int
	var meta FileMeta
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

	builder, err := NewSSTableBuilder(ctx, config, fs, int(mem.data.Len()))
	if err != nil {
		return SStable{}, FileMeta{}, fmt.Errorf("failed to create sstable builder: %w", err)
	}
	defer builder.Close(ctx)

	for r := mem.data.Head().Next(); r != nil; r = r.Next() {
		if err := builder.Add(r.Value); err != nil {
			return SStable{}, FileMeta{}, fmt.Errorf("failed to add record to builder: %w", err)
		}
	}

	var sst SStable
	sst, meta, written, err = builder.Build(ctx)
	if err != nil {
		written = 0
		return SStable{}, FileMeta{}, err
	}
	meta.Level = 0
	INFO(ctx, "Flushed memtable to SSTable at %s with %d entries", fs.Path(), mem.data.Len())

	// after flushing memtable to file system successfully.
	// memtable is supposed to be purged
	mem.Clear()

	return sst, meta, nil
}

var _ Iterator[Record] = (*sstableIterator)(nil)

type sstableIterator struct {
	*FileSystem
	currentIdx int
	maxIdx     int
	offset     int64
}

// HasNext implements Iterator.
func (s *sstableIterator) HasNext() bool {
	return s.currentIdx < s.maxIdx
}

// Next implements Iterator.
func (s *sstableIterator) Next() (Record, error) {
	if s.HasNext() {
		reader := newOffsetReader(s.FileSystem, s.offset)
		record, err := ReadRecord(reader)
		if err != nil {
			return nil, err
		}
		s.offset = reader.Offset()
		s.currentIdx++
		return record, nil
	}
	return nil, EOI
}

func (s SStable) Iterator() (Iterator[Record], error) {
	if !s.IsOpened() {
		if err := s.Open(context.Background()); err != nil {
			return nil, err
		}
	}

	return &sstableIterator{
		currentIdx: 0,
		maxIdx:     len(s.SparseIndex),
		FileSystem: s.FileSystem,
		offset:     0,
	}, nil
}

// sstableIRange iterates over a range of keys in an SSTable.
type sstableIRange struct {
	s       *SStable
	current int
	endKey  Bytes
	seq     uint64
	offset  int64
}

// HasNext implements Iterator.
func (sri *sstableIRange) HasNext() bool {
	return sri.current < len(sri.s.SparseIndex) && sri.s.SparseIndex[sri.current].key.Compare(sri.endKey) <= 0
}

// Next implements Iterator.
func (sri *sstableIRange) Next() (Record, error) {
	for sri.HasNext() {
		reader := newOffsetReader(sri.s.FileSystem, sri.offset)
		rec, err := ReadRecord(reader)
		if err != nil {
			return nil, err
		}
		sri.offset = reader.Offset()
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
	if !s.IsOpened() {
		if err := s.Open(context.Background()); err != nil {
			return nil, err
		}
	}
	startIdx := sort.Search(len(s.SparseIndex), func(i int) bool {
		return s.SparseIndex[i].key.Compare(start) >= 0
	})
	var startOffset int64
	if startIdx < len(s.SparseIndex) {
		startOffset = s.SparseIndex[startIdx].offset
	}
	return &sstableIRange{s: &s, current: startIdx, endKey: end, seq: maxSeq, offset: startOffset}, nil
}
