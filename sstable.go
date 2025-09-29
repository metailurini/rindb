package rindb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"time"
)

var (
	ErrKeyNotFound      = errors.New("key not found")
	ErrTombstoneFound   = errors.New("tombstone found")
	ErrMalFormedSSTable = errors.New("malformed sstable")
)

const (
	footerSize = 48

	// 201867972885 is the magic number for the SSTable file format
	// formula: https://go.dev/play/p/Sewh1S3nTcL
	magicNumber = uint64(201867972885)
)

type (
	KeyOffset struct {
		key    Bytes
		offset int64
	}

	SparseIndex []KeyOffset
)

type SStable struct {
	*FileSystem
	SparseIndex SparseIndex
	Bloom       *BloomFilter
	dataEnd     int64
	log         scopedLogger
}

func NewSSTable(ctx context.Context, config Config, fs *FileSystem) (SStable, error) {
	log := config.scopedLogger()
	tailOffset, err := getSSTableTailOffset(fs)
	if err != nil {
		return SStable{}, err
	}
	f, err := readFooter(fs, tailOffset)
	if err != nil {
		return SStable{}, fmt.Errorf("failed to read footer from %s: %w", fs.Path(), err)
	}
	if f.indexSize == 0 {
		log.errorf(ctx, "index size is zero in %s", fs.Path())
		return SStable{}, ErrMalFormedSSTable
	}
	if f.indexOffset > uint64(tailOffset) || f.indexSize > uint64(tailOffset) {
		log.errorf(ctx, "invalid footer values in %s: offset=%d size=%d tail=%d", fs.Path(), f.indexOffset, f.indexSize, tailOffset)
		return SStable{}, ErrMalFormedSSTable
	}
	if f.indexOffset+f.indexSize > uint64(tailOffset) || f.indexOffset+f.indexSize < f.indexOffset {
		log.errorf(ctx, "index block out of bounds in %s: offset=%d size=%d tail=%d", fs.Path(), f.indexOffset, f.indexSize, tailOffset)
		return SStable{}, ErrMalFormedSSTable
	}
	if f.indexOffset+f.indexSize < uint64(tailOffset) {
		log.errorf(ctx, "index block does not align with footer in %s: offset=%d size=%d tail=%d", fs.Path(), f.indexOffset, f.indexSize, tailOffset)
		return SStable{}, ErrMalFormedSSTable
	}
	if f.indexOffset > uint64(math.MaxInt64) || f.indexSize > uint64(math.MaxInt64) {
		log.errorf(ctx, "index offset or size too large in %s: offset=%d size=%d", fs.Path(), f.indexOffset, f.indexSize)
		return SStable{}, ErrMalFormedSSTable
	}
	sparseIndex, err := loadSparseIndex(fs, int64(f.indexOffset), int64(f.indexSize))
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

	fs.configureMmap(ctx, config.enableSSTableMmap, log)
	log.info(ctx, "Successfully created SSTable at %s with %d sparse index entries", fs.Path(), len(sparseIndex))
	return SStable{FileSystem: fs, SparseIndex: sparseIndex, Bloom: bloom, dataEnd: int64(f.indexOffset), log: log}, nil
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
		if !errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}
		idx := sort.Search(len(s.SparseIndex), func(i int) bool {
			return Compare(s.SparseIndex[i].key, key) >= 0
		})
		if idx > 0 {
			offset = s.SparseIndex[idx-1].offset
		} else {
			offset = 0
		}
	}

	reader := newOffsetReader(s.FileSystem, offset)
	for {
		if reader.Offset() >= s.dataEnd {
			return nil, ErrKeyNotFound
		}
		record, size, err := readRecord(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.log.errorf(ctx, "Unexpected EOF after reading at offset %d in %s", offset, s.Path())
				return nil, fmt.Errorf("unexpected EOF after reading at offset %d: %w", offset, ErrMalFormedSSTable)
			}
			if errors.Is(err, ErrChecksumMismatch) {
				s.log.errorf(ctx, "Checksum mismatch at offset %d in %s", reader.Offset(), s.Path())
				return nil, fmt.Errorf("checksum mismatch at offset %d: %w", reader.Offset(), err)
			}
			// if errors.Is(err, ErrFileNotOpened) {
			// 	if err := s.Open(ctx); err != nil {
			// 		return nil, fmt.Errorf("failed to reopen sstable %s: %w", s.Path(), err)
			// 	}
			// 	reader = newOffsetReader(s.FileSystem, reader.Offset())
			// 	continue
			// }
			s.log.errorf(ctx, "Failed to read record at offset %d in %s: %v", reader.Offset(), s.Path(), err)
			return nil, fmt.Errorf("failed to read record at offset %d: %w", reader.Offset(), err)
		}
		bytesRead += size
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

func writeKeyOffset(tx *transaction, ko KeyOffset) error {
	if err := writeNumber(tx, uint64(len(ko.key))); err != nil {
		return fmt.Errorf("failed to write key length: %w", err)
	}
	if _, err := tx.write(ko.key); err != nil {
		return fmt.Errorf("failed to write key bytes: %w", err)
	}
	if err := writeNumber(tx, uint64(ko.offset)); err != nil {
		return fmt.Errorf("failed to write offset: %w", err)
	}
	return nil
}

func readKeyOffset(r io.Reader) (KeyOffset, error) {
	keyLen, err := readNumber(r)
	if err != nil {
		return KeyOffset{}, fmt.Errorf("failed to read key length: %w", err)
	}

	key := make(Bytes, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return KeyOffset{}, fmt.Errorf("failed to read key bytes: %w", err)
	}

	off, err := readNumber(r)
	if err != nil {
		return KeyOffset{}, fmt.Errorf("failed to read offset: %w", err)
	}

	return KeyOffset{key: key, offset: int64(off)}, nil
}

func loadSparseIndex(fs *FileSystem, offset, size int64) (SparseIndex, error) {
	if !fs.IsOpened() {
		return SparseIndex{}, ErrFileNotOpened
	}

	limit := offset + size
	reader := newOffsetReader(fs, offset)
	sparseIndex := SparseIndex{}
	for reader.Offset() < limit {
		ko, err := readKeyOffset(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return SparseIndex{}, fmt.Errorf("unexpected EOF while reading sparse index in %s: %w", fs.Path(), ErrMalFormedSSTable)
			}
			return SparseIndex{}, fmt.Errorf("failed to read sparse index entry in %s: %w", fs.Path(), err)
		}
		sparseIndex = append(sparseIndex, ko)
	}

	if reader.Offset() != limit {
		return SparseIndex{}, fmt.Errorf("mismatched sparse index size in %s: expected end at %d, but read until %d: %w",
			fs.Path(), limit, reader.Offset(), ErrMalFormedSSTable)
	}

	return sparseIndex, nil
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

func flush(ctx context.Context, config Config, mem memtable, fs *FileSystem) (SStable, fileMeta, error) {
	ctx, span := sstableTracer.Start(ctx, "flush")
	start := time.Now()
	var written int64
	var meta fileMeta
	defer func() {
		span.End()
		flushLatency.Record(ctx, float64(time.Since(start).Milliseconds()))
		if written > 0 {
			flushIOSize.Add(ctx, written)
		}
	}()

	if mem.data.Len() == 0 {
		config.scopedLogger().errorf(ctx, "Flushing empty memtable! It's a bug!")
		panic("empty memtable!")
	}

	builder, err := NewSSTableBuilder(ctx, config, fs, int(mem.data.Len()))
	if err != nil {
		return SStable{}, fileMeta{}, fmt.Errorf("failed to create sstable builder: %w", err)
	}
	defer builder.Close(ctx)

	for r := mem.data.Head().Next(); r != nil; r = r.Next() {
		if err := builder.Add(r.Value); err != nil {
			return SStable{}, fileMeta{}, fmt.Errorf("failed to add record to builder: %w", err)
		}
	}

	var sst SStable
	sst, meta, written, err = builder.Build(ctx)
	if err != nil {
		written = 0
		return SStable{}, fileMeta{}, err
	}
	meta.Level = 0
	config.scopedLogger().info(ctx, "Flushed memtable to SSTable at %s with %d entries", fs.Path(), mem.data.Len())

	// after flushing memtable to file system successfully.
	// memtable is supposed to be purged
	mem.Clear()

	return sst, meta, nil
}

func getSSTableTailOffset(fs *FileSystem) (int64, error) {
	fileInfo, err := os.Stat(fs.Path())
	if err != nil {
		return 0, fmt.Errorf("failed to get file info for %s: %w", fs.Path(), err)
	}
	if fileInfo.Size() < footerSize {
		return 0, fmt.Errorf("file %s is too small (%d bytes) to be a valid SSTable: %w", fs.Path(), fileInfo.Size(), ErrMalFormedSSTable)
	}
	return fileInfo.Size() - footerSize, nil
}
