package rindb

import (
	"context"
	"errors"
	"fmt"
	"math"
)

var ErrSSTableAlreadyBuilt = errors.New("SSTable already built")

type SSTableBuilder struct {
	tx       *transaction
	index    []KeyOffset
	bloom    *BloomFilter
	offset   int64
	fs       *FileSystem
	config   Config
	built    bool
	lastKey  Bytes
	lastSeq  uint64
	smallest InternalKey
	largest  InternalKey
	seqLo    uint64
	seqHi    uint64
	commit   func(tx *transaction, ctx context.Context) error
}

func NewSSTableBuilder(ctx context.Context, cfg Config, fs *FileSystem, expected int) (*SSTableBuilder, error) {
	if fs == nil {
		return nil, fmt.Errorf("file system is nil")
	}
	if !fs.IsOpened() {
		if err := fs.Open(ctx); err != nil {
			return nil, fmt.Errorf("failed to open file system: %w", err)
		}
	}
	if err := fs.Clean(); err != nil {
		return nil, fmt.Errorf("failed to clean file system: %w", err)
	}
	tm := newTransactionManager()
	tx, err := tm.begin(ctx, fs)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	var bloom *BloomFilter
	if expected > 0 {
		bloom = NewBloomFilter(
			SetN(uint64(expected)),
			SetP(cfg.bloomFalsePositiveRate),
			WithCalculatedM(),
			WithCalculatedK(),
		)
	}
	cap := expected
	if cap <= 0 {
		cap = int(cfg.maxMemtableSize)
	}
	return &SSTableBuilder{
		tx:     tx,
		index:  make([]KeyOffset, 0, cap),
		bloom:  bloom,
		offset: 0,
		fs:     fs,
		config: cfg,
		seqLo:  math.MaxUint64,
		commit: func(tx *transaction, ctx context.Context) error {
			return tx.commit(ctx)
		},
	}, nil
}

func (b *SSTableBuilder) Add(rec Record) error {
	if b.built {
		return ErrSSTableAlreadyBuilt
	}

	key := rec.GetKey()
	seq := rec.GetSequenceNumber()
	if b.lastKey != nil {
		cmp := key.Compare(b.lastKey)
		if cmp == CmpLess {
			return fmt.Errorf("keys must be in non-decreasing order")
		}
		if cmp == CmpEqual && seq >= b.lastSeq {
			return fmt.Errorf("sequence numbers for the same key must be strictly decreasing")
		}
	}
	if err := writeRecord(b.tx, rec); err != nil {
		return err
	}
	ik := InternalKey{UserKey: key.Clone(), Seq: seq, Type: rec.GetType()}
	b.index = append(b.index, KeyOffset{key: key.Clone(), offset: b.offset})
	if b.bloom != nil {
		b.bloom.Insert(key)
	}
	b.offset += int64(CalOnDiskSize(rec))
	b.lastKey = key.Clone()
	b.lastSeq = seq
	if len(b.index) == 1 {
		b.smallest = ik
	}
	b.largest = ik
	if seq < b.seqLo {
		b.seqLo = seq
	}
	if seq > b.seqHi {
		b.seqHi = seq
	}
	return nil
}

func (b *SSTableBuilder) Build(ctx context.Context) (sst SStable, meta fileMeta, written int64, err error) {
	if len(b.index) == 0 {
		return SStable{}, fileMeta{}, 0, fmt.Errorf("no records to build")
	}
	if b.built {
		return SStable{}, fileMeta{}, 0, ErrSSTableAlreadyBuilt
	}

	defer func() {
		if err != nil {
			if cleanErr := b.fs.Clean(); cleanErr != nil {
				warn(ctx, "failed to clean file system after error: %v", cleanErr)
			}
			sst = SStable{}
			meta = fileMeta{}
			written = 0
		}
	}()

	if b.bloom == nil {
		n := len(b.index)
		b.bloom = NewBloomFilter(
			SetN(uint64(n)),
			SetP(b.config.bloomFalsePositiveRate),
			WithCalculatedM(),
			WithCalculatedK(),
		)
		for _, ko := range b.index {
			b.bloom.Insert(ko.key)
		}
	}
	indexOffset := b.tx.size()
	for _, ko := range b.index {
		if err = writeKeyOffset(b.tx, ko); err != nil {
			return
		}
	}
	indexSize := b.tx.size() - indexOffset
	if err = writeFooter(b.tx, footer{
		indexOffset: uint64(indexOffset),
		indexSize:   uint64(indexSize),
		magic:       magicNumber,
	}); err != nil {
		err = fmt.Errorf("failed to write footer: %w", err)
		return
	}
	written = b.tx.size()
	if err = b.commit(b.tx, ctx); err != nil {
		err = fmt.Errorf("failed to commit transaction: %w", err)
		return
	}

	if err = b.fs.Sync(); err != nil {
		err = fmt.Errorf("failed to sync file system for %s: %w", b.fs.Path(), err)
		return
	}

	b.built = true
	sst = SStable{FileSystem: b.fs, SparseIndex: b.index, Bloom: b.bloom}
	num, nerr := fileNum(b.fs.Path())
	if nerr != nil {
		err = fmt.Errorf("invalid sstable path %s: %w", b.fs.Path(), nerr)
		return
	}
	meta = fileMeta{
		Number:   num,
		Smallest: b.smallest,
		Largest:  b.largest,
		Size:     uint64(written),
		SeqLo:    b.seqLo,
		SeqHi:    b.seqHi,
	}
	return
}

func (b *SSTableBuilder) Close(ctx context.Context) error {
	if b.tx != nil && b.tx.isActive() {
		return b.tx.rollback(ctx)
	}
	return nil
}
