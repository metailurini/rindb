package rindb

import (
	"context"
	"errors"
	"fmt"
)

var ErrSSTableAlreadyBuilt = errors.New("SSTable already built")

type SSTableBuilder struct {
	tx      *Transaction
	index   []KeyOffset
	bloom   *BloomFilter
	offset  int64
	fs      *FileSystem
	config  Config
	built   bool
	lastKey Bytes
	lastSeq uint64
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
	tm := NewTransactionManager()
	tx := tm.Begin()
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
	if err := WriteRecord(b.tx, rec); err != nil {
		return err
	}
	b.index = append(b.index, KeyOffset{key: key.Clone(), offset: b.offset})
	if b.bloom != nil {
		b.bloom.Insert(key)
	}
	b.offset += int64(CalOnDiskSize(rec))
	b.lastKey = key.Clone()
	b.lastSeq = seq
	return nil
}

func (b *SSTableBuilder) Build(ctx context.Context) (SStable, int, error) {
	if len(b.index) == 0 {
		return SStable{}, 0, fmt.Errorf("no records to build")
	}
	if b.built {
		return SStable{}, 0, ErrSSTableAlreadyBuilt
	}

	if b.bloom == nil {
		n := len(b.index)
		if n > 0 {
			b.bloom = NewBloomFilter(
				SetN(uint64(n)),
				SetP(b.config.bloomFalsePositiveRate),
				WithCalculatedM(),
				WithCalculatedK(),
			)
			for _, ko := range b.index {
				b.bloom.Insert(ko.key)
			}
		} else {
			// n=0 would panic when calculating Bloom filter parameters.
			// Use n=1 to create a valid but empty filter.
			b.bloom = NewBloomFilter(
				SetN(1),
				SetP(b.config.bloomFalsePositiveRate),
				WithCalculatedM(),
				WithCalculatedK(),
			)
		}
	}
	sparseIndexOffset := int64(b.tx.buffer.Len())
	for _, ko := range b.index {
		if err := writeKeyOffset(b.tx, ko); err != nil {
			return SStable{}, 0, err
		}
	}
	if err := WriteNumber(b.tx, uint64(sparseIndexOffset)); err != nil {
		return SStable{}, 0, fmt.Errorf("failed to write sparse index offset: %w", err)
	}
	written := b.tx.buffer.Len()
	if err := b.tx.Commit(ctx, b.fs); err != nil {
		if cleanErr := b.fs.Clean(); cleanErr != nil {
			WARN(ctx, "failed to clean file system after commit error: %v", cleanErr)
		}
		return SStable{}, 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	if err := b.fs.Sync(); err != nil {
		if cleanErr := b.fs.Clean(); cleanErr != nil {
			WARN(ctx, "failed to clean file system after sync error: %v", cleanErr)
		}
		return SStable{}, 0, fmt.Errorf("failed to sync file system for %s: %w", b.fs.Path(), err)
	}

	b.built = true
	return SStable{FileSystem: b.fs, SparseIndex: b.index, Bloom: b.bloom}, written, nil
}

func (b *SSTableBuilder) Close(ctx context.Context) error {
	if b.tx != nil && b.tx.IsActive() {
		return b.tx.Rollback(ctx)
	}
	return nil
}
