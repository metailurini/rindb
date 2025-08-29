package rindb

import (
	"context"
	"fmt"
)

type SSTableBuilder struct {
	tx     *Transaction
	index  []KeyOffset
	bloom  *BloomFilter
	offset int64
	fs     *FileSystem
	config Config
}

func NewSSTableBuilder(ctx context.Context, cfg Config, fs *FileSystem) (*SSTableBuilder, error) {
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
	bloom := NewBloomFilter(
		SetN(uint64(cfg.maxMemtableSize)),
		SetP(cfg.bloomFalsePositiveRate),
		WithCalculatedM(),
		WithCalculatedK(),
	)
	return &SSTableBuilder{
		tx:     tx,
		index:  make([]KeyOffset, 0, int(cfg.maxMemtableSize)),
		bloom:  bloom,
		offset: 0,
		fs:     fs,
		config: cfg,
	}, nil
}

func (b *SSTableBuilder) Add(rec Record) error {
	if err := WriteRecord(b.tx, rec); err != nil {
		return err
	}
	b.index = append(b.index, KeyOffset{key: rec.GetKey().Clone(), offset: b.offset})
	b.bloom.Insert(rec.GetKey())
	b.offset += int64(CalOnDiskSize(rec))
	return nil
}

func (b *SSTableBuilder) Build(ctx context.Context) (SStable, int, error) {
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
		return SStable{}, 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return SStable{FileSystem: b.fs, SparseIndex: b.index, Bloom: b.bloom}, written, nil
}

func (b *SSTableBuilder) Close(ctx context.Context) error {
	if b.tx != nil && b.tx.IsActive() {
		return b.tx.Rollback(ctx)
	}
	return nil
}
