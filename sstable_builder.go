package rindb

import (
	"context"
	"fmt"
	"os"
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
		index:  make([]KeyOffset, 0),
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

func (b *SSTableBuilder) Build(ctx context.Context) (SStable, error) {
	sparseIndexOffset := int64(b.tx.buffer.Len())
	for _, ko := range b.index {
		if err := writeKeyOffset(b.tx, ko); err != nil {
			return SStable{}, err
		}
	}
	if err := WriteNumber(b.tx, uint64(sparseIndexOffset)); err != nil {
		return SStable{}, fmt.Errorf("failed to write sparse index offset: %w", err)
	}
	if err := b.tx.Commit(ctx, b.fs); err != nil {
		return SStable{}, fmt.Errorf("failed to commit transaction: %w", err)
	}
	if b.bloom != nil {
		bloomPath := b.fs.Path() + ".bloom"
		file, err := os.OpenFile(bloomPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, fileSystemPermission)
		if err != nil {
			return SStable{}, fmt.Errorf("failed to create bloom filter file %s: %w", bloomPath, err)
		}
		defer file.Close()
		var buf [mdByteSize]byte
		byteOrder.PutUint64(buf[:], uint64(b.bloom.bucket.size))
		if _, err := file.Write(buf[:]); err != nil {
			return SStable{}, fmt.Errorf("failed to write bloom filter size: %w", err)
		}
		for _, word := range b.bloom.bucket.set {
			byteOrder.PutUint64(buf[:], word)
			if _, err := file.Write(buf[:]); err != nil {
				return SStable{}, fmt.Errorf("failed to write bloom filter data: %w", err)
			}
		}
	}
	return NewSSTable(ctx, b.config, b.fs)
}

func (b *SSTableBuilder) Close() error {
	if b.tx != nil && b.tx.IsActive() {
		return b.tx.Rollback(context.Background())
	}
	return nil
}
