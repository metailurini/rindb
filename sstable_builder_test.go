package rindb

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSTableBuilder(t *testing.T) {
	t.Run("BuildWithoutAdd", func(t *testing.T) {
		ctx := context.Background()
		cfg := testConfig()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]

		builder, err := NewSSTableBuilder(ctx, cfg, fs, 0)
		assert.NoError(t, err)

		_, _, _, err = builder.Build(ctx)
		assert.EqualError(t, err, "no records to build")
	})

	t.Run("Build", func(t *testing.T) {
		ctx := context.Background()
		cfg := testConfig()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]

		recs := []Record{
			newRecord(Bytes("a"), Bytes("1"), 1),
			newRecord(Bytes("b"), Bytes("2"), 2),
			newRecord(Bytes("c"), Bytes("3"), 3),
		}
		builder, err := NewSSTableBuilder(ctx, cfg, fs, len(recs))
		assert.NoError(t, err)
		for _, r := range recs {
			assert.NoError(t, builder.Add(r))
		}

		sst, meta, _, err := builder.Build(ctx)
		assert.NoError(t, err)
		require.NotZero(t, meta.Number)

		var offset int64
		for i, r := range recs {
			assert.Equal(t, r.GetKey(), sst.SparseIndex[i].key)
			assert.Equal(t, offset, sst.SparseIndex[i].offset)
			assert.True(t, sst.Bloom.Lookup(r.GetKey()))
			offset += int64(CalOnDiskSize(r))
		}
		reader := newOffsetReader(fs, 0)
		for _, r := range recs {
			rec, size, err := readRecord(reader)
			require.NoError(t, err)
			assert.Equal(t, r.GetKey(), rec.GetKey())
			assert.Equal(t, r.GetValue(), rec.GetValue())
			assert.Equal(t, CalOnDiskSize(r), size)
		}
		assert.Equal(t, offset, reader.Offset())
		info, err := os.Stat(fs.Path())
		require.NoError(t, err)
		tail := info.Size() - footerSize
		f, err := readFooter(fs, tail)
		require.NoError(t, err)
		assert.Equal(t, uint64(offset), f.indexOffset)
		assert.Equal(t, uint64(tail-offset), f.indexSize)
		assert.Equal(t, magicNumber, f.magic)
		assert.False(t, sst.Bloom.Lookup(Bytes("z")))
	})

	t.Run("BuildWithNilBloom", func(t *testing.T) {
		ctx := context.Background()
		cfg := testConfig()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]

		recs := []Record{
			newRecord(Bytes("a"), Bytes("1"), 1),
			newRecord(Bytes("b"), Bytes("2"), 2),
		}
		builder, err := NewSSTableBuilder(ctx, cfg, fs, 0)
		assert.NoError(t, err)
		for _, r := range recs {
			assert.NoError(t, builder.Add(r))
		}

		sst, _, _, err := builder.Build(ctx)
		assert.NoError(t, err)
		for _, r := range recs {
			assert.True(t, sst.Bloom.Lookup(r.GetKey()))
		}
		assert.False(t, sst.Bloom.Lookup(Bytes("z")))
	})

	t.Run("AddEnforcesOrder", func(t *testing.T) {
		ctx := context.Background()
		cfg := testConfig()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]

		builder, err := NewSSTableBuilder(ctx, cfg, fs, 4)
		assert.NoError(t, err)

		assert.NoError(t, builder.Add(newRecord(Bytes("a"), Bytes("1"), 2)))
		assert.NoError(t, builder.Add(newRecord(Bytes("a"), Bytes("0"), 1)))
		assert.Error(t, builder.Add(newRecord(Bytes("a"), Bytes("2"), 1)))
		assert.Error(t, builder.Add(newRecord(Bytes("0"), Bytes("3"), 3)))
	})

	t.Run("TruncatesExistingFile", func(t *testing.T) {
		ctx := context.Background()
		cfg := testConfig()
		initial := bytes.Repeat([]byte("x"), 256)
		fss, closer := initTempFileSystems(t, 1, [][]byte{initial})
		defer closer()
		fs := fss[0]

		builder, err := NewSSTableBuilder(ctx, cfg, fs, 1)
		assert.NoError(t, err)
		assert.NoError(t, builder.Add(newRecord(Bytes("a"), Bytes("1"), 1)))

		_, _, written, err := builder.Build(ctx)
		assert.NoError(t, err)
		info, err := os.Stat(fs.Path())
		assert.NoError(t, err)
		assert.Equal(t, written, info.Size())
	})

	t.Run("Errors", func(t *testing.T) {
		ctx := context.Background()
		cfg := testConfig()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]

		builder, err := NewSSTableBuilder(ctx, cfg, fs, 2)
		assert.NoError(t, err)

		rec := newRecord(Bytes("a"), Bytes("1"), 1)
		assert.NoError(t, builder.Add(rec))

		_, _, _, err = builder.Build(ctx)
		assert.NoError(t, err)

		t.Run("AddAfterBuild", func(t *testing.T) {
			err := builder.Add(newRecord(Bytes("b"), Bytes("2"), 2))
			assert.ErrorIs(t, err, ErrSSTableAlreadyBuilt)
		})

		t.Run("BuildTwice", func(t *testing.T) {
			_, _, _, err := builder.Build(ctx)
			assert.ErrorIs(t, err, ErrSSTableAlreadyBuilt)
		})
	})

	t.Run("CleansOnWriteError", func(t *testing.T) {
		ctx := context.Background()
		cfg := testConfig()

		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "fail.sst")

		fs := &FileSystem{filePath: path}

		builder, err := NewSSTableBuilder(ctx, cfg, fs, 1)
		require.NoError(t, err)

		rw := fs.file
		ro, err := os.Open(path)
		require.NoError(t, err)
		_ = rw.Close()
		fs.file = ro

		require.NoError(t, builder.Add(newRecord(Bytes("a"), Bytes("1"), 1)))

		_, _, _, err = builder.Build(ctx)
		require.Error(t, err)

		info, statErr := os.Stat(path)
		require.NoError(t, statErr)
		require.Equal(t, int64(0), info.Size())

		require.NoError(t, fs.Close())
	})

	t.Run("CommitFailure", func(t *testing.T) {
		ctx := context.Background()
		cfg := testConfig()
		fss, closer := initTempFileSystems(t, 1, nil)
		defer closer()
		fs := fss[0]

		builder, err := NewSSTableBuilder(ctx, cfg, fs, 1)
		require.NoError(t, err)
		require.NoError(t, builder.Add(newRecord(Bytes("a"), Bytes("1"), 1)))

		builder.commit = func(tx *transaction, ctx context.Context) error {
			return errors.New("commit fail")
		}

		_, _, _, err = builder.Build(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "commit fail")
		require.NoError(t, builder.Close(ctx))

		info, err := os.Stat(fs.Path())
		require.NoError(t, err)
		require.Equal(t, int64(0), info.Size())
	})
}
