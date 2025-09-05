package rindb

import (
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManifest(t *testing.T) {
	t.Run("WriteRead", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		mf := filepath.Join(dir, DefaultManifestFile)
		w, err := newManifestWriter(ctx, mf)
		require.NoError(t, err)
		edit := versionEdit{ComparatorName: "bytes", LastSequence: 7}
		require.NoError(t, w.Append(edit))
		require.NoError(t, w.Sync())
		require.NoError(t, w.Close())

		r, err := newManifestReader(ctx, mf)
		require.NoError(t, err)
		got, err := r.Next()
		require.NoError(t, err)
		require.Equal(t, edit, got)
		_, err = r.Next()
		require.ErrorIs(t, err, io.EOF)
		require.NoError(t, r.Close())
	})

	t.Run("ReaderBadCRC", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		mf := filepath.Join(dir, DefaultManifestFile)
		w, err := newManifestWriter(ctx, mf)
		require.NoError(t, err)
		edit := versionEdit{LastSequence: 1}
		require.NoError(t, w.Append(edit))
		require.NoError(t, w.Close())

		f, err := os.OpenFile(mf, os.O_RDWR, 0)
		require.NoError(t, err)
		_, err = f.Seek(int64(manifestRecordHeaderSize), io.SeekStart)
		require.NoError(t, err)
		b := []byte{0}
		_, err = f.Read(b)
		require.NoError(t, err)
		b[0] ^= 0xff
		_, err = f.Seek(int64(manifestRecordHeaderSize), io.SeekStart)
		require.NoError(t, err)
		_, err = f.Write(b)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		r, err := newManifestReader(ctx, mf)
		require.NoError(t, err)
		_, err = r.Next()
		require.ErrorIs(t, err, ErrChecksumMismatch)
		require.NoError(t, r.Close())
	})

	t.Run("ReaderTruncated", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		mf := filepath.Join(dir, DefaultManifestFile)
		w, err := newManifestWriter(ctx, mf)
		require.NoError(t, err)
		edit := versionEdit{LastSequence: 1}
		require.NoError(t, w.Append(edit))
		require.NoError(t, w.Close())

		fi, err := os.Stat(mf)
		require.NoError(t, err)
		require.NoError(t, os.Truncate(mf, fi.Size()-1))

		r, err := newManifestReader(ctx, mf)
		require.NoError(t, err)
		_, err = r.Next()
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.NoError(t, r.Close())
	})

	t.Run("ReaderLengthOverflow", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		mf := filepath.Join(dir, DefaultManifestFile)
		f, err := os.Create(mf)
		require.NoError(t, err)
		var header [manifestRecordHeaderSize]byte
		byteOrder.PutUint64(header[0:manifestRecordLengthSize], uint64(math.MaxInt)+1)
		byteOrder.PutUint32(header[manifestRecordLengthSize:], 0)
		_, err = f.Write(header[:])
		require.NoError(t, err)
		require.NoError(t, f.Close())

		r, err := newManifestReader(ctx, mf)
		require.NoError(t, err)
		_, err = r.Next()
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds max slice size")
		require.NoError(t, r.Close())
	})

	t.Run("RecoverVersionSet", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		mf := DefaultManifestFile
		path := filepath.Join(dir, mf)
		w, err := newManifestWriter(ctx, path)
		require.NoError(t, err)
		seq := uint64(123)
		next := uint64(7)
		require.NoError(t, w.Append(versionEdit{LastSequence: seq, NextFileNumber: next}))
		require.NoError(t, w.Sync())
		require.NoError(t, w.Close())
		require.NoError(t, writeCurrent(ctx, dir, mf))

		alloc := newFileNumberAllocator(1)
		vs, _, err := recoverVersionSet(ctx, dir, alloc)
		require.NoError(t, err)
		require.Equal(t, seq, vs.LastSequence)
		require.Equal(t, next, vs.NextFileNumber)
		require.Equal(t, next, alloc.peek())
	})

	t.Run("RecoverVersionSetNoManifest", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		alloc := newFileNumberAllocator(1)
		vs, _, err := recoverVersionSet(ctx, dir, alloc)
		require.NoError(t, err)
		require.Equal(t, uint64(0), vs.LastSequence)
		require.Equal(t, uint64(1), alloc.peek())
	})
}
