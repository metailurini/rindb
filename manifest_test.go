package rindb

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManifest(t *testing.T) {
	t.Run("WriteRead", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		mf := filepath.Join(dir, "MANIFEST-000001")
		w, err := NewManifestWriter(ctx, mf)
		require.NoError(t, err)
		edit := VersionEdit{ComparatorName: "bytes", LastSequence: 7}
		require.NoError(t, w.Append(edit))
		require.NoError(t, w.Sync())
		require.NoError(t, w.Close())

		r, err := NewManifestReader(ctx, mf)
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
		mf := filepath.Join(dir, "MANIFEST-000001")
		w, err := NewManifestWriter(ctx, mf)
		require.NoError(t, err)
		edit := VersionEdit{LastSequence: 1}
		require.NoError(t, w.Append(edit))
		require.NoError(t, w.Close())

		f, err := os.OpenFile(mf, os.O_RDWR, 0)
		require.NoError(t, err)
		_, err = f.Seek(8, io.SeekStart)
		require.NoError(t, err)
		b := []byte{0}
		_, err = f.Read(b)
		require.NoError(t, err)
		b[0] ^= 0xff
		_, err = f.Seek(8, io.SeekStart)
		require.NoError(t, err)
		_, err = f.Write(b)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		r, err := NewManifestReader(ctx, mf)
		require.NoError(t, err)
		_, err = r.Next()
		require.ErrorIs(t, err, ErrChecksumMismatch)
		require.NoError(t, r.Close())
	})

	t.Run("ReaderTruncated", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		mf := filepath.Join(dir, "MANIFEST-000001")
		w, err := NewManifestWriter(ctx, mf)
		require.NoError(t, err)
		edit := VersionEdit{LastSequence: 1}
		require.NoError(t, w.Append(edit))
		require.NoError(t, w.Close())

		fi, err := os.Stat(mf)
		require.NoError(t, err)
		require.NoError(t, os.Truncate(mf, fi.Size()-1))

		r, err := NewManifestReader(ctx, mf)
		require.NoError(t, err)
		_, err = r.Next()
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.NoError(t, r.Close())
	})

	t.Run("RecoverVersionSet", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		mf := "MANIFEST-000001"
		path := filepath.Join(dir, mf)
		w, err := NewManifestWriter(ctx, path)
		require.NoError(t, err)
		seq := uint64(123)
		require.NoError(t, w.Append(VersionEdit{LastSequence: seq}))
		require.NoError(t, w.Sync())
		require.NoError(t, w.Close())
		require.NoError(t, WriteCURRENT(ctx, dir, mf))

		vs, _, err := recoverVersionSet(ctx, dir)
		require.NoError(t, err)
		require.Equal(t, seq, vs.LastSequence)
	})

	t.Run("RecoverVersionSetNoManifest", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		vs, _, err := recoverVersionSet(ctx, dir)
		require.NoError(t, err)
		require.Equal(t, uint64(0), vs.LastSequence)
	})
}
