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

func TestManifest_Operations(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "write and read",
			run: func(t *testing.T) {
				ctx, _, mf := newManifestPath(t)
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
			},
		},
		{
			name: "bad checksum",
			run: func(t *testing.T) {
				ctx, _, mf := newManifestPath(t)
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
			},
		},
		{
			name: "truncated manifest",
			run: func(t *testing.T) {
				ctx, _, mf := newManifestPath(t)
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
			},
		},
		{
			name: "length overflow",
			run: func(t *testing.T) {
				ctx, _, mf := newManifestPath(t)
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
			},
		},
		{
			name: "recover version set",
			run: func(t *testing.T) {
				ctx, dir, mf := newManifestPath(t)
				w, err := newManifestWriter(ctx, mf)
				require.NoError(t, err)
				seq := uint64(123)
				next := uint64(7)
				require.NoError(t, w.Append(versionEdit{LastSequence: seq, NextFileNumber: next}))
				require.NoError(t, w.Sync())
				require.NoError(t, w.Close())
				require.NoError(t, writeCurrent(ctx, dir, filepath.Base(mf)))

				alloc := newFileNumberAllocator(1)
				vs, _, err := recoverVersionSet(ctx, dir, alloc)
				require.NoError(t, err)
				require.Equal(t, seq, vs.LastSequence)
				require.Equal(t, next, vs.NextFileNumber)
				require.Equal(t, next, alloc.peek())
			},
		},
		{
			name: "recover version set without manifest",
			run: func(t *testing.T) {
				ctx, dir, _ := newManifestPath(t)
				alloc := newFileNumberAllocator(1)
				vs, _, err := recoverVersionSet(ctx, dir, alloc)
				require.NoError(t, err)
				require.Equal(t, uint64(0), vs.LastSequence)
				require.Equal(t, uint64(1), alloc.peek())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func newManifestPath(t *testing.T) (context.Context, string, string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	mf := filepath.Join(dir, DefaultManifestFile)
	return ctx, dir, mf
}
