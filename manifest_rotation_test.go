package rindb

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManifestRotation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rin, cleanup := initRinDBWithCleanup(t,
		WithDatabaseDir(dir),
		WithMaxMemtableSize(1),
		WithLevel0CompactionThreshold(1000),
		WithManifestSizeThreshold(1024),
	)
	defer cleanup()

	puts := 11
	for i := 0; i < puts; i++ {
		key := Bytes(fmt.Sprintf("k%02d", i))
		require.NoError(t, rin.Put(ctx, key, Bytes("v")))
	}
	rin.wg.Wait()

	data, err := os.ReadFile(filepath.Join(dir, CurrentFile))
	require.NoError(t, err)
	mf := strings.TrimSpace(string(data))
	require.NotEqual(t, DefaultManifestFile, mf)

	r, err := NewManifestReader(ctx, filepath.Join(dir, mf))
	require.NoError(t, err)
	edit, err := r.Next()
	require.NoError(t, err)
	require.Len(t, edit.AddFiles, puts)
	_, err = r.Next()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, r.Close())
}

func TestMaybeRotateManifest(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := NewConfig(WithDatabaseDir(dir), WithManifestSizeThreshold(10))
	fs, err := OpenFS(ctx, filepath.Join(dir, DefaultManifestFile))
	require.NoError(t, err)
	mw := NewManifestWriterMock(fs)
	vs := &VersionSet{}
	rin := &Rindb{config: cfg, versionSet: vs, manifest: mw, ssTableManager: &SSTableManager{manifest: mw, versionSet: vs, config: cfg}}

	// Below threshold
	require.NoError(t, rin.maybeRotateManifest(ctx))
	require.Equal(t, fs.Path(), rin.manifest.Path())

	// Exceed threshold
	_, err = fs.Write([]byte(strings.Repeat("x", int(cfg.manifestSizeThreshold+1))))
	require.NoError(t, err)
	oldPath := rin.manifest.Path()
	require.NoError(t, rin.maybeRotateManifest(ctx))
	require.NotEqual(t, oldPath, rin.manifest.Path())
}

type manifestWriterMock struct{ *FileSystem }

func NewManifestWriterMock(fs *FileSystem) *manifestWriterMock { return &manifestWriterMock{fs} }

func (m *manifestWriterMock) Append(VersionEdit) error { return nil }
func (m *manifestWriterMock) Sync() error              { return nil }
func (m *manifestWriterMock) Close() error             { return m.FileSystem.Close() }
