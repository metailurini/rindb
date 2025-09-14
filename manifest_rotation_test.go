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

func TestManifestRotation_Rotates(t *testing.T) {
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

	r, err := newManifestReader(ctx, filepath.Join(dir, mf))
	require.NoError(t, err)
	edit, err := r.Next()
	require.NoError(t, err)
	require.Len(t, edit.AddFiles, puts)
	_, err = r.Next()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, r.Close())
}

func TestManifestRotation_MaybeRotate(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name       string
		writeExtra bool
		rotated    bool
	}{
		{"below threshold", false, false},
		{"exceed threshold", true, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			cfg := NewConfig(WithDatabaseDir(dir), WithManifestSizeThreshold(10))
			fs, err := OpenFS(ctx, filepath.Join(dir, DefaultManifestFile))
			require.NoError(t, err)
			mw := newManifestWriterMock(fs)
			vs := &versionSet{}
			rin := &Rindb{config: cfg, versionSet: vs, manifest: mw, SSTableManager: &ssTableManager{manifest: mw, versionSet: vs, config: cfg}}

			if tc.writeExtra {
				_, err = fs.Write([]byte(strings.Repeat("x", int(cfg.manifestSizeThreshold+1))))
				require.NoError(t, err)
			}

			oldPath := rin.manifest.Path()
			require.NoError(t, rin.maybeRotateManifest(ctx))
			if tc.rotated {
				require.NotEqual(t, oldPath, rin.manifest.Path())
			} else {
				require.Equal(t, oldPath, rin.manifest.Path())
			}
		})
	}
}

func TestManifestRotation_Cleanup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	for i := 1; i <= 5; i++ {
		path := filepath.Join(dir, manifestPath(i))
		require.NoError(t, os.WriteFile(path, []byte("x"), 0o600))
	}
	require.NoError(t, writeCurrent(ctx, dir, manifestPath(5)))
	require.NoError(t, cleanupManifests(dir, manifestsToKeepAfterRotation))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "MANIFEST-") {
			names = append(names, e.Name())
		}
	}
	expected := []string{manifestPath(5)}
	for i := 1; i <= manifestsToKeepAfterRotation; i++ {
		expected = append(expected, manifestPath(5-i))
	}
	require.ElementsMatch(t, expected, names)
}

func TestManifestRotation_CleanupAfterRotation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := NewConfig(WithDatabaseDir(dir), WithManifestSizeThreshold(10))
	cfg.newManifestWriterFunc = func(ctx context.Context, p string) (manifestWriter, error) {
		fs, err := OpenFS(ctx, p)
		if err != nil {
			return nil, err
		}
		return newManifestWriterMock(fs), nil
	}
	fs, err := OpenFS(ctx, filepath.Join(dir, DefaultManifestFile))
	require.NoError(t, err)
	mw := newManifestWriterMock(fs)
	vs := &versionSet{}
	rin := &Rindb{config: cfg, versionSet: vs, manifest: mw, SSTableManager: &ssTableManager{manifest: mw, versionSet: vs, config: cfg}}

	for i := 0; i < 3; i++ {
		_, err = rin.manifest.(*manifestWriterMock).Write([]byte(strings.Repeat("x", int(cfg.manifestSizeThreshold+1))))
		require.NoError(t, err)
		require.NoError(t, rin.maybeRotateManifest(ctx))
	}

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var manifests []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "MANIFEST-") {
			manifests = append(manifests, e.Name())
		}
	}
	require.Equal(t, manifestsToKeepAfterRotation+1, len(manifests))
}

func TestManifestRotation_Recovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	db, err := InitRinDB(ctx,
		WithDatabaseDir(dir),
		WithMaxMemtableSize(1),
		WithLevel0CompactionThreshold(1000),
		WithManifestSizeThreshold(1024),
	)
	require.NoError(t, err)

	const numPuts = 11
	for i := 0; i < numPuts; i++ {
		key := Bytes(fmt.Sprintf("k%02d", i))
		require.NoError(t, db.Put(ctx, key, Bytes("v")))
	}
	require.NoError(t, db.Close())

	data, err := os.ReadFile(filepath.Join(dir, CurrentFile))
	require.NoError(t, err)
	mf := strings.TrimSpace(string(data))
	require.NotEqual(t, DefaultManifestFile, mf)

	alloc := newFileNumberAllocator(1)
	vs, manifestPath, err := recoverVersionSet(ctx, dir, alloc)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, mf), manifestPath)
	require.Len(t, vs.Levels[0], numPuts)
}

type manifestWriterMock struct{ *FileSystem }

func newManifestWriterMock(fs *FileSystem) *manifestWriterMock { return &manifestWriterMock{fs} }

func (m *manifestWriterMock) Append(versionEdit) error { return nil }
func (m *manifestWriterMock) Sync() error              { return nil }
func (m *manifestWriterMock) Close() error             { return m.FileSystem.Close() }
