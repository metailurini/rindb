package rindb

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const manifestRecordHeaderSize = 2 * checksumSize

// ManifestWriter appends edits to a MANIFEST file.
type ManifestWriter interface {
	Append(VersionEdit) error
	Sync() error
	Close() error
	Path() string
}

// ManifestReader iterates over manifest records.
type ManifestReader interface {
	Next() (VersionEdit, error)
	Close() error
}

type fileManifestWriter struct {
	fs *FileSystem
}

type fileManifestReader struct {
	fs *FileSystem
}

// NewManifestWriter creates a writer for the given path.
func NewManifestWriter(ctx context.Context, path string) (ManifestWriter, error) {
	fs, err := OpenFS(ctx, path)
	if err != nil {
		return nil, err
	}
	if _, err := fs.Seek(0, io.SeekEnd); err != nil {
		_ = fs.Close()
		return nil, err
	}
	return &fileManifestWriter{fs: fs}, nil
}

func (w *fileManifestWriter) Append(edit VersionEdit) error {
	data, err := json.Marshal(edit)
	if err != nil {
		return err
	}
	var header [manifestRecordHeaderSize]byte
	byteOrder.PutUint32(header[0:checksumSize], uint32(len(data)))
	crc := checksum(data)
	byteOrder.PutUint32(header[checksumSize:manifestRecordHeaderSize], crc)
	if _, err := w.fs.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.fs.Write(data); err != nil {
		return err
	}
	return nil
}

func (w *fileManifestWriter) Sync() error  { return w.fs.Sync() }
func (w *fileManifestWriter) Close() error { return w.fs.Close() }
func (w *fileManifestWriter) Path() string { return w.fs.Path() }

// NewManifestReader opens a reader for the manifest at path.
func NewManifestReader(ctx context.Context, path string) (ManifestReader, error) {
	fs, err := OpenFS(ctx, path)
	if err != nil {
		return nil, err
	}
	return &fileManifestReader{fs: fs}, nil
}

func (r *fileManifestReader) Next() (VersionEdit, error) {
	var header [manifestRecordHeaderSize]byte
	if _, err := io.ReadFull(r.fs, header[:]); err != nil {
		return VersionEdit{}, err
	}
	n := byteOrder.Uint32(header[0:checksumSize])
	crc := byteOrder.Uint32(header[checksumSize:manifestRecordHeaderSize])
	data := make([]byte, n)
	if _, err := io.ReadFull(r.fs, data); err != nil {
		return VersionEdit{}, err
	}
	if checksum(data) != crc {
		return VersionEdit{}, ErrChecksumMismatch
	}
	var edit VersionEdit
	if err := json.Unmarshal(data, &edit); err != nil {
		return VersionEdit{}, err
	}
	return edit, nil
}

func (r *fileManifestReader) Close() error { return r.fs.Close() }

// WriteCURRENT atomically updates the CURRENT file to point to manifest.
func WriteCURRENT(ctx context.Context, dir, manifest string) error {
	tmp := filepath.Join(dir, CurrentTmp)
	fs, err := OpenFS(ctx, tmp)
	if err != nil {
		return err
	}
	if _, err := fs.Write([]byte(manifest + "\n")); err != nil {
		_ = fs.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := fs.Sync(); err != nil {
		_ = fs.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := fs.Rename(filepath.Join(dir, CurrentFile)); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// RecoverVersionSet rebuilds the VersionSet by replaying the MANIFEST and
// updates the allocator with any NextFileNumber entries.
func RecoverVersionSet(ctx context.Context, dir string, a *FileNumberAllocator) (*VersionSet, string, error) {
	vs := &VersionSet{}
	curr := filepath.Join(dir, CurrentFile)
	data, err := os.ReadFile(curr)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return vs, "", nil
		}
		return nil, "", err
	}
	manifest := strings.TrimSpace(string(data))
	manifestPath := filepath.Join(dir, manifest)
	r, err := NewManifestReader(ctx, manifestPath)
	if err != nil {
		return nil, "", err
	}
	defer r.Close()
	for {
		edit, err := r.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, "", err
		}
		if err := edit.Apply(vs); err != nil {
			return nil, "", err
		}
		if a != nil {
			a.Apply(edit)
		}
	}
	return vs, manifestPath, nil
}
