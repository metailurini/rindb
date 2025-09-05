package rindb

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
)

const (
	manifestRecordLengthSize = 8
	manifestRecordHeaderSize = manifestRecordLengthSize + checksumSize
)

// manifestWriter appends edits to a MANIFEST file.
type manifestWriter interface {
	Append(versionEdit) error
	Sync() error
	Close() error
	Path() string
}

// manifestReader iterates over manifest records.
type manifestReader interface {
	Next() (versionEdit, error)
	Close() error
}

type fileManifestWriter struct {
	fs  *FileSystem
	buf bytes.Buffer
	enc *gob.Encoder
}

type fileManifestReader struct {
	fs *FileSystem
}

// newManifestWriter creates a writer for the given path.
func newManifestWriter(ctx context.Context, path string) (manifestWriter, error) {
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

func (w *fileManifestWriter) Append(edit versionEdit) error {
	w.buf.Reset()
	// gob.Encoder caches type information, so create a new encoder per record to
	// ensure each entry is self-contained.
	w.enc = gob.NewEncoder(&w.buf)
	if err := w.enc.Encode(edit); err != nil {
		return err
	}
	data := w.buf.Bytes()
	var header [manifestRecordHeaderSize]byte
	byteOrder.PutUint64(header[0:manifestRecordLengthSize], uint64(len(data)))
	crc := checksum(data)
	byteOrder.PutUint32(header[manifestRecordLengthSize:], crc)
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

// newManifestReader opens a reader for the manifest at path.
func newManifestReader(ctx context.Context, path string) (manifestReader, error) {
	fs, err := OpenFS(ctx, path)
	if err != nil {
		return nil, err
	}
	return &fileManifestReader{fs: fs}, nil
}

func (r *fileManifestReader) Next() (versionEdit, error) {
	var header [manifestRecordHeaderSize]byte
	if _, err := io.ReadFull(r.fs, header[:]); err != nil {
		return versionEdit{}, err
	}
	n := byteOrder.Uint64(header[0:manifestRecordLengthSize])
	if n > uint64(math.MaxInt) {
		return versionEdit{}, fmt.Errorf("manifest record size %d exceeds max slice size on this architecture", n)
	}
	crc := byteOrder.Uint32(header[manifestRecordLengthSize:])
	data := make([]byte, int(n))
	if _, err := io.ReadFull(r.fs, data); err != nil {
		return versionEdit{}, err
	}
	if checksum(data) != crc {
		return versionEdit{}, ErrChecksumMismatch
	}
	var edit versionEdit
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&edit); err != nil {
		return versionEdit{}, err
	}
	return edit, nil
}

func (r *fileManifestReader) Close() error { return r.fs.Close() }

// writeCurrent atomically updates the CURRENT file to point to manifest.
func writeCurrent(ctx context.Context, dir, manifest string) error {
	tmp := filepath.Join(dir, CurrentTmp)
	fs, err := OpenFS(ctx, tmp)
	if err != nil {
		return err
	}
	defer func() {
		_ = fs.Close()
		_ = os.Remove(tmp)
	}()

	if _, err := fs.Write([]byte(manifest + "\n")); err != nil {
		return err
	}
	if err := fs.Sync(); err != nil {
		return err
	}
	if err := fs.Rename(filepath.Join(dir, CurrentFile)); err != nil {
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

// recoverVersionSet rebuilds the versionSet by replaying the MANIFEST and
// updates the allocator with any NextFileNumber entries.
func recoverVersionSet(ctx context.Context, dir string, a *fileNumberAllocator) (*versionSet, string, error) {
	vs := &versionSet{}
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
	r, err := newManifestReader(ctx, manifestPath)
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
		if err := edit.apply(vs); err != nil {
			return nil, "", err
		}
		if a != nil {
			a.apply(edit)
		}
	}
	return vs, manifestPath, nil
}
