package rindb

import (
	"context"
	"os"
	"path/filepath"
)

// snapshotEdit returns a versionEdit that captures the full state of the
// versionSet. The edit can be written as a manifest snapshot.
func (vs *versionSet) snapshotEdit() versionEdit {
	edit := versionEdit{
		ComparatorName: vs.Comparator,
		LastSequence:   vs.LastSequence,
		NextFileNumber: vs.NextFileNumber,
		LogNumber:      vs.LogNumber,
		PrevLogNumber:  vs.PrevLogNumber,
	}
	for _, files := range vs.Levels {
		edit.AddFiles = append(edit.AddFiles, files...)
	}
	return edit
}

// rotateManifest writes a snapshot of vs to a new manifest file and updates the
// CURRENT file to point to it. It returns an open manifestWriter for further edits.
func rotateManifest(ctx context.Context, cfg Config, vs *versionSet, current manifestWriter) (manifestWriter, error) {
	currentPath := current.Path()
	dir := filepath.Dir(currentPath)
	base := filepath.Base(currentPath)
	num, err := manifestNum(base)
	if err != nil {
		return nil, err
	}
	num++
	newBase := manifestPath(num)
	newPath := filepath.Join(dir, newBase)

	w, err := cfg.newManifestWriterFunc(ctx, newPath)
	if err != nil {
		return nil, err
	}

	success := false
	defer func() {
		if !success {
			_ = w.Close()
			_ = os.Remove(newPath)
		}
	}()

	snap := vs.snapshotEdit()
	if err = w.Append(snap); err != nil {
		return nil, err
	}
	if err = w.Sync(); err != nil {
		return nil, err
	}
	if err = writeCurrent(ctx, dir, newBase); err != nil {
		return nil, err
	}

	success = true
	return w, nil
}

// maybeRotateManifest checks the current manifest size and triggers rotation if
// it exceeds the configured threshold. It swaps r.manifest and updates related
// fields atomically.
func (r *Rindb) maybeRotateManifest(ctx context.Context) error {
	if r.manifest == nil || r.manifest.Path() == "" {
		return nil
	}
	fi, err := os.Stat(r.manifest.Path())
	if err != nil {
		return err
	}
	if fi.Size() <= r.config.manifestSizeThreshold {
		return nil
	}

	r.ssTableManager.mu.Lock()
	mw, err := rotateManifest(ctx, r.config, r.versionSet, r.manifest)
	if err != nil {
		r.ssTableManager.mu.Unlock()
		return err
	}

	old := r.manifest
	r.manifest = mw
	r.ssTableManager.manifest = mw
	r.ssTableManager.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}
