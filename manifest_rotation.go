package rindb

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

	r.SSTableManager.mu.Lock()
	mw, err := rotateManifest(ctx, r.config, r.versionSet, r.manifest)
	if err != nil {
		r.SSTableManager.mu.Unlock()
		return err
	}

	old := r.manifest
	r.manifest = mw
	r.SSTableManager.manifest = mw
	r.SSTableManager.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return cleanupManifests(r.config.databaseDir, 1)
}

// cleanupManifests removes old MANIFEST files, keeping the one referenced by
// CURRENT and up to keepRecent additional files.
func cleanupManifests(dir string, keepRecent int) error {
	data, err := os.ReadFile(filepath.Join(dir, CurrentFile))
	if err != nil {
		return err
	}
	currName := strings.TrimSpace(string(data))
	currNum, err := manifestNum(currName)
	if err != nil {
		return err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	type mInfo struct {
		name string
		num  int
	}
	var manifests []mInfo
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "MANIFEST-") {
			continue
		}
		n, err := manifestNum(e.Name())
		if err == nil && n != currNum {
			manifests = append(manifests, mInfo{name: e.Name(), num: n})
		}
	}
	sort.Slice(manifests, func(i, j int) bool { return manifests[i].num > manifests[j].num })
	for i, m := range manifests {
		if i < keepRecent {
			continue
		}
		_ = os.Remove(filepath.Join(dir, m.name))
	}
	return nil
}
