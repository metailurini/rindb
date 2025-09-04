package rindb

import "fmt"

// FileMeta holds metadata about an SSTable file.
type FileMeta struct {
	Number   uint64
	Level    int
	Smallest InternalKey
	Largest  InternalKey
	Size     uint64
	SeqLo    uint64
	SeqHi    uint64
}

// DeletedFileMeta references a file to remove.
type DeletedFileMeta struct {
	Level  int
	Number uint64
}

// VersionEdit describes a change to the VersionSet.
type VersionEdit struct {
	ComparatorName string
	LastSequence   uint64
	NextFileNumber uint64
	LogNumber      uint64
	PrevLogNumber  uint64

	AddFiles    []FileMeta
	DeleteFiles []DeletedFileMeta
}

// VersionSet represents the in-memory state of levels and file numbering.
type VersionSet struct {
	Levels         [][]FileMeta
	Comparator     string
	NextFileNumber uint64
	LogNumber      uint64
	PrevLogNumber  uint64
	LastSequence   uint64
}

func (vs *VersionSet) ensureLevel(level int) {
	for len(vs.Levels) <= level {
		vs.Levels = append(vs.Levels, nil)
	}
}

// coalesceNonZero returns newVal if it's not the zero value for its type,
// otherwise returns existing.
func coalesceNonZero[T comparable](existing, newVal T) T {
	var zero T
	if newVal != zero {
		return newVal
	}
	return existing
}

// Apply applies the VersionEdit to the VersionSet.
func (e VersionEdit) Apply(vs *VersionSet) error {
	if e.ComparatorName != "" {
		if vs.Comparator != "" && vs.Comparator != e.ComparatorName {
			return fmt.Errorf("comparator mismatch: have %s want %s", vs.Comparator, e.ComparatorName)
		}
		vs.Comparator = e.ComparatorName
	}
	vs.LastSequence = coalesceNonZero(vs.LastSequence, e.LastSequence)
	vs.NextFileNumber = coalesceNonZero(vs.NextFileNumber, e.NextFileNumber)
	vs.LogNumber = coalesceNonZero(vs.LogNumber, e.LogNumber)
	vs.PrevLogNumber = coalesceNonZero(vs.PrevLogNumber, e.PrevLogNumber)
	for _, f := range e.AddFiles {
		vs.ensureLevel(f.Level)
		vs.Levels[f.Level] = append(vs.Levels[f.Level], f)
	}
	if len(e.DeleteFiles) > 0 {
		deletionsByLevel := make(map[int]map[uint64]struct{})
		for _, d := range e.DeleteFiles {
			if _, ok := deletionsByLevel[d.Level]; !ok {
				deletionsByLevel[d.Level] = make(map[uint64]struct{})
			}
			deletionsByLevel[d.Level][d.Number] = struct{}{}
		}

		for level, toDelete := range deletionsByLevel {
			if level >= len(vs.Levels) {
				continue
			}
			files := vs.Levels[level]
			filtered := files[:0]
			for _, f := range files {
				if _, ok := toDelete[f.Number]; !ok {
					filtered = append(filtered, f)
				}
			}
			vs.Levels[level] = filtered
		}
	}
	return nil
}
