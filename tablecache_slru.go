package rindb

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

type segment uint8

const (
	segNone segment = iota
	segProbation
	segProtected
)

type entry struct {
	key    tableKey
	entry  *tableCacheEntry
	seg    segment
	elem   *list.Element // element in prob/prot list
	pinned bool          // pin top-N / critical tables
}

type shard struct {
	mu    sync.Mutex
	items map[tableKey]*entry

	// SLRU segments: probation & protected
	prob list.List
	prot list.List

	// per-segment soft caps (by bytes)
	probCapBytes int64
	protCapBytes int64

	// per-segment tracked usage (actual bytes)
	probBytes int64
	protBytes int64

	// tracked usage (actual bytes) of entries in lists/map
	usedBytes int64
	capBytes  int64

	// admission stop — set during Close() to reject new installs
	stopAdmission atomic.Bool

	// corrupt quarantine: key -> expiry
	corrupt map[tableKey]time.Time

	// tombstones to prevent re-admission after Delete/obsolete
	tombstone map[tableKey]time.Time

	// per-shard singleflight for open de-dup
	flight singleflight.Group

	// backref to parent (for global byte accounting)
	parent *tableCache

	// metrics (local)
	hits, misses, opens, closes, evicts, promotions atomic.Int64
}

// --- SLRU helpers (all require s.mu held unless stated) ---

// isTombstoned checks if a key has an active tombstone. It prunes expired
// tombstones. s.mu must be held by the caller.
func (s *shard) isTombstoned(k tableKey) bool {
	if exp, dead := s.tombstone[k]; dead {
		if time.Now().Before(exp) {
			return true
		}
		delete(s.tombstone, k)
	}
	return false
}

// getEntryLocked finds an entry in the cache, handling tombstones, corruption,
// and closed entries. It must be called with s.mu held.
func (s *shard) getEntryLocked(k tableKey) (*entry, error) {
	if s.isTombstoned(k) {
		return nil, ErrObsolete
	}
	if exp, bad := s.corrupt[k]; bad {
		if time.Now().Before(exp) {
			return nil, ErrCorruption
		}
		delete(s.corrupt, k)
	}
	e, ok := s.items[k]
	if !ok {
		return nil, nil
	}
	if e.entry.closed.Load() {
		s.unlink(e)
		delete(s.items, k)
		s.usedBytes -= e.entry.actualBytes
		s.parent.totalBytes.Add(-e.entry.actualBytes)
		return nil, nil
	}
	return e, nil
}

func (s *shard) promoteOnHit(ctx context.Context, e *entry) {
	switch e.seg {
	case segProbation:
		// second hit → promote to protected
		if e.elem != nil {
			s.prob.Remove(e.elem)
		}
		s.probBytes -= e.entry.actualBytes
		e.elem = s.prot.PushFront(e)
		e.seg = segProtected
		s.protBytes += e.entry.actualBytes
		s.promotions.Add(1)
		cachePromotions.Add(ctx, 1)

		// enforce protected cap via demotion if necessary
		for s.segmentBytes(segProtected) > s.protCapBytes {
			if tail := s.prot.Back(); tail != nil {
				dem := tail.Value.(*entry)
				if dem.elem != nil {
					s.prot.Remove(dem.elem)
				}
				s.protBytes -= dem.entry.actualBytes
				dem.elem = s.prob.PushFront(dem)
				dem.seg = segProbation
				s.probBytes += dem.entry.actualBytes
			} else {
				break
			}
		}
	case segProtected:
		// move to MRU
		if e.elem != nil {
			s.prot.MoveToFront(e.elem)
		}
	}
}

func (s *shard) unlink(e *entry) {
	switch e.seg {
	case segProbation:
		if e.elem != nil {
			s.prob.Remove(e.elem)
		}
		s.probBytes -= e.entry.actualBytes
	case segProtected:
		if e.elem != nil {
			s.prot.Remove(e.elem)
		}
		s.protBytes -= e.entry.actualBytes
	}
	e.elem = nil
	e.seg = segNone
}

func (s *shard) chooseVictim() *entry {
	// Prefer tail of probation; fall back to tail of protected, skipping pinned entries.
	for e := s.prob.Back(); e != nil; e = e.Prev() {
		if ent := e.Value.(*entry); !ent.pinned {
			return ent
		}
	}
	for e := s.prot.Back(); e != nil; e = e.Prev() {
		if ent := e.Value.(*entry); !ent.pinned {
			return ent
		}
	}
	return nil
}

// segmentBytes returns the tracked byte usage for the requested segment.
func (s *shard) segmentBytes(seg segment) int64 {
	switch seg {
	case segProbation:
		return s.probBytes
	case segProtected:
		return s.protBytes
	default:
		return 0
	}
}

func (s *shard) evictEntryLocked(ctx context.Context, e *entry, toClose *[]*tableCacheEntry) {
	s.unlink(e)
	delete(s.items, e.key)
	s.usedBytes -= e.entry.actualBytes
	s.parent.totalBytes.Add(-e.entry.actualBytes)

	if e.entry.refs.Load() > 0 {
		e.entry.evictWhenZero.Store(true)
	} else {
		*toClose = append(*toClose, e.entry)
	}
	s.evicts.Add(1)
	cacheEvicts.Add(ctx, 1)
}

// evictOrDemoteLocked ensures segment splits and capBytes.
// It closes unpinned, zero-ref victims outside the lock to avoid blocking.
func (s *shard) evictOrDemoteLocked(ctx context.Context, recent *entry) bool {
	if s.capBytes <= 0 {
		return true
	}
	// First, if protected exceeds its cap, demote from protected tail into probation head.
	for s.segmentBytes(segProtected) > s.protCapBytes {
		if tail := s.prot.Back(); tail != nil {
			dem := tail.Value.(*entry)
			if dem.elem != nil {
				s.prot.Remove(dem.elem)
			}
			s.protBytes -= dem.entry.actualBytes
			dem.elem = s.prob.PushFront(dem)
			dem.seg = segProbation
			s.probBytes += dem.entry.actualBytes
		} else {
			break
		}
	}

	// Then evict until within total cap.
	var toClose []*tableCacheEntry
	for s.usedBytes > s.capBytes {
		v := s.chooseVictim()
		if v == nil {
			// No evictable entries remain (all pinned); drop the recently added entry.
			if recent != nil && recent.seg != segNone {
				s.evictEntryLocked(ctx, recent, &toClose)
			}
			break
		}
		// Remove from SLRU + map; stop future pins; free budget immediately (cache residency accounting).
		s.evictEntryLocked(ctx, v, &toClose)
	}

	if len(toClose) > 0 {
		s.mu.Unlock()
		for _, entry := range toClose {
			s.closeNow(ctx, entry)
		}
		s.mu.Lock()
	}

	if recent != nil && recent.seg == segNone {
		return false
	}
	return true
}

func (s *shard) closeNow(ctx context.Context, entry *tableCacheEntry) {
	if entry.closed.CompareAndSwap(false, true) {
		_ = entry.closer(entry.Table)
		s.closes.Add(1)
		cacheCloses.Add(ctx, 1)
	}
}
