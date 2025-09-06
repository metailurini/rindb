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
	h      *Handle
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
	tombstone map[tableKey]struct{}

	// per-shard singleflight for open de-dup
	flight singleflight.Group

	// backref to parent (for global byte accounting)
	parent *tableCache

	// metrics (local)
	hits, misses, opens, closes, evicts, promotions atomic.Int64
}

// --- SLRU helpers (all require s.mu held unless stated) ---

func (s *shard) promoteOnHit(ctx context.Context, e *entry) {
	switch e.seg {
	case segProbation:
		// second hit → promote to protected
		if e.elem != nil {
			s.prob.Remove(e.elem)
		}
		s.probBytes -= e.h.actualBytes
		e.elem = s.prot.PushFront(e)
		e.seg = segProtected
		s.protBytes += e.h.actualBytes
		s.promotions.Add(1)
		cachePromotions.Add(ctx, 1)

		// enforce protected cap via demotion if necessary
		for s.segmentBytes(segProtected) > s.protCapBytes {
			if tail := s.prot.Back(); tail != nil {
				dem := tail.Value.(*entry)
				if dem.elem != nil {
					s.prot.Remove(dem.elem)
				}
				s.protBytes -= dem.h.actualBytes
				dem.elem = s.prob.PushFront(dem)
				dem.seg = segProbation
				s.probBytes += dem.h.actualBytes
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
		s.probBytes -= e.h.actualBytes
	case segProtected:
		if e.elem != nil {
			s.prot.Remove(e.elem)
		}
		s.protBytes -= e.h.actualBytes
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
			s.protBytes -= dem.h.actualBytes
			dem.elem = s.prob.PushFront(dem)
			dem.seg = segProbation
			s.probBytes += dem.h.actualBytes
		} else {
			break
		}
	}

	// Then evict until within total cap.
	var toClose []*Handle
	for s.usedBytes > s.capBytes {
		v := s.chooseVictim()
		if v == nil {
			// No evictable entries remain (all pinned); drop the recently added entry.
			if recent != nil && recent.seg != segNone {
				s.unlink(recent)
				delete(s.items, recent.key)
				s.usedBytes -= recent.h.actualBytes
				s.parent.totalBytes.Add(-recent.h.actualBytes)
				if recent.h.refs.Load() > 0 {
					recent.h.evictWhenZero.Store(true)
				} else {
					toClose = append(toClose, recent.h)
				}
				s.evicts.Add(1)
				cacheEvicts.Add(ctx, 1)
			}
			break
		}
		// Remove from SLRU + map; stop future pins; free budget immediately (cache residency accounting).
		s.unlink(v)
		delete(s.items, v.key)
		s.usedBytes -= v.h.actualBytes
		s.parent.totalBytes.Add(-v.h.actualBytes)

		if v.h.refs.Load() > 0 {
			// Busy: mark for close later when refs drain.
			v.h.evictWhenZero.Store(true)
		} else {
			// Cold: close now (outside lock).
			toClose = append(toClose, v.h)
		}
		s.evicts.Add(1)
		cacheEvicts.Add(ctx, 1)
	}

	if len(toClose) > 0 {
		s.mu.Unlock()
		for _, h := range toClose {
			s.closeNow(ctx, h)
		}
		s.mu.Lock()
	}

	if recent != nil && recent.seg == segNone {
		return false
	}
	return true
}

func (s *shard) closeNow(ctx context.Context, h *Handle) {
	if h.closed.CompareAndSwap(false, true) {
		_ = h.closer(h.Table)
		s.closes.Add(1)
		cacheCloses.Add(ctx, 1)
	}
}
