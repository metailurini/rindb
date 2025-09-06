package rindb

//lint:file-ignore U1000 The table cache will be wired into the DB in a later step.

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// --- You already have this type in your project ---
// type SStable struct {
//  *FileSystem
//  SparseIndex SparseIndex
//  Bloom       *BloomFilter
// }

// --- typed errors ---
var (
	ErrClosed     = errors.New("tablecache: closed")
	ErrObsolete   = errors.New("tablecache: obsolete")
	ErrCorruption = errors.New("tablecache: corruption verified")
	ErrNotFound   = errors.New("tablecache: not found")
)

// tableKey uniquely identifies an sstable file within (possibly) multi-DB setups.
type tableKey struct {
	DBID    uint64
	FileNum uint64
}

// tableCacheOptions configures the cache.
type tableCacheOptions struct {
	// Total byte budget across all shards (actual bytes; see Measure).
	CapBytes int64
	// Number of shards (rounded up to power of two). 64 is a good default.
	Shards int

	// Open must fully open & initialize the SStable for this key (fd/mmap, parse).
	Open func(context.Context, tableKey) (*SStable, error)
	// Close must release all resources held by the SStable (fd/mmap/etc).
	Close func(*SStable) error
	// Measure returns (logicalBytes, actualBytes). If nil, (0,1) is used as a trivial weight.
	Measure func(*SStable) (int64, int64)
	// Verify (optional) checksums/consistency on first use; return ErrCorruption on failure.
	Verify func(*SStable) error
	// CorruptTTL defines how long a key stays quarantined after a corruption failure.
	CorruptTTL time.Duration

	// FDLimiter limits concurrent open FDs. If nil, no limit.
	FDLimiter FDLimiter

	// SLRU segment size ratio by bytes. If <=0 or >=1 → defaults to 0.25 (probation) / 0.75 (protected).
	ProbationFraction float64
}

// FDLimiter is a minimal semaphore interface (Acquire before Open, Release after Close).
type FDLimiter interface {
	Acquire(ctx context.Context) error
	Release()
}

// Handle wraps a live SStable + refcount + size accounting.
type Handle struct {
	Table *SStable

	refs          atomic.Int32 // pin/unpin (must be >0 when returned by Get/TryGet)
	evictWhenZero atomic.Bool  // set when obsolete/evicted
	closed        atomic.Bool  // finalizer guard; true after Close()

	logicalBytes int64
	actualBytes  int64

	closer  func(*SStable) error // injected from tableCacheOptions.Close
	onClose func()               // shard hook (metrics)
}

// Pin increments the refcount.
func (h *Handle) Pin() { h.refs.Add(1) }

// Unref drops a refcount; if it hits zero AND eviction/obsolete was requested,
// the underlying table is finally closed.
func (h *Handle) Unref() {
	if h.refs.Add(-1) == 0 && h.evictWhenZero.Load() {
		if h.closed.CompareAndSwap(false, true) {
			_ = h.closer(h.Table)
			if h.onClose != nil {
				h.onClose()
			}
		}
	}
}

// --- SLRU internals ---

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

type tableCache struct {
	shards    []*shard
	shardMask uint64 // len(shards)-1, power of two
	opt       tableCacheOptions

	// global metrics
	totalBytes atomic.Int64
}

// newTableCache creates a tableCache with SLRU + singleflight + byte budgeting.
func newTableCache(opt tableCacheOptions) *tableCache {
	if opt.Shards <= 0 {
		opt.Shards = 64
	}
	fr := opt.ProbationFraction
	if fr <= 0 || fr >= 1 {
		fr = 0.25
	}
	n := nextPow2(uint64(opt.Shards))
	c := &tableCache{opt: opt}
	shards := make([]*shard, n)
	per := int64(0)
	if opt.CapBytes > 0 {
		per = opt.CapBytes / int64(n)
	}
	for i := range shards {
		s := &shard{
			items:        make(map[tableKey]*entry, 256),
			corrupt:      make(map[tableKey]time.Time, 16),
			tombstone:    make(map[tableKey]struct{}, 16),
			capBytes:     per,
			parent:       c,
			probCapBytes: int64(float64(per) * fr),
		}
		s.protCapBytes = per - s.probCapBytes
		shards[i] = s
	}
	c.shards = shards
	c.shardMask = uint64(len(shards) - 1)
	return c
}

func (c *tableCache) shardFor(k tableKey) *shard {
	// Cheap 64-bit mix; shards is power-of-two so we can mask.
	h := (k.DBID * 11400714819323198485) ^ (k.FileNum * 14029467366897019727)
	return c.shards[h&c.shardMask]
}

// TryGet returns a pinned handle if present without doing any I/O.
// ok=false if not resident or tombstoned/quarantined.
func (c *tableCache) TryGet(k tableKey) (h *Handle, ok bool) {
	s := c.shardFor(k)
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, dead := s.tombstone[k]; dead {
		return nil, false
	}
	if exp, bad := s.corrupt[k]; bad {
		if time.Now().Before(exp) {
			return nil, false
		}
		delete(s.corrupt, k)
	}
	if e, ok := s.items[k]; ok {
		e.h.Pin()
		s.promoteOnHit(e)
		s.hits.Add(1)
		return e.h, true
	}
	return nil, false
}

// TryRef increments a ref on an existing key if present; returns false otherwise.
func (c *tableCache) TryRef(k tableKey) bool {
	_, ok := c.TryGet(k)
	return ok
}

// Get returns a pinned handle; caller MUST Unref() when done.
// On miss it opens exactly once per key (singleflight), then installs into SLRU.
// ctx is used for Open() and for optional FDLimiter acquisition.
func (c *tableCache) Get(ctx context.Context, k tableKey) (*Handle, error) {
	s := c.shardFor(k)

	// admission stopped?
	if s.stopAdmission.Load() {
		return nil, ErrClosed
	}

	// Fast path: map hit
	s.mu.Lock()
	// deny install if tombstoned (obsolete)
	if _, dead := s.tombstone[k]; dead {
		s.mu.Unlock()
		return nil, ErrObsolete
	}
	// deny if quarantined for corruption
	if exp, bad := s.corrupt[k]; bad {
		if time.Now().Before(exp) {
			s.mu.Unlock()
			return nil, ErrCorruption
		}
		// expired quarantine — drop it
		delete(s.corrupt, k)
	}
	if e, ok := s.items[k]; ok {
		e.h.Pin()
		s.promoteOnHit(e)
		s.hits.Add(1)
		s.mu.Unlock()
		return e.h, nil
	}
	s.mu.Unlock()

	// Miss: singleflight open (no locks while doing I/O).
	keyStr := fmt.Sprintf("%d/%d", k.DBID, k.FileNum)
	v, err, _ := s.flight.Do(keyStr, func() (any, error) {
		// FD limiter (optional)
		if c.opt.FDLimiter != nil {
			if err := c.opt.FDLimiter.Acquire(ctx); err != nil {
				return nil, err
			}
			defer c.opt.FDLimiter.Release()
		}
		t, err := c.opt.Open(ctx, k)
		if err != nil {
			return nil, err
		}
		s.opens.Add(1) // count successful open attempts

		// checksums on first use
		if c.opt.Verify != nil {
			if err := c.opt.Verify(t); err != nil {
				_ = c.opt.Close(t)
				return nil, ErrCorruption
			}
		}
		var logical, actual int64 = 0, 1
		if c.opt.Measure != nil {
			logical, actual = c.opt.Measure(t)
			if actual <= 0 {
				actual = 1
			}
		}
		h := &Handle{
			Table:        t,
			logicalBytes: logical,
			actualBytes:  actual,
			closer:       c.opt.Close,
		}
		h.refs.Store(1) // caller's ref
		return h, nil
	})
	if err != nil {
		// quarantine on corruption
		if errors.Is(err, ErrCorruption) {
			ttl := c.opt.CorruptTTL
			if ttl <= 0 {
				ttl = 5 * time.Minute
			}
			s.mu.Lock()
			s.corrupt[k] = time.Now().Add(ttl)
			s.mu.Unlock()
		}
		return nil, err
	}
	h := v.(*Handle)

	// Install (idempotent in case someone else won the race).
	s.mu.Lock()
	// Recheck admission stop or tombstone (race with Delete/Close):
	if s.stopAdmission.Load() {
		s.mu.Unlock()
		if h.closed.CompareAndSwap(false, true) {
			_ = c.opt.Close(h.Table)
			s.closes.Add(1)
		}
		return nil, ErrClosed
	}
	if _, dead := s.tombstone[k]; dead {
		s.mu.Unlock()
		if h.closed.CompareAndSwap(false, true) {
			_ = c.opt.Close(h.Table)
			s.closes.Add(1)
		}
		return nil, ErrObsolete
	}
	if existing, ok := s.items[k]; ok {
		// Another goroutine installed first. Use it.
		existing.h.Pin()
		s.promoteOnHit(existing)
		s.hits.Add(1)
		s.mu.Unlock()
		// Close duplicate we just opened.
		if h != existing.h && h.closed.CompareAndSwap(false, true) {
			_ = c.opt.Close(h.Table)
			s.closes.Add(1)
		}
		return existing.h, nil
	}

	e := &entry{key: k, h: h, seg: segProbation}
	// hook: when handle closes via Unref path, count closes
	h.onClose = func() { s.closes.Add(1) }

	e.elem = s.prob.PushFront(e)
	s.items[k] = e
	s.usedBytes += h.actualBytes
	s.probBytes += h.actualBytes
	c.totalBytes.Add(h.actualBytes)
	s.misses.Add(1)

	// Evict/demote to budget (may close victims outside the lock).
	s.evictOrDemoteLocked()
	s.mu.Unlock()
	return h, nil
}

// Delete marks a key obsolete and unlinks it from the cache immediately.
// Actual close happens immediately only if refcount hits zero.
func (c *tableCache) Delete(k tableKey) {
	s := c.shardFor(k)
	var toClose *Handle

	s.mu.Lock()
	// Set tombstone first to block racing admissions
	s.tombstone[k] = struct{}{}
	if e, ok := s.items[k]; ok {
		e.h.evictWhenZero.Store(true)
		s.unlink(e) // remove from SLRU
		delete(s.items, k)
		s.usedBytes -= e.h.actualBytes
		c.totalBytes.Add(-e.h.actualBytes)
		if e.h.refs.Load() == 0 {
			toClose = e.h
		}
	}
	s.mu.Unlock()

	if toClose != nil {
		s.closeNow(toClose)
	}
}

// PinKey marks a cached entry as pinned (immune to eviction).
func (c *tableCache) PinKey(k tableKey) bool {
	s := c.shardFor(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[k]; ok {
		e.pinned = true
		return true
	}
	return false
}

// UnpinKey removes the pin flag.
func (c *tableCache) UnpinKey(k tableKey) {
	s := c.shardFor(k)
	s.mu.Lock()
	if e, ok := s.items[k]; ok {
		e.pinned = false
	}
	s.mu.Unlock()
}

// Close stops admission and attempts a bounded drain.
// Any entries that reach ref=0 are closed; others will close when Unref() happens.
func (c *tableCache) Close(ctx context.Context, drainTimeout time.Duration) error {
	// stop admission on all shards
	for _, s := range c.shards {
		s.stopAdmission.Store(true)
	}

	// fast unlink all entries (non-blocking I/O outside locks)
	type hpair struct {
		s *shard
		h *Handle
	}
	var toClose []hpair
	var busy []*Handle

	for _, s := range c.shards {
		s.mu.Lock()
		for k, e := range s.items {
			_ = k // keep for clarity; we drop the map entry
			e.h.evictWhenZero.Store(true)
			s.unlink(e)
			delete(s.items, k)
			s.usedBytes -= e.h.actualBytes
			c.totalBytes.Add(-e.h.actualBytes)
			if e.h.refs.Load() == 0 {
				toClose = append(toClose, hpair{s: s, h: e.h})
			} else {
				busy = append(busy, e.h)
			}
		}
		s.mu.Unlock()
	}

	// close the cold ones immediately
	for _, p := range toClose {
		p.s.closeNow(p.h)
	}

	// bounded wait for busy ones to drain
	if drainTimeout <= 0 {
		return nil
	}
	deadline := time.Now().Add(drainTimeout)
	for time.Now().Before(deadline) {
		remaining := 0
		for _, h := range busy {
			if h.refs.Load() > 0 {
				remaining++
			}
		}
		if remaining == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil
}

// --- SLRU helpers (all require s.mu held unless stated) ---

func (s *shard) promoteOnHit(e *entry) {
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
	// Prefer tail of probation; fall back to tail of protected.
	if back := s.prob.Back(); back != nil {
		return back.Value.(*entry)
	}
	if back := s.prot.Back(); back != nil {
		return back.Value.(*entry)
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
func (s *shard) evictOrDemoteLocked() {
	if s.capBytes <= 0 {
		return
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
			break // nothing to evict; budget won't be met but we're out of candidates
		}
		if v.pinned {
			// skip pinned victims: keep them MRU-protected to avoid tight loops
			if v.seg == segProbation && v.elem != nil {
				s.prob.Remove(v.elem)
				s.probBytes -= v.h.actualBytes
				v.elem = s.prot.PushFront(v)
				v.seg = segProtected
				s.protBytes += v.h.actualBytes
			} else if v.seg == segProtected && v.elem != nil {
				s.prot.MoveToFront(v.elem)
			}
			continue
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
	}

	if len(toClose) > 0 {
		s.mu.Unlock()
		for _, h := range toClose {
			s.closeNow(h)
		}
		s.mu.Lock()
	}
}

func (s *shard) closeNow(h *Handle) {
	if h.closed.CompareAndSwap(false, true) {
		_ = h.closer(h.Table)
		s.closes.Add(1)
	}
}

// --- Stats & utils ---

type tableCacheStats struct {
	Shards int

	// bytes
	UsedBytes int64 // sum across shards (actual bytes)
	CapBytes  int64

	// counters (sum across shards)
	Hits, Misses, Opens, Closes, Evicts, Promotions int64
}

func (c *tableCache) Stats() tableCacheStats {
	var st tableCacheStats
	st.Shards = len(c.shards)
	st.UsedBytes = c.totalBytes.Load()
	for _, s := range c.shards {
		st.CapBytes += s.capBytes
		st.Hits += s.hits.Load()
		st.Misses += s.misses.Load()
		st.Opens += s.opens.Load()
		st.Closes += s.closes.Load()
		st.Evicts += s.evicts.Load()
		st.Promotions += s.promotions.Load()
	}
	return st
}

func nextPow2(x uint64) int {
	if x <= 1 {
		return 1
	}
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return int(x)
}
