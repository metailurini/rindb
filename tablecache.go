package rindb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultCacheQuarantineTTL   = 5 * time.Minute
	defaultCorruptMapCapacity   = 16
	defaultShardItemCapacity    = 256
	defaultTombstoneMapCapacity = 16
)

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
	// TombstoneTTL defines how long a key stays tombstoned after Delete.
	TombstoneTTL time.Duration

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

// noopFDLimiter implements FDLimiter without enforcing any limit.
// It is used as the default to make the absence of FD limiting explicit.
type noopFDLimiter struct{}

func (noopFDLimiter) Acquire(context.Context) error { return nil }
func (noopFDLimiter) Release()                      {}

// tableCacheEntry wraps a live SStable along with refcount and size accounting.
type tableCacheEntry struct {
	Table *SStable

	refs          atomic.Int32 // pin/unpin (must be >0 when returned by Get/TryGet)
	evictWhenZero atomic.Bool  // set when obsolete/evicted
	closed        atomic.Bool  // finalizer guard; true after Close()

	logicalBytes int64
	actualBytes  int64

	closer  func(*SStable) error   // injected from tableCacheOptions.Close
	onClose atomic.Pointer[func()] // shard hook (metrics)
}

// pin increments the refcount.
func (e *tableCacheEntry) pin() { e.refs.Add(1) }

// unref drops a refcount; if it hits zero AND eviction/obsolete was requested,
// the underlying table is finally closed.
func (e *tableCacheEntry) unref() {
	if e.refs.Add(-1) == 0 && e.evictWhenZero.Load() {
		if e.closed.CompareAndSwap(false, true) {
			_ = e.closer(e.Table)
			if f := e.onClose.Load(); f != nil {
				(*f)()
			}
		}
	}
}

// release marks the entry for closure and drops a refcount. The underlying
// resources are released when the reference count reaches zero.
func (e *tableCacheEntry) release() {
	e.evictWhenZero.Store(true)
	e.unref()
}

// --- SLRU internals ---

type tableCache struct {
	shards    []*shard
	shardMask uint64 // len(shards)-1, power of two
	opt       tableCacheOptions

	// global metrics
	totalBytes atomic.Int64
}

// newTableCache creates a tableCache with SLRU + singleflight + byte budgeting.
func newTableCache(opt tableCacheOptions) *tableCache {
	fr := opt.ProbationFraction
	n := nextPow2(uint64(opt.Shards))
	c := &tableCache{opt: opt}
	shards := make([]*shard, n)
	per := int64(0)
	if opt.CapBytes > 0 {
		per = opt.CapBytes / int64(n)
	}
	for i := range shards {
		s := &shard{
			items:        make(map[tableKey]*entry, defaultShardItemCapacity),
			corrupt:      make(map[tableKey]time.Time, defaultCorruptMapCapacity),
			tombstone:    make(map[tableKey]time.Time, defaultTombstoneMapCapacity),
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

// shardFor deterministically assigns a table key to a shard.
//
// The magic constants below come from the SplitMix64 finalizer. Multiplying
// the DB and file numbers by different odd, well-distributed 64-bit values and
// XOR'ing them together cheaply mixes the bits without pulling in an external
// hash library. Because the number of shards is always a power of two, we can
// mask the low bits to pick the shard instead of using modulo.
func (c *tableCache) shardFor(k tableKey) *shard {
	h := (k.DBID * 11400714819323198485) ^ (k.FileNum * 14029467366897019727)
	return c.shards[h&c.shardMask]
}

// tryGet returns a pinned TableCacheEntry if present without doing any I/O.
// ok=false if not resident or tombstoned/quarantined.
func (c *tableCache) tryGet(ctx context.Context, k tableKey) (entry *tableCacheEntry, ok bool) {
	s := c.shardFor(k)
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, _ := s.getEntryLocked(k); e != nil {
		e.entry.pin()
		s.promoteOnHit(ctx, e)
		s.hits.Add(1)
		cacheHits.Add(ctx, 1)
		return e.entry, true
	}
	s.misses.Add(1)
	cacheMisses.Add(ctx, 1)
	return nil, false
}

// tryRef returns true if the key is resident in the cache without updating
// hit/miss counters or promoting the entry.
func (c *tableCache) tryRef(k tableKey) bool {
	s := c.shardFor(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	e, _ := s.getEntryLocked(k)
	return e != nil
}

// get returns a pinned TableCacheEntry; caller MUST Unref() when done.
// On miss it opens exactly once per key (singleflight), then installs into SLRU.
// ctx is used for Open() and for optional FDLimiter acquisition.
func (c *tableCache) get(ctx context.Context, k tableKey) (*tableCacheEntry, error) {
	s := c.shardFor(k)

	// admission stopped?
	if s.stopAdmission.Load() {
		return nil, ErrClosed
	}

	// Fast path: map hit
	s.mu.Lock()
	ent, lookupErr := s.getEntryLocked(k)
	if lookupErr != nil {
		s.mu.Unlock()
		return nil, lookupErr
	}
	if ent != nil {
		ent.entry.pin()
		s.promoteOnHit(ctx, ent)
		s.hits.Add(1)
		cacheHits.Add(ctx, 1)
		s.mu.Unlock()
		return ent.entry, nil
	}
	s.mu.Unlock()

	// quick ctx check before starting the flight
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Miss: singleflight open (no locks while doing I/O).
	keyStr := fmt.Sprintf("%d/%d", k.DBID, k.FileNum)
	ch := s.flight.DoChan(keyStr, func() (any, error) {
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
		cacheOpens.Add(ctx, 1)

		// checksums on first use
		if c.opt.Verify != nil {
			if err := c.opt.Verify(t); err != nil {
				if cerr := c.opt.Close(t); cerr != nil {
					_ = cerr // close error ignored
				}
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
		entry := &tableCacheEntry{
			Table:        t,
			logicalBytes: logical,
			actualBytes:  actual,
			closer:       c.opt.Close,
		}
		entry.refs.Store(1) // caller's ref
		return entry, nil
	})

	var (
		v   any
		err error
	)
	select {
	case res := <-ch:
		v, err = res.Val, res.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err != nil {
		// quarantine on corruption
		if errors.Is(err, ErrCorruption) {
			ttl := c.opt.CorruptTTL
			if ttl <= 0 {
				ttl = defaultCacheQuarantineTTL
			}
			s.mu.Lock()
			s.corrupt[k] = time.Now().Add(ttl)
			s.mu.Unlock()
		}
		return nil, err
	}
	ce := v.(*tableCacheEntry)

	// Install (idempotent in case someone else won the race).
	s.mu.Lock()
	// Recheck admission stop or tombstone (race with Delete/Close):
	if s.stopAdmission.Load() {
		s.mu.Unlock()
		if ce.closed.CompareAndSwap(false, true) {
			_ = c.opt.Close(ce.Table)
			s.closes.Add(1)
			cacheCloses.Add(ctx, 1)
		}
		return nil, ErrClosed
	}
	if s.isTombstoned(k) {
		s.mu.Unlock()
		if ce.closed.CompareAndSwap(false, true) {
			_ = c.opt.Close(ce.Table)
			s.closes.Add(1)
			cacheCloses.Add(ctx, 1)
		}
		return nil, ErrObsolete
	}
	if existing, ok := s.items[k]; ok {
		// Another goroutine installed first. Use it.
		existing.entry.pin()
		s.promoteOnHit(ctx, existing)
		s.hits.Add(1)
		cacheHits.Add(ctx, 1)
		s.mu.Unlock()
		// Close duplicate we just opened.
		if ce != existing.entry && ce.closed.CompareAndSwap(false, true) {
			_ = c.opt.Close(ce.Table)
			s.closes.Add(1)
			cacheCloses.Add(ctx, 1)
		}
		return existing.entry, nil
	}

	e := &entry{key: k, entry: ce, seg: segProbation}
	// hook: when entry closes via Unref path, count closes
	fn := func() {
		s.closes.Add(1)
		cacheCloses.Add(ctx, 1)
	}
	ce.onClose.Store(&fn)

	e.elem = s.prob.PushFront(e)
	s.items[k] = e
	s.usedBytes += ce.actualBytes
	s.probBytes += ce.actualBytes
	c.totalBytes.Add(ce.actualBytes)
	s.misses.Add(1)
	cacheMisses.Add(ctx, 1)

	// Evict/demote to budget (may close victims outside the lock).
	if !s.evictOrDemoteLocked(ctx, e) {
		// Entry was dropped due to full pinned cache; the table remains valid
		// but won't be reachable through the cache and will close on Unref().
	}
	s.mu.Unlock()
	return ce, nil
}

// delete marks a key obsolete and unlinks it from the cache immediately.
// The tombstone expires after TombstoneTTL. Actual close happens immediately
// only if the refcount hits zero.
func (c *tableCache) delete(ctx context.Context, k tableKey) {
	s := c.shardFor(k)
	var toClose *tableCacheEntry

	s.mu.Lock()
	// Set tombstone first to block racing admissions
	ttl := c.opt.TombstoneTTL
	if ttl <= 0 {
		ttl = defaultCacheQuarantineTTL
	}
	s.tombstone[k] = time.Now().Add(ttl)
	delete(s.corrupt, k)
	if e, ok := s.items[k]; ok {
		e.entry.evictWhenZero.Store(true)
		s.unlink(e) // remove from SLRU
		delete(s.items, k)
		s.usedBytes -= e.entry.actualBytes
		c.totalBytes.Add(-e.entry.actualBytes)
		if e.entry.refs.Load() == 0 {
			toClose = e.entry
		}
	}
	s.mu.Unlock()

	if toClose != nil {
		s.closeNow(ctx, toClose)
	}
}

// pinKey marks a cached entry as pinned (immune to eviction).
func (c *tableCache) pinKey(k tableKey) bool {
	s := c.shardFor(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[k]; ok {
		e.pinned = true
		return true
	}
	return false
}

// unpinKey removes the pin flag and enforces the cache's capacity.
func (c *tableCache) unpinKey(ctx context.Context, k tableKey) {
	s := c.shardFor(k)
	s.mu.Lock()
	if e, ok := s.items[k]; ok {
		e.pinned = false
		s.evictOrDemoteLocked(ctx, nil)
	}
	s.mu.Unlock()
}

// close stops admission and attempts a bounded drain.
// Any entries that reach ref=0 are closed; others will close when Unref() happens.
func (c *tableCache) close(ctx context.Context, drainTimeout time.Duration) error {
	// stop admission on all shards
	for _, s := range c.shards {
		s.stopAdmission.Store(true)
	}

	// fast unlink all entries (non-blocking I/O outside locks)
	type entryPair struct {
		s     *shard
		entry *tableCacheEntry
	}
	var toClose []entryPair
	var busy []*tableCacheEntry

	for _, s := range c.shards {
		s.mu.Lock()
		for k, e := range s.items {
			e.entry.evictWhenZero.Store(true)
			s.unlink(e)
			delete(s.items, k)
			s.usedBytes -= e.entry.actualBytes
			c.totalBytes.Add(-e.entry.actualBytes)
			if e.entry.refs.Load() == 0 {
				toClose = append(toClose, entryPair{s: s, entry: e.entry})
			} else {
				busy = append(busy, e.entry)
			}
		}
		s.mu.Unlock()
	}

	// close the cold ones immediately
	for _, p := range toClose {
		p.s.closeNow(ctx, p.entry)
	}

	// bounded wait for busy ones to drain
	if drainTimeout <= 0 || len(busy) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	for _, entry := range busy {
		wg.Add(1)
		var once sync.Once
		prev := entry.onClose.Load()
		for {
			wrapper := func() {
				if prev != nil {
					(*prev)()
				}
				once.Do(func() { wg.Done() })
			}
			if entry.onClose.CompareAndSwap(prev, &wrapper) {
				break
			}
			prev = entry.onClose.Load()
		}
		if entry.closed.Load() {
			once.Do(func() { wg.Done() })
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(drainTimeout):
		remaining := 0
		for _, entry := range busy {
			if entry.refs.Load() > 0 {
				remaining++
			}
		}
		if remaining > 0 {
			return fmt.Errorf("%d entries still busy after timeout", remaining)
		}
		return nil
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

func (c *tableCache) stats() tableCacheStats {
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

/*
nextPow2 returns the next power of two >= x, or x if it's already a power of two.

| Input (`x`) | Output | Explanation                                |
| ----------- | ------ | ------------------------------------------ |
| 0           | 1      | The smallest power of 2 is 1.              |
| 1           | 1      | 1 is 2⁰, which is a power of 2.            |
| 2           | 2      | 2 is 2¹, which is a power of 2.            |
| 3           | 4      | The next power of 2 after 3 is 4 (2²).     |
| 7           | 8      | The next power of 2 after 7 is 8 (2³).     |
| 8           | 8      | 8 is 2³, which is a power of 2.            |
| 15          | 16     | The next power of 2 after 15 is 16 (2⁴).   |
| 17          | 32     | The next power of 2 after 17 is 32 (2⁵).   |
| 100         | 128    | The next power of 2 after 100 is 128 (2⁷). |
*/
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
