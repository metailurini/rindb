# SSTable Cache Integration Plan

The complete reference implementation used to inform this plan is available in [`tablecache_sample.go`](tablecache_sample.go).

## Plan
1. Add a sharded SLRU table cache in this package (no new subpackage).
   - define typed errors, `tableKey`, `Options`, `FDLimiter`, and core structs
   - implement `TableCacheEntry`, `shard`, singleflight-backed `Get`, and eviction helpers
   - expose `Stats`, `Delete`, and graceful `Close`

2. Wire the cache into `ssTableManager`.
   - instantiate the cache in `InitSSTableManager`
   - use it inside `openByNumber` and drain it during manager shutdown

3. Replace direct SSTable usage with cache operations.
   - switch readers and iterators to `cache.Get`
   - call `cache.Delete`, `PinKey`, and `UnpinKey` during compaction and metadata phases
   - surface `ErrObsolete` and `ErrCorruption` to callers

4. Expose configuration and observability.
   - introduce `CacheBytes` and `CacheShards` fields plus CLI flags
   - integrate cache metrics with telemetry and expose `Stats` via a DB or manager method
   - document configuration and usage

5. Test and document the feature.
   - unit tests for hit/miss, eviction, pinning, and corruption handling
   - integration tests covering compaction and WAL recovery
   - run `make check`, `make test`, and `make test-integration-smoke` before merging

## Code Sketch
```go
package rindb

import (
    "context"
    "time"
)

// tableCache mirrors the reference implementation placed in this package.
type tableCache struct { /* ... */ }

// ssTableManager embeds the cache to reuse opened tables.
type ssTableManager struct {
    cache  *tableCache
    config Config
    // existing fields omitted
}

// InitSSTableManager wires the cache using configuration values.
func InitSSTableManager(ctx context.Context, cfg Config, vs *versionSet, mw manifestWriter) (*ssTableManager, error) {
    tc := newTableCache(options{
        CapBytes: cfg.CacheBytes,
        Shards:   cfg.CacheShards,
        Open: func(ctx context.Context, k tableKey) (*SStable, error) {
            fs, err := OpenExistingFS(ctx, sstPath(k.FileNum))
            if err != nil {
                return nil, err
            }
            sst, err := NewSSTable(ctx, cfg, fs)
            if err != nil {
                _ = fs.Close()
                return nil, err
            }
            return &sst, nil
        },
        Close: func(t *SStable) error { return t.Close() },
    })

    m := &ssTableManager{
        cache:  tc,
        config: cfg,
        // other fields...
    }
    return m, nil
}

// openByNumber now fetches tables from the cache.
func (m *ssTableManager) openByNumber(ctx context.Context, num uint64) (*SStable, error) {
    h, err := m.cache.Get(ctx, tableKey{DBID: m.config.dbID, FileNum: num})
    if err != nil {
        return nil, err
    }
    return h.Table, nil
}

// Close drains the cache during shutdown.
func (m *ssTableManager) Close(ctx context.Context) error {
    return m.cache.Close(ctx, 5*time.Second)
}
```
