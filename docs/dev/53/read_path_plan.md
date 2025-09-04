# VersionSet Read Path Plan

Now that `VersionSet` tracks SSTables, migrate lookup helpers to operate on its metadata rather than legacy linked lists. The prose-to-code ratio is roughly 3:7.

## Plan
- Iterate `VersionSet.Levels` and return opened SSTables overlapping a key range.
- Track open files by number instead of pointer identity.
- Drop linked-list based bookkeeping.

```go
// GetRelevantSSTables gathers SSTables whose ranges overlap [start, end].
func (h *SSTableManager) GetRelevantSSTables(ctx context.Context, start, end InternalKey) ([]*SSTable, error) {
    h.mu.RLock()
    defer h.mu.RUnlock()
    var out []*SSTable
    for _, level := range h.versionSet.Levels {
        for _, f := range level {
            if !end.Before(f.Smallest) && !start.After(f.Largest) {
                sst, err := h.openByNumber(ctx, f.Number)
                if err != nil {
                    for _, s := range out {
                        _ = s.Close()
                    }
                    return nil, err
                }
                out = append(out, sst)
            }
        }
    }
    return out, nil
}

// searchKey walks the candidates and stops at the first match.
func (h *SSTableManager) searchKey(ctx context.Context, key InternalKey) ([]byte, error) {
    ssts, err := h.GetRelevantSSTables(ctx, key, key)
    if err != nil {
        return nil, err
    }
    for _, sst := range ssts {
        if v, err := sst.Lookup(key); err == nil {
            return v, nil
        }
    }
    return nil, errKeyNotFound
}
```

## Follow Ups
- Legacy `openedFs` tracking has been removed; callers close file systems directly.
- Tests seed `VersionSet` fixtures and assert lookups by file number.
