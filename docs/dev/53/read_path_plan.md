# VersionSet Read Path Plan

Migrate lookup helpers to operate on `VersionSet` metadata rather than legacy linked lists. The prose-to-code ratio is roughly 3:7.

## Plan
- Iterate `VersionSet.Levels` and return file numbers overlapping a key range.
- Track open files by number instead of pointer identity.
- Drop linked-list based bookkeeping.

```go
// GetRelevantSSTables gathers file numbers whose ranges overlap [start, end].
func (h *SSTableManager) GetRelevantSSTables(start, end InternalKey) []uint64 {
    h.mu.RLock()
    defer h.mu.RUnlock()
    var out []uint64
    for _, level := range h.versionSet.Levels {
        for _, f := range level {
            if !end.Before(f.Smallest) && !start.After(f.Largest) {
                out = append(out, f.Number)
            }
        }
    }
    return out
}

// searchKey walks the candidates and stops at the first match.
func (h *SSTableManager) searchKey(ctx context.Context, key InternalKey) ([]byte, error) {
    for _, num := range h.GetRelevantSSTables(key, key) {
        fs := h.openByNumber(num)
        if v, ok := fs.Lookup(key); ok {
            return v, nil
        }
    }
    return nil, errKeyNotFound
}
```

## Follow Ups
- `removeOpenedFS` switches to `map[uint64]*FileSystem`.
- Tests seed `VersionSet` fixtures and assert lookups by file number.
