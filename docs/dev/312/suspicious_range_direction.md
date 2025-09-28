# Range iteration directional audit

## Suspicious logic

- `sstable_iteration.go`: `sstableIRange.Prev` surfaces the record found at the previous physical offset without reapplying the original `[start, end]` window or the `seq` ceiling. When walking backwards after a forward scan has skipped entries (for example because their sequence exceeds the caller's snapshot), `Prev` can reintroduce those filtered versions, leaking state from outside the requested snapshot into descending scans.
- `range.go`: `RangeIterator.preparePrev` and `prepareNext` assume that every child iterator enforces the snapshot bounds in both directions. They only filter tombstones/duplicates, so if an underlying iterator leaks an out-of-range version during `Prev` (as in `sstableIRange` above) the user-facing iterator cannot defend against it. This makes the backward direction particularly risky when multiple layers contribute to the merged view.

