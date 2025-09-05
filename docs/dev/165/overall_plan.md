# Public API and Documentation Plan

The goal is to expose a stable, well documented surface for advanced users while keeping internals private. Every exported identifier must carry a GoDoc comment describing its semantics and any invariants.

## Core Database Surface
- `type Rindb`, `InitRinDB`, and methods `Put`, `Get`, `Remove`, `IRange`, `Stats`, `Close` remain the primary entry points.
- `type Config`, `type Option`, and all `With…` helpers configure the database.
- Read‑only views: `type Snapshot` with `Sequence`, `Get`, `IRange`, `Release`; `type Stats`.
- `type RangeIterator` for range scans.

## Extension Points to Keep Public
- **Generic utilities:** `Comparable`, `Iterator[T]`, `PriorityQueue[T]`, `SkipList[K,V]`, `BloomFilter`, `Bitset`.
- **Storage building blocks:** `Bytes`, `InternalKey`, `Memtable`, `WAL`, `SSTable`, `SSTableBuilder`, `FileSystem`.
- **Iteration helpers:** `MergingIterator`, `RangeIterator` for composing custom iterators.

Each of these types/functions requires a concise GoDoc comment explaining usage and warning that higher‑level coordination (manifest, versions, transactions) remains internal.

## Internals to Hide
- Rename or move to internal packages: manifest/versions, transaction manager, logging helpers, linked lists, file number allocator, and other supporting data structures.
- Ensure unexported names (`lowerCamelCase`) for helpers that should not appear in the public API.

## Documentation Tasks
1. Audit all exported identifiers and add missing GoDoc comments.
2. Provide short examples in package docs or `docs/` showing:
   - Building and loading an `SSTable`.
   - Replaying a `WAL`.
   - Chaining custom iterators with `MergingIterator` or `RangeIterator`.
3. Update `README.md` or new docs to mention advanced extension points.

## Verification
- Run `make check`, `make test`, and `make test-integration-smoke`.
- Confirm `go vet` and static analysis pass after documentation additions.
