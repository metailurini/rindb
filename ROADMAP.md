## Roadmap

This roadmap outlines the development path for `rindb`, starting from version `v0.2.0`, with specific version milestones for each category.

### Stabilization and Documentation

**Goal**: Build a stable base and improve accessibility.

- **Tasks**:
  - **Complete Documentation**:
    - [ ] Detailed README (installation, usage, architecture).
    - [x] Inline comments and GoDoc for key functions (e.g., `InitRinDB`, `SSTableManager.Compact`).
  - **Fix Bugs and Edge Cases**:
    - [x] Audit tests (e.g., WAL recovery in `wal_test.go` for partial writes).
    - [ ] Consistent error handling (e.g., propagate `ErrDatabaseClosed` in `rindb.go`).
  - **Basic CLI**:
    - [ ] Expand `cmd/main.go` to support `put`, `get`, `remove`, `list` commands.

- **Deliverables**:
  - Comprehensive README and GoDoc.
  - Basic CLI with CRUD operations.
  - 90%+ test coverage (building on existing tests like `rindb_test.go`).

- **Version**: `v0.2.1`
  - **Rationale**: These are stabilizing improvements and minor CLI additions, warranting a patch release. The codebase already has core functionality (e.g., `Put`, `Get`, `Remove` in `rindb.go`), so this focuses on polish and usability.

---

### Core Feature Enhancements

**Goal**: Add essential features and improve usability.

- **Tasks**:
  - **Range Queries**:
    - [ ] Add `Range(start, end Bytes) ([]Record, error)` to `Rindb` in `rindb.go`.
    - [ ] Extend `SStable.Iterator()` in `sstable.go` for range filtering (leverages existing `sstableIterator`).
  - **Advanced Compaction**:
    - [ ] Implement tiered compaction in `sstable_manager.go` as an alternative to leveled compaction.
    - [ ] Add triggers (write rate, I/O load) to `SSTableManager.shouldCompact`.
  - **Configuration Validation**:
    - [ ] Validate `Config` options in `config.go` (e.g., ensure `maxMemtableSize` > 0).
    - [ ] Add `Config.Validate()` method.
  - **Basic Metrics**:
    - [ ] Expose stats (Memtable size via `Memtable.ByteSize()`, SSTable count from `SSTableManager.levels`) via `Stats()` in `rindb.go`.

- **Deliverables**:
  - [ ] Range query support in API and CLI.
  - [ ] Configurable compaction strategies.
  - [ ] Basic observability (e.g., stats output).

- **Version**: `v0.3.0`
  - **Rationale**: Range queries and advanced compaction are significant new features, justifying a minor version bump. The codebase lacks these (e.g., no `Range` method in `rindb.go`, basic compaction in `SSTableManager`), so this is a natural next step.

---

### Performance Optimization

**Goal**: Boost speed, scalability, and efficiency.

- **Tasks**:
  - **Benchmarking Suite**:
    - [ ] Add benchmarks for CRUD (`Put`, `Get`, `Remove`) and compaction in `Makefile` (e.g., extend `test-coverage` target).
    - [ ] Compare with LevelDB/RocksDB using existing test utils (e.g., `test_utils.go`).
  - **Read Optimization**:
    - [ ] Cache SSTable sparse indexes in `SStable` (modify `sstable.go`).
    - [ ] Parallelize `Get` searches in `SSTableManager.searchKey` using goroutines.
  - **Write Optimization**:
    - [ ] Batch WAL writes in `wal.go` (e.g., buffer multiple `Append` calls).
    - [ ] Async Memtable flushes in `rindb.go` (e.g., background goroutine for `Put`).
  - **Memory Management**:
    - [ ] Add memory budget in `rindb.go` to limit Memtable, Bloom filters, and index usage.

- **Deliverables**:
  - Benchmark results in README.
  - 2x write throughput, 1.5x read latency improvement (target).
  - Memory usage controls.

- **Version**: `v0.4.0`
  - **Rationale**: Performance enhancements add significant value without breaking the API, fitting a minor release. The codebase has no benchmarks or optimizations like caching (e.g., `SStable` lacks index caching), making this a distinct step forward.

---

### Production Readiness

**Goal**: Ensure reliability and completeness for real-world use.

- **Tasks**:
  - **Replication**:
    - [ ] Add leader-follower replication in `rindb.go` using WAL streaming (extend `wal.go`).
  - **Backup and Restore**:
    - [ ] Implement `Backup()` and `Restore()` in `rindb.go` (e.g., snapshot SSTables and WAL).
  - **Advanced Transactions**:
    - [ ] Support multi-key transactions in `transaction_manager.go` with conflict detection.
  - **CLI Enhancements**:
    - [ ] Add `stats`, `backup`, and `config` commands to `cmd/main.go`.
  - **Packaging**:
    - [ ] Publish Go module with versioning (update `go.mod`).
    - [ ] Create Docker image (add `Dockerfile`).
  - **Release v1.0**:
    - [ ] Production-ready version with comprehensive testing.

- **Deliverables**:
  - Replication and backup features.
  - Full-featured CLI.
  - v1.0 with changelog.

- **Version**: `v1.0.0`
  - **Rationale**: Replication, backup, and advanced transactions mark `rindb` as production-ready, justifying a major release. The codebase lacks these features (e.g., no replication in `rindb.go`), and v1.0 signals stability and maturity.

---

### Ecosystem and Community

**Goal**: Grow adoption and extend capabilities.

- **Tasks**:
  - **Bindings**:
    - [ ] Create C or Python bindings (new files, e.g., `bindings/c/rindb.c`).
  - **Plugins**:
    - [ ] Support custom compaction/storage via interfaces in `sstable_manager.go` and `filesystem.go`.
  - **Community Engagement**:
    - [ ] Set up GitHub Discussions and issue templates.
    - [ ] Write tutorials and blog posts (e.g., in `docs/` directory).
  - **Specialized Use Cases**:
    - [ ] Add time-series optimizations (e.g., TTL in `record.go`).

- **Deliverables**:
  - [ ] Language bindings and plugin system.
  - [ ] Active community contributions.

- **Version**: `v1.1.0`
  - **Rationale**: Bindings and plugins extend functionality post-v1.0 without breaking changes, fitting a minor release. Time-series support could be `v1.2.0` if significant. The codebase has no such extensions currently, making this a future enhancement.

---

## Version Summary

- **`v0.2.0`**: Current version (core LSM functionality: Memtable, WAL, SSTables, Bloom filters).
- **`v0.2.1`**: Stabilization and Documentation (bug fixes, docs, basic CLI).
- **`v0.3.0`**: Core Feature Enhancements (range queries, advanced compaction, metrics).
- **`v0.4.0`**: Performance Optimization (benchmarks, read/write optimizations, memory management).
- **`v1.0.0`**: Production Readiness (replication, backup, advanced transactions, full CLI, packaging).
- **`v1.1.0`**: Ecosystem and Community (bindings, plugins, community engagement, optional time-series).
