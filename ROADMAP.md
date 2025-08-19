## Roadmap

This roadmap outlines the development path for `rindb`, starting from version `v0.2.0`, with specific version milestones for each category.

### Stabilization and Documentation

**Goal**: Build a stable base and improve accessibility.

- **Tasks**:
  - **Complete Documentation**:
    - [x] Detailed README (installation, usage, architecture).
    - [x] Inline comments and GoDoc for key functions (e.g., `InitRinDB`, `SSTableManager.Compact`).
  - **Fix Bugs and Edge Cases**:
    - [x] Audit tests (e.g., WAL recovery in `wal_test.go` for partial writes).
    - [x] Consistent error handling (e.g., propagate `ErrDatabaseClosed` in `rindb.go`).
  - **Basic CLI**:
    - [x] Expand `cmd/main.go` to support `put`, `get`, `remove` commands.

- **Deliverables**:
  - Comprehensive README and GoDoc.
  - Basic CLI with CRUD operations.
  - 85%+ test coverage.

- **Version**: `v0.2.1`
  - **Rationale**: These are stabilizing improvements and minor CLI additions, warranting a patch release. The codebase already has core functionality (e.g., `Put`, `Get`, `Remove` in `rindb.go`), so this focuses on polish and usability.

---

### Core Feature Enhancements

**Goal**: Add essential features and improve usability.

- **Tasks**:
  - **Range Queries**:
    - [x] Add `IRange(start, end Bytes) (*RangeIterator, error)` to `Rindb` in `rindb.go`.
    - [x] Extend `SStable.IRange()` in `sstable.go` for range filtering (leverages existing `sstableIterator`).
    - [x] Implement `Memtable.IRange` and `RangeIterator` for comprehensive range query support.
  - **Advanced Compaction**:
    - [ ] Add triggers (write rate, I/O load) to `SSTableManager.shouldCompact`. (Current implementation uses configurable thresholds, but not dynamic triggers based on system load.)
  - **Configuration Validation**:
    - [x] Validate `Config` options in `config.go` (e.g., ensure `maxMemtableSize` > 0).
    - [x] Add `Config.Validate()` method.
  - **Basic Metrics**:
    - [x] Implement OpenTelemetry for metrics collection (e.g., `putCalls`, `getCalls`, `compactLatency`).
    - [x] Expose runtime statistics via a `Stats()` API and CLI command.
  - **Pprof Tests**:
    - [x] Add `test-pprof` target to `Makefile` for generating CPU and memory profiles.
    - [x] Create `scripts/run-pprof-tests.sh` to automate pprof file generation for all packages.

- **Deliverables**:
  - [x] Comprehensive range query support in API.
  - [ ] Configurable compaction strategies with dynamic triggers.
  - [x] Robust configuration validation.
  - [x] Advanced observability via OpenTelemetry.

- **Version**: `v0.3.0`
  - **Rationale**: Range queries and configuration validation are significant new features, justifying a minor version bump. The codebase has made substantial progress in these areas, with OpenTelemetry providing a more advanced metrics solution than initially planned.

---

### Performance Optimization

**Goal**: Boost speed, scalability, and efficiency.

- **Tasks**:
  - **Benchmarking Suite**:
    - [ ] Add benchmarks for CRUD (`Put`, `Get`, `Remove`) and compaction in `Makefile` (e.g., extend `test-coverage` target).
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
    - [x] Add `stats` command to `cmd/main.go`.
    - [ ] Add `backup` and `config` commands to `cmd/main.go`.
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
