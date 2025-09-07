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
    - [x] Add triggers (write rate, I/O load) to `SSTableManager.shouldCompact`.
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
  - [x] Configurable compaction strategies with dynamic triggers.
  - [x] Robust configuration validation.
  - [x] Advanced observability via OpenTelemetry.

- **Version**: `v0.3.0`
  - **Rationale**: Range queries and configuration validation are significant new features, justifying a minor version bump. The codebase has made substantial progress in these areas, with OpenTelemetry providing a more advanced metrics solution than initially planned.

---

### Performance Optimization

**Goal**: Boost speed, scalability, and efficiency.

- **Tasks**:
  - **Benchmarking Suite**:
    - [x] Add benchmarks for CRUD (`Put`, `Get`, `Remove`) and compaction in `Makefile`.
  - **Read Optimization**:
    - [x] Cache SSTable sparse indexes in `SStable`.
    - [x] Introduce merging iterator to optimize range scans.
  - **Write Optimization**:
    - [x] Batch WAL writes in `wal.go`.
    - [x] Async Memtable flushes in `rindb.go`.
  - **Memory Management**:
    - [x] Add memory budget in `rindb.go` to limit Memtable and index usage.

- **Deliverables**:
  - [x] Benchmark results integrated into the test suite.
  - [x] Significant write throughput and read latency improvements.
  - [x] Memory usage controls and optimizations.

- **Version**: `v0.4.0`
  - **Rationale**: Performance enhancements add significant value without breaking the API. This release focused on establishing a benchmarking suite and implementing foundational read/write optimizations, which are now largely complete.

---

### Snapshot and Concurrency

**Goal**: Introduce point-in-time snapshots and improve concurrent operations.

- **Tasks**:
  - **Snapshot Implementation**:
    - [x] Add `Snapshot()` method to `DB` to create read-only views.
    - [x] Implement `Snapshot.Get()` and `Snapshot.IRange()` for consistent reads.
    - [x] Ensure `Snapshot.Release()` cleans up resources correctly.
  - **Concurrency Control**:
    - [x] Guard file operations and critical sections with mutexes.
    - [x] Make `Snapshot.Release` thread-safe.
  - **Optimized Merging**:
    - [x] Introduce a merging iterator to combine results from Memtable and SSTables.
    - [x] Implement `mergeSSTablesV2` for more efficient compaction.
  - **Internal Key Handling**:
    - [x] Introduce versioned memtables with internal keys.
    - [x] Refine internal key ordering and range bounds for correctness.

- **Deliverables**:
  - [x] Full snapshot support for point-in-time reads.
  - [x] Improved stability under concurrent loads.
  - [x] More efficient and correct data merging during reads and compactions.

- **Version**: `v0.5.0`
  - **Rationale**: Snapshots are a major feature that significantly enhances the database's capabilities. This, combined with substantial improvements to concurrency and data merging logic, justifies a minor version bump.

---

### SSTable Enhancements and Reliability

**Goal**: Improve SSTable robustness, efficiency, and data integrity.

- **Tasks**:
  - **SSTable Builder**:
    - [x] Implement a dedicated SSTable builder for efficient creation.
    - [x] Allow configuration of SSTable builder size.
  - **Checksum Verification**:
    - [x] Add checksum verification for data integrity.
  - **Bug Fixes and Performance Improvements**:
    - [x] Address various SSTable-related bugs and optimize performance (e.g., checksum computation).

- **Deliverables**:
  - More robust and configurable SSTable creation.
  - Enhanced data integrity with checksums.
  - Improved overall stability and performance.

- **Version**: `v0.6.0`
  - **Rationale**: These features significantly enhance the reliability and efficiency of SSTables, justifying a minor version bump.

---

### Manifest and Version Management

**Goal**: Enhance manifest and version management for improved reliability and recovery.

- **Tasks**:
  - [x] Add manifest rotation and snapshot support.
  - [x] Add repair mode option and conditional level loading.
  - [x] Introduce file path helpers.
  - [x] Manifest integration plan.
  - [x] Register SSTables via manifest.
  - [x] Track file numbers from version edits.

- **Deliverables**:
  - Robust manifest management.
  - Improved recovery.
  - Consistent file tracking.

- **Version**: `v0.7.0`
  - **Rationale**: These features significantly enhance the database's internal consistency, recovery mechanisms, and overall reliability, justifying a minor version bump.

---

### Table Cache and File Descriptor Management

**Goal**: Improve table cache efficiency and manage file descriptors effectively.

- **Tasks**:
  - [x] Implement SSTable Table Cache.
  - [x] Add expiry for table cache tombstones.
  - [x] Cache SSTable handles.
  - [x] Expose table cache configuration and stats.
  - [x] Add semaphore-based FD limiter.
  - [x] Fix various bugs related to table cache and FD limiter.

- **Deliverables**:
  - Efficient and robust SSTable table cache.
  - Controlled file descriptor usage.
  - Enhanced stability and performance.

- **Version**: `v0.8.0`
  - **Rationale**: These features significantly improve the performance and reliability of the database by optimizing SSTable access and managing system resources more effectively, justifying a minor version bump.

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
- **`v0.5.0`**: Snapshot and Concurrency (point-in-time snapshots, stability improvements).
- **`v0.6.0`**: SSTable Enhancements and Reliability (SSTable builder, checksums, bug fixes).
- **`v0.7.0`**: Manifest and Version Management (manifest rotation, repair mode, file tracking).
- **`v0.8.0`**: Table Cache and File Descriptor Management (table cache, FD limiter, bug fixes).
- **`v1.0.0`**: Production Readiness (replication, backup, advanced transactions, full CLI, packaging).
- **`v1.1.0`**: Ecosystem and Community (bindings, plugins, community engagement, optional time-series).
