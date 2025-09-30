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

### Cleanup and Reliability

**Goal**: Remove deprecated artifacts and harden manifest lifecycle.

- **Tasks**:
  - [x] Remove obsolete manifest files.
  - [x] Handle manifest cleanup errors gracefully.

- **Deliverables**:
  - Leaner on-disk state without stale manifests.
  - More reliable startup/shutdown cycles.

- **Version**: `v0.9.0`
  - **Rationale**: Focused cleanup and reliability improvements warrant a minor release.

---

### Footer & Index Metadata

**Goal**: Encode and validate rich SSTable metadata for safer reads.

- **Tasks**:
  - [x] Write index metadata to SSTable footer.
  - [x] Validate footer metadata and index alignment.
  - [x] Validate empty-index SSTables and padding.
  - [x] Reuse offset reader for sparse index loading.

- **Deliverables**:
  - Stronger read-path invariants with verified metadata.
  - Clear errors on malformed or misaligned indexes.

- **Version**: `v0.10.0`
  - **Rationale**: Material changes to file format validation justify a minor version.

---

### Oracle Fuzzing & Read Safety

**Goal**: Improve correctness via fuzzing and tighten read semantics.

- **Tasks**:
  - [x] Add SQLite oracle for fuzzing harness.
  - [x] Avoid truncated range checks; advance offset reader correctly.
  - [x] Correct offset reader behavior on read errors.
  - [x] Improve internal key decoding; prevent reads past data section.

- **Deliverables**:
  - Differential testing against a known-good oracle.
  - Safer iterators and IO error handling.

- **Version**: `v0.11.0`
  - **Rationale**: New fuzzing capabilities and safety fixes merit a minor bump.

---

### Deterministic Replay & Harness Hardening

**Goal**: Make test harness behavior reproducible and robust across restarts.

- **Tasks**:
  - [x] Replay diffharness operations on restart.
  - [x] Harden diffharness replay against edge cases.
  - [x] Avoid spurious commits; prevent key tracker leaks and nondeterminism.

- **Deliverables**:
  - Deterministic harness behavior across crashes/restarts.
  - Reduced flakiness in integration testing.

- **Version**: `v0.12.0` / `v0.12.1`
  - **Rationale**: Feature plus follow-up hardening patch.

---

### Query UX & Test Conventions

**Goal**: Enhance query capabilities and standardize testing/validation.

- **Tasks**:
  - [x] Add support for reverse range scanning.
  - [x] Introduce testing conventions and validation tools.
  - [x] Refine iterator rewind semantics.
  - [x] Enforce tracing span naming via custom `go vet` analyzer (`tool/spanname`) and integrate into `make check`.

- **Deliverables**:
  - Richer range query ergonomics.
  - Consistent tests and stronger quality gates in CI.

- **Version**: `v0.13.0`
  - **Rationale**: User-facing API improvement alongside developer tooling.

---

### Iterator Engine & Harness Reliability

**Goal**: Solidify iterator behavior and expand diff harness tooling.

- **Tasks**:
  - [x] Introduce an iterator engine interface to centralize iteration control.
  - [x] Add an iterator walk flag to the diff harness for expanded validation coverage.
  - [x] Make `HasNext` / `HasPrev` idempotent when prefetching.
  - [x] Tighten iterator boundary handling and state management across SSTable and merging iterators.

- **Deliverables**:
  - Unified iterator engine abstraction with improved ergonomics.
  - Stronger diff harness diagnostics through walk flag support.
  - Reliable forward/backward iteration with consistent error handling.

- **Version**: `v0.14.0`
  - **Rationale**: Iterator engine and harness upgrades materially improve correctness and developer productivity, warranting a new minor release.

---

### Descending Range Iteration
**Goal**: Enhance range query capabilities with descending order iteration.

- **Tasks**:
  - [x] Implement full support for descending range iteration across memtable, skiplist, and sstable.
  - [x] Add range order options.
  - [x] Document descending range order.
  - [x] Fix bugs related to descending iteration (e.g., snapshot history, tail priming, tombstoned records, EOF handling).

- **Deliverables**:
  - Robust and correct descending range iteration.

- **Version**: `v0.15.0` / `v0.15.1` / `v0.15.2`
  - **Rationale**: Major enhancement to range query functionality, followed by several bug fix releases to stabilize it.

---

### Memory-Mapped SSTable Reads
**Goal**: Improve read performance and efficiency using memory-mapped files for SSTables.

- **Tasks**:
  - [x] Integrate mmap-backed sstable reads.
  - [x] Harden mmap slice bounds handling and improve error handling.
  - [x] Disable telemetry during tests.
  - [x] Resolve benchmark workflow PR lookup.

- **Deliverables**:
  - Faster SSTable reads with reduced memory overhead.
  - Improved stability and error handling for mmap operations.

- **Version**: `v0.16.0`
  - **Rationale**: Significant performance feature with associated bug fixes.

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
    - [x] Publish Go module with versioning (update `go.mod`).
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

- **`v0.16.0`**: Current (mmap-backed sstable reads; mmap error handling; telemetry disablement during tests).
- **`v0.15.2` / `v0.15.1` / `v0.15.0`**: Descending range iteration support and related bug fixes.
- **`v0.14.0`**: Iterator engine interface; diff harness walk flag; idempotent iterator navigation; iterator reliability fixes.
- **`v0.13.0`**: Reverse range scans; testing conventions; spanname vet; iterator rewind refinements.
- **`v0.12.1` / `v0.12.0`**: Deterministic diffharness replay plus hardening.
- **`v0.11.0`**: SQLite oracle fuzzing; read-path safety fixes.
- **`v0.10.0`**: Footer/index metadata encoding and validation.
- **`v0.9.0`**: Manifest cleanup and reliability.
- **`v0.8.0`**: Table cache and FD limiter; stability fixes.
- **`v0.7.0`**: Manifest and version management (rotation, repair mode, tracking).
- **`v0.6.0`**: SSTable builder, checksums, and reliability improvements.
- **`v0.5.0`**: Snapshots and concurrency improvements.
- **`v0.4.0`**: Performance optimization and benchmarking.
- **`v0.3.0`**: Range queries, compaction triggers, metrics.
- **`v0.2.1`**: Stabilization, docs, and basic CLI.
- **`v0.2.0`**: Initial core LSM (Memtable, WAL, SSTables, Bloom filters).

- **`v1.0.0`**: Production readiness (replication, backup, transactions, packaging).
- **`v1.1.0`**: Ecosystem and community (bindings, plugins, engagement, optional time-series).
