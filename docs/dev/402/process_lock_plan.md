# Goals
- Prevent multiple OS processes from concurrently mutating the same RinDB directory by enforcing an exclusive startup lock.
- Ensure the lock integrates cleanly with existing init/close flows so crash recovery and background tasks remain unaffected.

# Context
- `rindb.InitRinDB` currently builds the directory structure, WAL, manifest writer, and SSTable manager without guarding against multi-process opens (`rindb.go`).
- `Config` lacks any notion of a lock file, so callers cannot override or introspect locking behavior (`config.go`).
- The shutdown path (`Rindb.Close`) tears down WAL/SSTables but leaves no hook to release extra resources, which we will need for a process-scoped lock.

# Implementation Steps
1. **Add cross-platform process-lock scaffolding (Complexity 6/10).** Implement a light wrapper that owns the lock file descriptor/handle, tracks ownership, and exposes `Acquire/Release`.
   ```diff
+// process_lock.go
+package rindb
+
+import "context"
+
+type processLock interface {
+        Acquire(ctx context.Context) error
+        Release() error
+}
+
+type fileProcessLock struct {
+        path string
+        log  scopedLogger
+        fd   *os.File
+}
+
+func newProcessLock(path string, log scopedLogger) *fileProcessLock {
+        return &fileProcessLock{path: path, log: log}
+}
   ```
   - Rationale: A dedicated type keeps platform-specific locking isolated behind build tags.
   - Rollout: Stubs compile everywhere; later steps wire real acquire/release implementations per OS.

2. **Provide POSIX/Windows locking implementations (Complexity 7/10).** Back the wrapper with `unix.Flock` on Unix-like hosts and `windows.LockFileEx` on Windows, including informative errors when the lock is held.
   ```diff
+//go:build unix
+
+package rindb
+
+import (
+        "fmt"
+        "os"
+        "time"
+
+        "golang.org/x/sys/unix"
+)
+
+func (l *fileProcessLock) Acquire(ctx context.Context) error {
+        f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0o640)
+        if err != nil {
+                return fmt.Errorf("open lock file: %w", err)
+        }
+        if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
+                _ = f.Close()
+                if errors.Is(err, unix.EWOULDBLOCK) {
+                        return fmt.Errorf("database is already open (lock %s busy)", l.path)
+                }
+                return fmt.Errorf("flock %s: %w", l.path, err)
+        }
+        l.fd = f
+        l.log.info(ctx, "acquired process lock %s", l.path)
+        return nil
+}
+
+func (l *fileProcessLock) Release() error {
+        if l.fd == nil {
+                return nil
+        }
+        defer func() { l.fd = nil }()
+        if err := unix.Flock(int(l.fd.Fd()), unix.LOCK_UN); err != nil {
+                return fmt.Errorf("unlock %s: %w", l.path, err)
+        }
+        return l.fd.Close()
+}
   ```
   - Rationale: Non-blocking file locks fail fast when another process already owns the database.
   - Rollout: Mirror logic in `process_lock_windows.go` using `windows.LockFileEx`/`UnlockFileEx`; reuse `fileProcessLock` fields.

3. **Integrate locking with init and shutdown (Complexity 5/10).** Acquire immediately after directory creation, store the lock on `Rindb`, release during `Close`, and ensure error paths drop ownership.
   ```diff
   diff --git a/rindb.go b/rindb.go
   --- a/rindb.go
   +++ b/rindb.go
   @@
   -       if err := os.MkdirAll(cfg.databaseDir, 0750); err != nil {
   +       if err := os.MkdirAll(cfg.databaseDir, 0750); err != nil {
                return nil, fmt.Errorf("failed to create database directory %s: %w", cfg.databaseDir, err)
        }
   +    locker := newProcessLock(path.Join(cfg.databaseDir, "LOCK"), log)
   +    if err := locker.Acquire(ctx); err != nil {
   +            return nil, err
   +    }
   +    defer func() {
   +            if err != nil {
   +                    _ = locker.Release()
   +            }
   +    }()
   @@
   -       rin := &Rindb{
   +       rin := &Rindb{
                WAL:               wal,
                Memtable:          memtable,
                SSTableManager:    ssTableManager,
                versionSet:        vs,
                manifest:          mw,
                config:            cfg,
                log:               log,
                shutdownTelemetry: shutdownTelemetry,
                sequenceNumber:    maxSeqNum,
   +            processLock:       locker,
        }
   diff --git a/rindb.go b/rindb.go
   @@
   -       if err := r.shutdownTelemetry(ctx); err != nil {
   +       if err := r.shutdownTelemetry(ctx); err != nil {
                r.log.errorf(ctx, "Error shutting down telemetry: %v", err)
        } else {
                r.log.info(ctx, "Telemetry shutdown completed.")
        }
   +    if err := r.processLock.Release(); err != nil {
   +            r.log.errorf(ctx, "Error releasing process lock: %v", err)
   +    }
   ```
   - Rationale: Acquire early ensures all subsequent startup work assumes exclusive access; storing the lock enables deterministic release.
   - Rollout: Extend `Rindb` struct/constructor, add nil checks for optional lock (e.g., tests injecting `nil`).

4. **Expose configuration toggles and tests (Complexity 5/10).** Allow opting out (for advanced multi-process test harnesses) and codify behavior with integration-style tests that spawn helper processes.
   ```diff
+// config.go
+type Config struct {
+        ...
+        disableProcessLock bool
+}
+
+func WithDisableProcessLock() Option {
+        return func(c *Config) { c.disableProcessLock = true }
+}
+
+// init path
+if !cfg.disableProcessLock {
+        locker := newProcessLock(...)
+}
+
+// integration/process_lock_test.go
+cmd := exec.Command(testProgram, tmpDir)
+require.NoError(t, cmd.Run())
+cmd2 := exec.Command(testProgram, tmpDir)
+require.Error(t, cmd2.Run())
   ```
   - Rationale: Config switch keeps deterministic tests simple; process-spawned test asserts mutual exclusion behavior end-to-end.
   - Rollout: Use `t.Parallel()` sparingly to avoid cross-test lock contention; guard tests with build tags if Windows-specific handling needs additional coverage.

# Pitfalls & Validation
- **Risks:**
  - Lock file might leak if startup fails before `Rindb` is returned → mitigation: `defer locker.Release()` on error paths in `InitRinDB`.
  - Windows API differences can silently fail if overlapped I/O flags are wrong → mitigation: mirror proven `LockFileEx` patterns and add a platform-specific regression test verifying handle reuse.
  - Forceful process termination leaves a stale file descriptor but not a held lock → document that stale `LOCK` files are safe to ignore.
- **Tests:**
  - *Positive:* `TestInitRinDB_SerialAcquisition` opens DB twice sequentially expecting success → ensures lock releases on `Close`.
  - *Negative:* `TestInitRinDB_ConcurrentProcessFails` launches helper process that keeps DB open while second process should fail with `database is already open` → proves exclusive enforcement.
  - *Invalidation:* `TestInitRinDB_DisableProcessLockAllowsParallel` sets `WithDisableProcessLock()` and spawns two helpers expecting both succeed → confirms opt-out path and guards against regressions in config plumbing.
  - *Platform:* `TestProcessLock_ReacquireAfterCrash` simulates lost handle by closing underlying file before `Release()` to ensure unlock errors propagate → validates error logging and cleanup paths.
