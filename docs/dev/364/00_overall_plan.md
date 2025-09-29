# Goals
- Enable SSTable reads via memory-mapped files on Linux, macOS, and Windows while retaining the current file I/O path as a fallback.
- Ensure runtime detection gates mmap usage per supported OS and keeps unsupported targets functional without build breaks.
- Provide configuration hooks and tests that validate both the mmap-enabled and fallback code paths.

# Context
- `SStable` currently reads through `FileSystem` helpers (`ReadAt`, `Seek`) without mmap support, so every lookup incurs syscalls.
- `FileSystem` owns the `*os.File`, making it the natural attachment point for mmap lifecycle management.
- We must guard platform-specific code with build tags: POSIX (`linux`, `darwin`) via `golang.org/x/sys/unix`, Windows via `golang.org/x/sys/windows`, and a stub for other OSes.
- Config plumbing lives in `config.go`; adding a toggle here lets callers opt out or run tests without mmap.

# Implementation Steps
1. **Config toggle for SSTable mmap (Complexity: 4)**
   - _Rationale_: Expose a feature flag that defaults to runtime-supported platforms, letting users disable mmap when debugging.
   - _Proposed diff_
     ```diff
     diff --git a/config.go b/config.go
     @@
     -type Config struct {
     +type Config struct {
     +     // enableSSTableMmap gates mmap-backed SSTable reads on supported OSes.
     +     enableSSTableMmap bool
           …
     @@
     -func DefaultConfig() Config {
     -    return Config{
     +func DefaultConfig() Config {
     +    return Config{
     +        enableSSTableMmap: runtime.GOOS == "linux" || runtime.GOOS == "darwin" || runtime.GOOS == "windows",
           …
     +func WithSSTableMmap(enabled bool) Option {
     +    return func(cfg *Config) {
     +        cfg.enableSSTableMmap = enabled
     +    }
     +}
     ```
   - _Rollout_: No additional rollout sequencing; toggle defaults to true only where we provide platform support.

2. **OS-specific mmap helpers (Complexity: 6)**
   - _Rationale_: Encapsulate mmap/munmap logic per OS while presenting a uniform API to `FileSystem`.
   - _Proposed diff_
     ```diff
     diff --git a/sstable_mmap_posix.go b/sstable_mmap_posix.go
     +//go:build darwin || linux
     +package rindb
     +
     +import (
     +    "fmt"
     +    "math"
     +    "unsafe"
     +
     +    "golang.org/x/sys/unix"
     +)
     +
     +type mmapHandle struct {
     +    data []byte
     +}
     +
     +func mapFile(f *os.File) (*mmapHandle, error) {
     +    info, err := f.Stat()
     +    if err != nil || info.Size() == 0 {
     +        return nil, err
     +    }
     +    if unsafe.Sizeof(uintptr(0)) == 4 && info.Size() > math.MaxInt32 {
     +        return nil, fmt.Errorf("sstable mmap: file too large for 32-bit build (%d bytes)", info.Size())
     +    }
     +    data, err := unix.Mmap(int(f.Fd()), 0, int(info.Size()), unix.PROT_READ, unix.MAP_SHARED)
     +    if err != nil {
     +        return nil, err
     +    }
     +    return &mmapHandle{data: data}, nil
     +}
     +
     +func (h *mmapHandle) Close() error {
     +    if h == nil || h.data == nil {
     +        return nil
     +    }
     +    err := unix.Munmap(h.data)
     +    h.data = nil
     +    return err
     +}
     diff --git a/sstable_mmap_windows.go b/sstable_mmap_windows.go
     +//go:build windows
     +package rindb
     +
     +import (
     +    "fmt"
     +    "math"
     +    "unsafe"
     +
     +    "golang.org/x/sys/windows"
     +)
     +
     +type mmapHandle struct {
     +    data   []byte
     +    handle windows.Handle
     +}
     +
     +func mapFile(f *os.File) (*mmapHandle, error) {
     +    info, err := f.Stat()
     +    if err != nil || info.Size() == 0 {
     +        return nil, err
     +    }
     +    if unsafe.Sizeof(uintptr(0)) == 4 && info.Size() > math.MaxInt32 {
     +        return nil, fmt.Errorf("sstable mmap: file too large for 32-bit build (%d bytes)", info.Size())
     +    }
     +    size := info.Size()
     +    h, err := windows.CreateFileMapping(windows.Handle(f.Fd()), nil, windows.PAGE_READONLY, 0, 0, nil)
     +    if err != nil {
     +        return nil, err
     +    }
     +    ptr, err := windows.MapViewOfFile(h, windows.FILE_MAP_READ, 0, 0, uintptr(size))
     +    if err != nil {
     +        windows.CloseHandle(h)
     +        return nil, err
     +    }
     +    data := unsafe.Slice((*byte)(unsafe.Pointer(ptr)), int(size))
     +    return &mmapHandle{data: data, handle: h}, nil
     +}
     +
     +func (h *mmapHandle) Close() error {
     +    if h == nil || h.data == nil {
     +        return nil
     +    }
     +    data := h.data
     +    h.data = nil
     +    errUnmap := windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&data[0])))
     +
     +    var errClose error
     +    if h.handle != 0 {
     +        errClose = windows.CloseHandle(h.handle)
     +        h.handle = 0
     +    }
     +
     +    if errUnmap != nil {
     +        return errUnmap
     +    }
     +    return errClose
     +}
     diff --git a/sstable_mmap_stub.go b/sstable_mmap_stub.go
     +//go:build !darwin && !linux && !windows
     +package rindb
     +
     +type mmapHandle struct{}
     +
     +func mapFile(*os.File) (*mmapHandle, error) { return nil, errMmapUnsupported }
     +func (h *mmapHandle) Close() error      { return nil }
     ```
   - _Rollout_: Requires `go get golang.org/x/sys/{unix,windows}` update; keep build tags to prevent unsupported builds.

3. **Integrate mmap into FileSystem/SSTable (Complexity: 7)**
   - _Rationale_: Attach the mapped slice to `FileSystem`, exposing zero-copy reads when enabled, while preserving existing APIs.
   - _Proposed diff_
     ```diff
     diff --git a/filesystem.go b/filesystem.go
     @@
     -type FileSystem struct {
     -    mu       sync.RWMutex
     -    filePath string
     -    file     *os.File
     +type FileSystem struct {
     +    mu       sync.RWMutex
     +    filePath string
     +    file     *os.File
     +    mmap     *mmapHandle
     }
     @@
     func (fs *FileSystem) Open(ctx context.Context) error {
     -    if fs.file != nil {
     +    if fs.file != nil {
     +        if fs.mmap == nil && fs.shouldMmap(ctx) {
     +            if handle, err := mapFile(fs.file); err == nil {
     +                fs.mmap = handle
     +            } else {
     +                logMmapFailure(ctx, fs.filePath, err)
     +            }
     +        }
             return nil
         }
     @@
     -    fs.file = file
     +    fs.file = file
     +    if fs.shouldMmap(ctx) {
     +        if handle, err := mapFile(file); err == nil {
     +            fs.mmap = handle
     +        } else {
     +            logMmapFailure(ctx, fs.filePath, err)
     +        }
     +    }
         return nil
     }
     @@
     func (fs *FileSystem) Close() error {
     -    if fs.file == nil {
     +    if fs.file == nil {
             return nil
         }
     +    if fs.mmap != nil {
     +        if err := fs.mmap.Close(); err != nil {
     +            logMmapCloseFailure(ctx, fs.filePath, err)
     +        }
     +        fs.mmap = nil
     +    }
         if err := fs.file.Close(); err != nil {
             return err
         }
         fs.file = nil
         return nil
     }
     diff --git a/offset_reader.go b/offset_reader.go
     @@
     -func (r *offsetReader) Read(p []byte) (int, error) {
     -    n, err := r.fs.ReadAt(p, r.offset)
     +func (r *offsetReader) Read(p []byte) (int, error) {
     +    if data := r.fs.mmapBytes(r.offset, len(p)); data != nil {
     +        n := copy(p, data)
     +        r.offset += int64(n)
     +        if n < len(p) {
     +            return n, io.EOF
     +        }
     +        return n, nil
     +    }
     +    n, err := r.fs.ReadAt(p, r.offset)
             if n > 0 {
                 r.offset += int64(n)
             }
             return n, err
     }
     ```
   - _Rollout_: Keep mmap optional; errors while mapping are logged but non-fatal so fallback path remains.

4. **Tests and configuration coverage (Complexity: 5)**
   - _Rationale_: Ensure both code paths work and Windows/Linux builds compile.
   - _Proposed diff_
     ```diff
     diff --git a/filesystem_test.go b/filesystem_test.go
     @@
     +func TestFileSystem_MmapFallback(t *testing.T) {
     +    cfg := DefaultConfig()
     +    cfg.enableSSTableMmap = false
     +    fs := tempFS(t)
     +    require.NoError(t, fs.Open(context.Background()))
     +    _, err := fs.ReadAt(make([]byte, 1), 0)
     +    require.ErrorIs(t, err, io.EOF)
     +}
     diff --git a/sstable_test.go b/sstable_test.go
     @@
     +t.Run("mmap-enabled", func(t *testing.T) {
     +    cfg := DefaultConfig()
     +    cfg.enableSSTableMmap = true
     +    sstable := openFixture(t, cfg)
     +    got, err := sstable.GetValue(ctx, []byte("k1"))
     +    require.NoError(t, err)
     +    assert.Equal(t, []byte("v1"), got)
     +})
     diff --git a/go.mod b/go.mod
     @@
     -require (
     +require (
     +    golang.org/x/sys v0.20.0
           …
     ```
   - _Rollout_: Add CI targets for Windows cross-compilation (`GOOS=windows go build ./...`) to catch build regressions.

# Pitfalls & Validation
- **Risks**
  - mmap exhaustion on 32-bit builds: guard via file size checks before mapping and allow config opt-out.
  - Windows handle leaks: ensure `Close` unmaps view and closes mapping handle.
  - Concurrent remapping during reopen: protect with `FileSystem.mu` and close previous mapping before re-map.
  - Unsupported OS builds: stub file returns `errMmapUnsupported` so defaults skip mapping.
- **Tests**
  - _Positive_: `TestSSTable_GetValue_mmap` (new) ensures data reads succeed when mmap is on; verifies zero-copy path doesn’t alter semantics.
  - _Negative_: `TestFileSystem_MmapUnsupported` toggles config false/unsupported GOOS to assert fallback read path remains functional.
  - _Invalidation_: Add `go test ./...` with `GOOS=windows` in CI script to invalidate regressions in Windows-specific implementation.
  - _Resource_: Extend integration smoke test to run with `WithSSTableMmap(false)` ensuring toggle works end-to-end.
  - These checks surface platform-specific regressions early and prevent silent data-corruption by verifying both code paths on every run.
