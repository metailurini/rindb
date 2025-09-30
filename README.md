# RinDB

<p align="center">
  <img src='./assets/mascot.png' width='70%'>
</p>

<p align="center">
    <img src='https://img.shields.io/codecov/c/github/metailurini/rindb.svg?maxAge=2592000'>
    <img src='https://qlty.sh/gh/metailurini/projects/rindb/maintainability.svg'>
    <img src='https://img.shields.io/github/license/metailurini/rindb'>
    <a href="https://deepwiki.com/metailurini/rindb">
        <img src="https://deepwiki.com/badge.svg" alt="Ask DeepWiki">
    </a>
</p>


**RinDB** is a lightweight, embeddable key-value database inspired by Log-Structured Merge (LSM) trees and LevelDB. It is designed for simplicity, performance, and extensibility, making it suitable for applications requiring fast, persistent storage.

## Features

- **LSM Storage Engine**: Mutable memtable with leveled SSTables, manifest recovery, and deterministic WAL replay.
- **Crash Safety & Validation**: Write-ahead logging, manifest rotation, SQLite oracle fuzzing, and a diff harness with walk mode to enforce correctness.
- **Bidirectional Range Iterators**: Unified iterator engine that merges memtable and SSTables with forward/backward scans and reverse range support.
- **Snapshots**: Point-in-time reads with automatic retention and cleanup to keep historical versions visible while active.
- **Adaptive Table Cache**: Sharded SLRU cache with tombstones, corruption quarantine, and runtime statistics.
- **Observability**: OpenTelemetry metrics/tracing plus programmatic stats and CLI reporting.
- **Configurable Runtime**: Functional options for directories, memory budgets, cache sizing, telemetry exporters, and logging.
- **Memory-Mapped SSTable Reads**: Accelerates SSTable data access on Linux, macOS, and Windows using memory-mapped files, with a fallback to standard file I/O.

## Installation

RinDB is written in Go and requires Go 1.25.0 or later. To include it in your project:

```bash
go get github.com/metailurini/rindb@latest
```

Clone the repository for development:

```bash
git clone https://github.com/metailurini/rindb.git
cd rindb
```

## Usage

### Basic Example

```go
package main

import (
    "context"
    "fmt"

    "github.com/metailurini/rindb"
)

func main() {
    // Initialize RinDB with default configuration
    ctx := context.Background()
    db, err := rindb.InitRinDB(ctx)
    if err != nil {
        fmt.Println("Error initializing RinDB:", err)
        return
    }
    defer db.Close()

    // Put a key-value pair
    err = db.Put(ctx, []byte("key1"), []byte("value1"))
    if err != nil {
        fmt.Println("Error putting key:", err)
        return
    }

    // Get a value by key
    value, err := db.Get(ctx, []byte("key1"))
    if err != nil {
        fmt.Println("Error getting key:", err)
        return
    }
    fmt.Println("Value:", string(value)) // Output: Value: value1

    // Remove a key
    err = db.Remove(ctx, []byte("key1"))
    if err != nil {
        fmt.Println("Error removing key:", err)
        return
    }
}
```

### Snapshot-Based Reads

```go
ctx := context.Background()
db, _ := rindb.InitRinDB(ctx)
defer db.Close()

// Write initial values
_ = db.Put(ctx, []byte("k1"), []byte("v1"))
_ = db.Put(ctx, []byte("k2"), []byte("v2"))

snap, _ := db.NewSnapshot(ctx)
defer snap.Release(ctx)

// Mutations after snapshot do not affect reads through it
_ = db.Put(ctx, []byte("k1"), []byte("v3"))
_ = db.Remove(ctx, []byte("k2"))

val, _ := snap.Get(ctx, []byte("k1"))
fmt.Println(string(val)) // Output: v1

it, _ := snap.IRange(ctx, []byte("k1"), []byte("k3"))
for it.HasNext() {
    rec, _ := it.Next()
    fmt.Printf("%s => %s\n", rec.GetKey(), rec.GetValue())
}
for it.HasPrev() {
    rec, _ := it.Prev()
    fmt.Printf("rev %s => %s\n", rec.GetKey(), rec.GetValue())
}
it.Close()

// Descending scans stream the highest key first.
desc, _ := db.IRange(ctx, []byte("k1"), []byte("k3"), rindb.IRangeOrder(rindb.RangeDesc))
for desc.HasNext() {
    rec, _ := desc.Next()
    fmt.Printf("desc %s => %s\n", rec.GetKey(), rec.GetValue())
}
desc.Close()
```

Range scans walk keys in ascending order by default. Pass
`rindb.IRangeOrder(rindb.RangeDesc)` (or `range <start> <end> desc` in the CLI) to
stream records from the upper bound down to the start key.

### Runtime Statistics

```go
s := db.Stats()
fmt.Println("Active snapshots:", s.ActiveSnapshots)
tc := db.TableCacheStats()
fmt.Println("Cache hits:", tc.Hits)
```

The `Stats` method reports metrics such as memtable size, sequence number,
active snapshot count, per-level SSTable counts, WAL usage, and operation counters.
`TableCacheStats` exposes cache hit/miss ratios and byte usage.

### Custom Configuration

```go
ctx := context.Background()
db, err := rindb.InitRinDB(ctx,
    rindb.WithDatabaseDir("./mydb"),
    rindb.WithMaxMemtableSize(1024*1024), // 1MB
    rindb.WithBloomFalsePositiveRate(0.01),
    rindb.WithCacheBytes(64<<20), // 64MB table cache
    rindb.WithCacheShards(8),
    rindb.WithCacheTombstoneTTL(30 * time.Second),
)
if err != nil {
    fmt.Println("Error:", err)
    return
}
defer db.Close()
```

The sample CLI accepts `--cache-bytes` and `--cache-shards` flags to tune the cache.

### Logging

RinDB does not emit logs unless a logger is provided. Inject a custom logger and
set the desired verbosity:

```go
logger := rindb.NewStdLogger(log.New(os.Stdout, "", log.LstdFlags))
db, _ := rindb.InitRinDB(ctx,
    rindb.WithLogger(logger),
    rindb.WithLogLevel(rindb.LogLevelDebug),
)
```

Omitting `WithLogger` keeps the database silent. To log only warnings and errors:

```go
db, _ := rindb.InitRinDB(ctx,
    rindb.WithLogger(logger),
    rindb.WithLogLevel(rindb.LogLevelWarn),
)
```

OpenTelemetry telemetry options operate independently of the logger.

### Table Cache

RinDB keeps recently used SSTables open in a sharded SLRU cache. The cache size and
number of shards are controlled via `WithCacheBytes` and `WithCacheShards`.
Deleted tables leave temporary tombstones, blocking re-admission until the
`WithCacheTombstoneTTL` duration elapses (default 5m).
You can inspect runtime metrics to observe hit/miss ratios and byte usage:

```go
stats := db.TableCacheStats()
fmt.Printf("cache hits=%d misses=%d\n", stats.Hits, stats.Misses)
```

Files that fail verification are quarantined automatically, and the cache evicts
least-recently-used entries while respecting internally pinned tables during
compaction.

## CLI

The interactive CLI under `cmd/main.go` exposes `put`, `get`, `remove`, `range`, `stats`, and `exit` commands. Optional flags such as `--cache-bytes` and `--cache-shards` allow quick cache tuning while experimenting locally:

```bash
go build -o rindb cmd/main.go
./rindb --cache-bytes=67108864 --cache-shards=8
```

## Building and Testing

Run formatting, vet (including the custom span-name analyzer), and static analysis:

```bash
make check
```

Run tests:

```bash
make test
```

Run tests with coverage:

```bash
make test-coverage
```

Run smoke integration tests:

```bash
make test-integration-smoke
```

Run full integration tests:

```bash
make test-integration-full
```

Build the CLI:

```bash
go build -o rindb cmd/main.go
```

## Contributing

Contributions are welcome!

## License

RinDB is licensed under the [MIT License](LICENSE).
