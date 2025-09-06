# RinDB

<p align="center">
  <img src='./assets/mascot.png' width='70%'>
</p>

<p align="center">
    <img src='https://img.shields.io/codecov/c/github/metailurini/rindb.svg?maxAge=2592000'>
    <img src='https://qlty.sh/gh/metailurini/projects/rindb/maintainability.svg'>
    <img src='https://img.shields.io/github/license/metailurini/rindb'>
</p>


**RinDB** is a lightweight, embeddable key-value database inspired by Log-Structured Merge (LSM) trees and LevelDB. It is designed for simplicity, performance, and extensibility, making it suitable for applications requiring fast, persistent storage.

🚧 *The project is under active development. Expect incomplete documentation and potential unexpected behavior.* 🚧

## Features

- **LSM-Based Architecture**: Efficient write-heavy workloads with a memory table (Memtable) and on-disk SSTables.
- **Skip List Implementation**: Fast in-memory key lookups using a probabilistic skip list data structure.
- **Bloom Filters**: Reduces unnecessary disk reads for non-existent keys.
- **Write-Ahead Logging (WAL)**: Ensures durability and crash recovery.
- **Compaction**: Background process to manage disk space and optimize read performance. Compaction now writes directly to new SSTables via the builder without buffering into an intermediate memtable.
- **Configurable**: Customize database behavior with options like memtable size, compaction thresholds, and bloom filter settings.
- **Concurrent Access**: Thread-safe operations with transaction support.
- **Range Queries**: Efficient retrieval of key-value pairs within a specified key range.
- **Telemetry**: OpenTelemetry integration for metrics and tracing to monitor database performance.
- **Table Cache**: Reuses open SSTables via a sharded SLRU cache.

## Installation

RinDB is written in Go and requires Go 1.25.0 or later. To include it in your project:

```bash
go get github.com/metailurini/rindb
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
    "rindb"
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
it.Close()
```

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
)
if err != nil {
    fmt.Println("Error:", err)
    return
}
defer db.Close()
```

The sample CLI accepts `--cache-bytes` and `--cache-shards` flags to tune the cache.

## Building and Testing

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
