# RinDB

<p align="center">
  <img src='./assets/mascot.jpeg' width='70%'>
</p>

**RinDB** is a lightweight, embeddable key-value database inspired by Log-Structured Merge (LSM) trees and LevelDB. It is designed for simplicity, performance, and extensibility, making it suitable for applications requiring fast, persistent storage.

🚧 *The project is under active development. Expect incomplete documentation and potential unexpected behavior.* 🚧

## Features

- **LSM-Based Architecture**: Efficient write-heavy workloads with a memory table (Memtable) and on-disk SSTables.
- **Skip List Implementation**: Fast in-memory key lookups using a probabilistic skip list data structure.
- **Bloom Filters**: Reduces unnecessary disk reads for non-existent keys.
- **Write-Ahead Logging (WAL)**: Ensures durability and crash recovery.
- **Compaction**: Background process to manage disk space and optimize read performance.
- **Configurable**: Customize database behavior with options like memtable size, compaction thresholds, and bloom filter settings.
- **Concurrent Access**: Thread-safe operations with transaction support.

## Installation

RinDB is written in Go and requires Go 1.21.6 or later. To include it in your project:

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

### Custom Configuration

```go
ctx := context.Background()
db, err := rindb.InitRinDB(ctx,
    rindb.WithDatabaseDir("./mydb"),
    rindb.WithMaxMemtableSize(1024*1024), // 1MB
    rindb.WithBloomFalsePositiveRate(0.01),
)
if err != nil {
    fmt.Println("Error:", err)
    return
}
defer db.Close()
```

## Building and Testing

Run tests with coverage:

```bash
make test-coverage
```

Build the CLI:

```bash
go build -o rindb cmd/main.go
```

## Contributing

Contributions are welcome!

## License

RinDB is licensed under the [MIT License](LICENSE).
