package rindb

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
)

const (
	benchmarkMemtableSize          = 64 << 20 // 64 MiB
	benchmarkLevel0CompactionFiles = 1 << 20  // A large number to effectively disable L0 compaction
)

var (
	benchBytesSink  Bytes
	benchRecordSink Record
)

func benchmarkOptions(b *testing.B) []Option {
	b.Helper()

	opts := []Option{WithDatabaseDir(b.TempDir())}
	if isTempoEndpointResolvable() {
		opts = append(opts,
			WithEnableTelemetry(true),
			WithExporterEndpoint("tempo.magpie-gopher.ts.net:4317"),
			WithExporterInsecure(true),
			WithTelemetrySamplingRate(1.0),
		)
	}
	return opts
}

func setupBenchmarkDB(b *testing.B, opts ...Option) (*Rindb, func()) {
	b.Helper()

	benchmarkOpts := append([]Option{}, benchmarkOptions(b)...)
	benchmarkOpts = append(benchmarkOpts,
		WithMaxMemtableSize(benchmarkMemtableSize),
		WithLevel0CompactionThreshold(benchmarkLevel0CompactionFiles),
	)
	benchmarkOpts = append(benchmarkOpts, opts...)

	rin, err := InitRinDB(context.Background(), benchmarkOpts...)
	if err != nil {
		b.Fatalf("failed to initialize RinDB: %v", err)
	}

	cleanup := func() {
		if err := rin.Close(); err != nil && !errors.Is(err, ErrDatabaseClosed) {
			b.Fatalf("failed to close RinDB: %v", err)
		}
	}

	return rin, cleanup
}

func makeBenchKey(i int) Bytes {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(i))
	return Bytes(key)
}

func setupBenchmarkDBWithData(b *testing.B, keyCount int) (*Rindb, []Bytes, func()) {
	b.Helper()

	rin, cleanup := setupBenchmarkDB(b)

	ctx := context.Background()
	keys := make([]Bytes, keyCount)
	value := Bytes([]byte("value"))

	for i := range keys {
		key := makeBenchKey(i)
		keys[i] = key
		if err := rin.Put(ctx, key, value); err != nil {
			if closeErr := rin.Close(); closeErr != nil && !errors.Is(closeErr, ErrDatabaseClosed) {
				b.Logf("failed to close RinDB during setup: %v", closeErr)
			}
			b.Fatalf("failed to prepare key %d: %v", i, err)
		}
	}

	return rin, keys, cleanup
}

func BenchmarkRindb_Put(b *testing.B) {
	ctx := context.Background()
	rin, cleanup := setupBenchmarkDB(b)
	b.Cleanup(cleanup)

	value := Bytes([]byte("value"))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := rin.Put(ctx, makeBenchKey(i), value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

func BenchmarkRindb_Get(b *testing.B) {
	const keyCount = 8192
	ctx := context.Background()
	rin, keys, cleanup := setupBenchmarkDBWithData(b, keyCount)
	b.Cleanup(cleanup)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := keys[i%keyCount]
		val, err := rin.Get(ctx, key)
		if err != nil {
			b.Fatalf("Get failed: %v", err)
		}
		benchBytesSink = val
	}
}

func BenchmarkRindb_Remove(b *testing.B) {
	const keyCount = 8192
	ctx := context.Background()
	rin, keys, cleanup := setupBenchmarkDBWithData(b, keyCount)
	b.Cleanup(cleanup)

	value := Bytes([]byte("value"))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := keys[i%keyCount]
		b.StopTimer()
		if err := rin.Put(ctx, key, value); err != nil {
			b.Fatalf("Put failed during setup: %v", err)
		}
		b.StartTimer()

		if err := rin.Remove(ctx, key); err != nil {
			b.Fatalf("Remove failed: %v", err)
		}
	}
}

func BenchmarkRindb_IRangeAscending(b *testing.B) {
	const (
		keyCount   = 8192
		startIndex = 2048
		endIndex   = 6143
	)
	ctx := context.Background()
	rin, keys, cleanup := setupBenchmarkDBWithData(b, keyCount)
	b.Cleanup(cleanup)

	startKey := keys[startIndex]
	endKey := keys[endIndex]

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iter, err := rin.IRange(ctx, startKey, endKey)
		if err != nil {
			b.Fatalf("IRange failed: %v", err)
		}

		for {
			rec, err := iter.Next()
			if errors.Is(err, EOI) {
				break
			}
			if err != nil {
				if closeErr := iter.Close(); closeErr != nil {
					b.Logf("error closing iterator after a Next() error: %v", closeErr)
				}
				b.Fatalf("IRange iteration failed: %v", err)
			}
			benchRecordSink = rec
		}

		if err := iter.Close(); err != nil {
			b.Fatalf("IRange close failed: %v", err)
		}
	}
}

func BenchmarkRindb_IRangeDescending(b *testing.B) {
	const (
		keyCount   = 8192
		startIndex = 2048
		endIndex   = 6143
	)
	ctx := context.Background()
	rin, keys, cleanup := setupBenchmarkDBWithData(b, keyCount)
	b.Cleanup(cleanup)

	startKey := keys[startIndex]
	endKey := keys[endIndex]

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iter, err := rin.IRange(ctx, startKey, endKey, IRangeOrder(RangeDesc))
		if err != nil {
			b.Fatalf("IRange failed: %v", err)
		}

		for {
			rec, err := iter.Next()
			if errors.Is(err, EOI) {
				break
			}
			if err != nil {
				if closeErr := iter.Close(); closeErr != nil {
					b.Logf("error closing iterator after a Next() error: %v", closeErr)
				}
				b.Fatalf("IRange iteration failed: %v", err)
			}
			benchRecordSink = rec
		}

		if err := iter.Close(); err != nil {
			b.Fatalf("IRange close failed: %v", err)
		}
	}
}
