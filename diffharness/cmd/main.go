package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"
	"time"

	rindb "github.com/metailurini/rindb"
	"github.com/metailurini/rindb/diffharness"
)

func main() {
	ctx := context.Background()
	seed := flag.Int64("seed", time.Now().UnixNano(), "PRNG seed")
	n := flag.Int("n", -1, "number of operations (-1 for infinite)")
	logPath := flag.String("log", "repro.jsonl", "log file path")
	crashEvery := flag.Int("crash-every", 0, "crash/recover every N ops")
	telemetryEvery := flag.Int("telemetry-every", 0, "emit telemetry every N ops")
	jaeger := flag.String("jaeger", "", "Jaeger OTLP gRPC endpoint (e.g., localhost:4317)")
	metamorphic := flag.Bool("metamorphic", false, "run metamorphic config variants")
	flag.Parse()

	configs := []struct {
		label string
		opts  []rindb.Option
	}{{"default", nil}}
	if *metamorphic {
		configs = append(configs,
			struct {
				label string
				opts  []rindb.Option
			}{"small-mem", []rindb.Option{rindb.WithMaxMemtableSize(64)}},
			struct {
				label string
				opts  []rindb.Option
			}{"no-bloom", []rindb.Option{rindb.WithBloomFalsePositiveRate(0)}})
	}

	for _, c := range configs {
		if err := runOne(ctx, *seed, *n, *logPath+"-"+c.label, *crashEvery, *telemetryEvery, *jaeger, c.opts); err != nil {
			log.Fatalf("%s run failed: %v", c.label, err)
		}
	}
}

func runOne(ctx context.Context, seed int64, n int, logPath string, crashEvery, telemetryEvery int, jaeger string, opts []rindb.Option) error {
	dir, err := os.MkdirTemp("", "diffharness")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	dbDir := filepath.Join(dir, "db")
	refPath := filepath.Join(dir, "ref.db")
	baseOpts := []rindb.Option{rindb.WithDatabaseDir(dbDir)}
	if jaeger != "" {
		baseOpts = append(baseOpts,
			rindb.WithEnableTelemetry(true),
			rindb.WithExporterEndpoint(jaeger),
			rindb.WithExporterInsecure(true),
		)
	}
	db, err := rindb.InitRinDB(ctx, append(baseOpts, opts...)...)
	if err != nil {
		return err
	}
	eng := diffharness.NewRinDBEngine(db)
	ref, err := diffharness.OpenSQLiteOracle(refPath)
	if err != nil {
		return err
	}
	h, err := diffharness.NewHarness(eng, ref, seed, logPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = h.Close()
		_ = eng.Close()
		_ = ref.Close()
	}()

	cfg := diffharness.Cfg{
		KeyLen:    4,
		ValLenMin: 1,
		ValLenMax: 8,
		RangeMax:  32,
		Weights: map[diffharness.OpKind]int{
			diffharness.OpPut:   1,
			diffharness.OpDel:   1,
			diffharness.OpGet:   1,
			diffharness.OpRange: 1,
			diffharness.OpSnap:  1,
		},
		CrashEvery:     crashEvery,
		TelemetryEvery: telemetryEvery,
	}

	if crashEvery > 0 {
		h.SetCrashHook(func() error {
			if err := eng.Close(); err != nil {
				return err
			}
			if err := ref.Close(); err != nil {
				return err
			}
			db, err := rindb.InitRinDB(ctx, append(baseOpts, opts...)...)
			if err != nil {
				return err
			}
			eng = diffharness.NewRinDBEngine(db)
			r, err := diffharness.OpenSQLiteOracle(refPath)
			if err != nil {
				return err
			}
			h.My = eng
			h.Ref = r
			ref = r
			return nil
		})
	}

	if telemetryEvery > 0 {
		start := time.Now()
		lastOps := 0
		h.SetTelemetryHook(func(seq uint64, ops int) {
			elapsed := time.Since(start)
			rate := float64(ops-lastOps) / elapsed.Seconds()
			log.Printf("seq=%d ops=%d rate=%.1f ops/s", seq, ops, rate)
			start = time.Now()
			lastOps = ops
		})
	}

	if n < 0 {
		return h.RunForever(ctx, cfg)
	}
	return h.Run(ctx, cfg, n)
}
