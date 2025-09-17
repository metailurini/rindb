package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/metailurini/rindb"
	"github.com/metailurini/rindb/diffharness"
)

type runConfig struct {
	seed           int64
	n              int
	logPath        string
	iterWalk       int
	crashEvery     int
	telemetryEvery int
	jaeger         string
	opts           []rindb.Option
}

func main() {
	go func() {
		err := http.ListenAndServe("0.0.0.0:6060", nil)
		if err != nil {
			log.Fatalf("Failed to start HTTP server: %v", err)
		} else {
			log.Printf("HTTP server started on :6060")
		}
	}()
	ctx := context.Background()
	seed := flag.Int64("seed", time.Now().UnixNano(), "PRNG seed")
	n := flag.Int("n", -1, "number of operations (-1 for infinite)")
	logPath := flag.String("log", "repro.jsonl", "log file path")
	iterWalk := flag.Int("iter-walk", 64, "max elements to walk when testing iterators")
	crashEvery := flag.Int("crash-every", 0, "crash/recover every N ops")
	telemetryEvery := flag.Int("telemetry-every", 0, "emit telemetry every N ops")
	jaeger := flag.String("jaeger", "", "Jaeger OTLP gRPC endpoint (e.g., localhost:4317)")
	dir := flag.String("dir", "", "work directory (default temp dir)")
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
			}{"no-bloom", []rindb.Option{rindb.WithBloomFalsePositiveRate(0.9)}})

		var wg sync.WaitGroup
		errCh := make(chan error, len(configs))
		for _, c := range configs {
			c := c
			wg.Add(1)
			go func() {
				defer wg.Done()
				runDir := *dir
				if runDir != "" {
					runDir = filepath.Join(runDir, c.label)
				}
				cfg := runConfig{
					seed:           *seed,
					n:              *n,
					logPath:        *logPath + "-" + c.label,
					iterWalk:       *iterWalk,
					crashEvery:     *crashEvery,
					telemetryEvery: *telemetryEvery,
					jaeger:         *jaeger,
					opts:           c.opts,
				}
				if err := runOne(ctx, runDir, cfg); err != nil {
					errCh <- fmt.Errorf("%s run failed: %w", c.label, err)
				}
			}()
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				log.Fatal(err)
			}
		}
		return
	}

	baseCfg := runConfig{
		seed:           *seed,
		n:              *n,
		logPath:        *logPath,
		iterWalk:       *iterWalk,
		crashEvery:     *crashEvery,
		telemetryEvery: *telemetryEvery,
		jaeger:         *jaeger,
	}
	for _, c := range configs {
		runDir := *dir
		if runDir != "" {
			runDir = filepath.Join(runDir, c.label)
		}
		cfg := baseCfg
		cfg.logPath = baseCfg.logPath + "-" + c.label
		cfg.opts = c.opts
		if err := runOne(ctx, runDir, cfg); err != nil {
			log.Fatalf("%s run failed: %v", c.label, err)
		}
	}
}

func runOne(ctx context.Context, dir string, runCfg runConfig) error {
	cleanup := false
	existed := false
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "diffharness")
		if err != nil {
			return err
		}
		cleanup = true
	} else {
		if _, err := os.Stat(dir); err == nil {
			existed = true
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	if cleanup {
		defer os.RemoveAll(dir)
	}
	logPath := runCfg.logPath
	if dir != "" {
		logPath = filepath.Join(dir, logPath)
	}
	dbDir := filepath.Join(dir, "db")
	refPath := filepath.Join(dir, "ref.db")
	baseOpts := []rindb.Option{
		rindb.WithDatabaseDir(dbDir),
		rindb.WithCacheBytes(32 << 20), // 32MiB table cache budget
		rindb.WithLogger(rindb.NewStdLogger(log.Default())),
		rindb.WithLogLevel(rindb.LogLevelDebug),
	}
	if runCfg.jaeger != "" {
		baseOpts = append(baseOpts,
			rindb.WithEnableTelemetry(true),
			rindb.WithExporterEndpoint(runCfg.jaeger),
			rindb.WithExporterInsecure(true),
		)
	}
	db, err := rindb.InitRinDB(ctx, append(baseOpts, runCfg.opts...)...)
	if err != nil {
		return err
	}
	eng := diffharness.NewRinDBEngine(db)
	ref, err := diffharness.OpenSQLiteOracle(refPath)
	if err != nil {
		return err
	}
	var startSeq uint64
	if existed {
		if _, err := os.Stat(logPath); err == nil {
			if startSeq, err = diffharness.Replay(ctx, eng, ref, logPath); err != nil {
				return err
			}
		}
	}
	h, err := diffharness.NewHarness(eng, ref, runCfg.seed, logPath)
	if err != nil {
		return err
	}
	h.Seq = startSeq
	defer func() {
		_ = h.Close()
		_ = eng.Close()
		_ = ref.Close()
	}()

	cfg := diffharness.Cfg{
		KeyLen:    10,
		ValLenMin: 10,
		ValLenMax: 100,
		RangeMax:  64,
		IterWalk:  runCfg.iterWalk,
		Weights: map[diffharness.OpKind]int{
			diffharness.OpPut:   5,
			diffharness.OpDel:   1,
			diffharness.OpGet:   5,
			diffharness.OpRange: 1,
			diffharness.OpSnap:  1,
		},
		CrashEvery:         runCfg.crashEvery,
		TelemetryEvery:     runCfg.telemetryEvery,
		MaxKnownKeys:       100,
		SnapshotReuseEvery: 10,
	}

	if runCfg.crashEvery > 0 {
		h.WithCrash(func(ops int) error {
			if err := eng.Close(); err != nil {
				return err
			}
			if err := ref.Close(); err != nil {
				return err
			}
			db, err := rindb.InitRinDB(ctx, append(baseOpts, runCfg.opts...)...)
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
			h.Snapshots = nil
			return nil
		})
	}

	if runCfg.telemetryEvery > 0 {
		start := time.Now()
		lastOps := 0
		h.WithTelemetry(func(seq uint64, ops int) {
			elapsed := time.Since(start)
			rate := float64(ops-lastOps) / elapsed.Seconds()
			log.Printf("seq=%d ops=%d rate=%.1f ops/s", seq, ops, rate)
			start = time.Now()
			lastOps = ops
		})
	}

	if runCfg.n < 0 {
		return h.RunForever(ctx, cfg)
	}
	return h.Run(ctx, cfg, runCfg.n)
}
