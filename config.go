package rindb

import (
	"context"
	"runtime"
	"time"
)

// NewWALFunc defines the signature for a function that creates a WAL instance.
type NewWALFunc func(ctx context.Context, cfg Config) (*wal, error)

// NewSSTableManagerFunc defines the signature for a function that creates an SSTableManager instance.
//
// The versionSet parameter provides metadata about existing SSTables. Callers
// should pass the same instance used elsewhere in the database so the manager
// can operate on consistent state.
type NewSSTableManagerFunc func(ctx context.Context, cfg Config, vs *versionSet, mw manifestWriter) (*ssTableManager, error)

type Config struct {
	// databaseDir specifies the directory where WAL and SSTables are stored
	databaseDir string

	// enableSSTableMmap gates mmap-backed SSTable reads on supported OSes.
	enableSSTableMmap bool

	// repairMode forces a full directory scan of SSTables during startup,
	// bypassing the manifest's versionSet.
	repairMode bool

	// maxMemtableSize triggers memtable flush to SSTable when this number of entries is reached
	maxMemtableSize uint

	// level0CompactionThreshold triggers compaction when level 0 has more than threshold
	level0CompactionThreshold int

	// baseCompactionSizeMB is the base size in MB for level 1 compaction threshold.
	// Level N threshold = baseCompactionSizeMB * (levelSizeMultiplier ^ N) * 1024 * 1024 bytes.
	baseCompactionSizeMB int

	// levelSizeMultiplier is the multiplier for calculating compaction thresholds for levels > 0.
	levelSizeMultiplier int

	// writeRateTrigger defines the write throughput in writes/sec at which
	// background compaction should be attempted.
	writeRateTrigger float64

	// ioLoadMax specifies the maximum fraction (0-1) of disk utilization
	// allowed for dynamic compaction triggers.
	ioLoadMax float64

	// bloomFalsePositiveRate sets the Bloom filter’s false positive rate.
	bloomFalsePositiveRate float64

	// skipListDefaultLevel is initial height of the skip list
	skipListDefaultLevel uint

	// skipListMaxLevel is maximum height of the skip list
	skipListMaxLevel uint

	// skipListP is probability for skip list level promotion
	skipListP float64

	// enableTelemetry toggles OpenTelemetry collection
	enableTelemetry bool

	// exporterEndpoint configures OTLP gRPC endpoint
	exporterEndpoint string

	// exporterInsecure disables TLS for the OTLP exporter
	exporterInsecure bool

	// telemetrySamplingRate sets the sampling rate for traces (0.0 - 1.0)
	telemetrySamplingRate float64

	// newWALFunc allows custom WAL initialization logic.
	newWALFunc NewWALFunc

	// newSSTableManagerFunc allows custom SSTableManager initialization logic.
	newSSTableManagerFunc NewSSTableManagerFunc

	// fileNumberAllocator provides sequential identifiers for WAL and SSTable files.
	fileNumberAllocator *fileNumberAllocator

	// newManifestWriterFunc allows custom manifestWriter initialization.
	newManifestWriterFunc func(ctx context.Context, path string) (manifestWriter, error)

	// manifestSizeThreshold triggers manifest rotation once the MANIFEST
	// file grows beyond this size in bytes.
	manifestSizeThreshold int64

	// table cache configuration
	cacheBytes             int64
	cacheShards            int
	cacheProbationFraction float64
	cacheCorruptTTL        time.Duration
	cacheTombstoneTTL      time.Duration

	// fdLimiter limits concurrent open file descriptors. A nil value means
	// no limit is enforced, which may lead to resource exhaustion on busy
	// systems. Provide an implementation to bound FD usage.
	fdLimiter FDLimiter

	// logger receives log messages. Defaults to a no-op implementation.
	logger Logger

	// logLevel controls which messages are emitted via the logger.
	logLevel LogLevel
}

// Option defines a functional option type for Config.
type Option func(*Config)

// NewConfig creates a new Config with default values and applies the provided options.
func NewConfig(opts ...Option) Config {
	cfg := DefaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.Validate()
	return cfg
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() Config {
	return Config{
		databaseDir:               "rindat",
		enableSSTableMmap:         runtime.GOOS == "linux" || runtime.GOOS == "darwin" || runtime.GOOS == "windows",
		maxMemtableSize:           1000,
		level0CompactionThreshold: 2,
		baseCompactionSizeMB:      10, // Default: Level 1 threshold = 10MB * (10^1) = 100MB
		levelSizeMultiplier:       10, // Default: Level N threshold = base * (multiplier^N)
		writeRateTrigger:          0,
		ioLoadMax:                 0,
		bloomFalsePositiveRate:    0.01,
		skipListDefaultLevel:      2,
		skipListMaxLevel:          32,
		skipListP:                 0.5,
		enableTelemetry:           false,
		exporterEndpoint:          "",
		exporterInsecure:          false,
		telemetrySamplingRate:     0.1, // Default to sample 10% of traces
		newWALFunc:                DefaultNewWALFunc,
		newSSTableManagerFunc:     InitSSTableManager,
		fileNumberAllocator:       newFileNumberAllocator(1),
		newManifestWriterFunc:     newManifestWriter,
		manifestSizeThreshold:     1 << 20, // 1MiB
		repairMode:                false,
		cacheBytes:                64 << 20, // 64MiB table cache budget
		cacheShards:               64,
		cacheProbationFraction:    0.25,
		cacheCorruptTTL:           5 * time.Minute,
		cacheTombstoneTTL:         0,
		fdLimiter:                 noopFDLimiter{},
		logger:                    nopLogger{},
		logLevel:                  LogLevelWarn,
	}
}

// Validate checks if the Config has valid values.
func (c Config) Validate() {
	if c.databaseDir == "" {
		panic("databaseDir cannot be empty")
	}
	if c.maxMemtableSize == 0 {
		panic("maxMemtableSize cannot be zero")
	}
	if c.level0CompactionThreshold <= 0 {
		panic("level0CompactionThreshold must be greater than zero")
	}
	if c.baseCompactionSizeMB <= 0 {
		panic("baseCompactionSizeMB must be greater than zero")
	}
	if c.levelSizeMultiplier <= 0 {
		panic("levelSizeMultiplier must be greater than zero")
	}
	if c.writeRateTrigger < 0 {
		panic("writeRateTrigger must be >= 0")
	}
	if c.ioLoadMax < 0 || c.ioLoadMax > 1 {
		panic("ioLoadMax must be between 0 and 1")
	}
	if c.bloomFalsePositiveRate <= 0 || c.bloomFalsePositiveRate >= 1 {
		panic("bloomFalsePositiveRate must be between 0 and 1")
	}
	if c.skipListDefaultLevel == 0 {
		panic("skipListDefaultLevel cannot be zero")
	}
	if c.skipListMaxLevel == 0 {
		panic("skipListMaxLevel cannot be zero")
	}
	if c.skipListDefaultLevel > c.skipListMaxLevel {
		panic("skipListDefaultLevel cannot be greater than skipListMaxLevel")
	}
	if c.skipListP <= 0 || c.skipListP >= 1 {
		panic("skipListP must be between 0 and 1")
	}
	if c.enableTelemetry && c.exporterEndpoint == "" {
		panic("exporterEndpoint cannot be empty if telemetry is enabled")
	}
	if c.telemetrySamplingRate < 0 || c.telemetrySamplingRate > 1 {
		panic("telemetrySamplingRate must be between 0 and 1")
	}
	if c.newWALFunc == nil {
		panic("newWALFunc cannot be nil")
	}
	if c.newSSTableManagerFunc == nil {
		panic("newSSTableManagerFunc cannot be nil")
	}
	if c.fileNumberAllocator == nil {
		panic("fileNumberAllocator cannot be nil")
	}
	if c.newManifestWriterFunc == nil {
		panic("newManifestWriterFunc cannot be nil")
	}
	if c.manifestSizeThreshold <= 0 {
		panic("manifestSizeThreshold must be greater than zero")
	}
	if c.cacheBytes < 0 {
		panic("cacheBytes must be >= 0")
	}
	if c.cacheShards <= 0 {
		panic("cacheShards must be > 0")
	}
	if c.cacheProbationFraction <= 0 || c.cacheProbationFraction >= 1 {
		panic("cacheProbationFraction must be between 0 and 1")
	}
	if c.cacheCorruptTTL <= 0 {
		panic("cacheCorruptTTL must be > 0")
	}
	if c.fdLimiter == nil {
		panic("fdLimiter cannot be nil")
	}
	if c.cacheTombstoneTTL < 0 {
		panic("cacheTombstoneTTL must be >= 0")
	}
	if c.logger == nil {
		panic("logger cannot be nil")
	}
	if c.logLevel < LogLevelDebug || c.logLevel > LogLevelError {
		panic("invalid logLevel")
	}
}

func (c Config) scopedLogger() scopedLogger {
	return newScopedLogger(c.logger, c.logLevel)
}

func WithConfig(cfg Config) Option {
	return func(c *Config) { *c = cfg }
}

// Option functions
func WithDatabaseDir(dir string) Option {
	return func(c *Config) { c.databaseDir = dir }
}

// WithRepairMode enables repair mode which scans SSTables from disk on startup.
func WithRepairMode(v bool) Option {
	return func(c *Config) { c.repairMode = v }
}

// WithSSTableMmap toggles mmap-backed SSTable reads.
func WithSSTableMmap(enabled bool) Option {
	return func(c *Config) { c.enableSSTableMmap = enabled }
}

// WithCacheBytes sets the total byte budget for the table cache.
func WithCacheBytes(v int64) Option {
	return func(c *Config) { c.cacheBytes = v }
}

// WithCacheShards sets the number of shards for the table cache.
func WithCacheShards(v int) Option {
	return func(c *Config) { c.cacheShards = v }
}

// WithCacheProbationFraction sets the probation segment fraction for the table cache.
func WithCacheProbationFraction(v float64) Option {
	return func(c *Config) { c.cacheProbationFraction = v }
}

// WithCacheCorruptTTL sets the corruption quarantine duration for the table cache.
func WithCacheCorruptTTL(d time.Duration) Option {
	return func(c *Config) { c.cacheCorruptTTL = d }
}

// WithCacheTombstoneTTL sets the tombstone duration for the table cache.
func WithCacheTombstoneTTL(d time.Duration) Option {
	return func(c *Config) { c.cacheTombstoneTTL = d }
}

// WithFDLimiter sets the file descriptor limiter used by the table cache. The
// provided limiter must not be nil. Omit this option to use the default
// unlimited implementation.
func WithFDLimiter(l FDLimiter) Option {
	return func(c *Config) { c.fdLimiter = l }
}

// WithLogger sets the logger used by RinDB. A nil logger results in a no-op logger.
func WithLogger(l Logger) Option {
	return func(c *Config) {
		if l == nil {
			c.logger = nopLogger{}
			return
		}
		c.logger = l
	}
}

// WithLogLevel sets the minimum log level emitted through the logger.
func WithLogLevel(level LogLevel) Option {
	return func(c *Config) { c.logLevel = level }
}

// WithMaxMemtableSize sets the maximum number of entries allowed in the memtable before flushing.
func WithMaxMemtableSize(size uint) Option {
	return func(c *Config) { c.maxMemtableSize = size }
}

// WithLevel0CompactionThreshold configures how many level 0 files trigger a compaction.
func WithLevel0CompactionThreshold(threshold int) Option {
	return func(c *Config) { c.level0CompactionThreshold = threshold }
}

// WithBaseCompactionSizeMB sets the base size in MB for level 1 compaction.
func WithBaseCompactionSizeMB(size int) Option {
	return func(c *Config) { c.baseCompactionSizeMB = size }
}

// WithLevelSizeMultiplier specifies the multiplier for computing higher level sizes.
func WithLevelSizeMultiplier(multiplier int) Option {
	return func(c *Config) { c.levelSizeMultiplier = multiplier }
}

// WithWriteRateTrigger sets the write throughput threshold to attempt background compaction.
func WithWriteRateTrigger(trigger float64) Option {
	return func(c *Config) { c.writeRateTrigger = trigger }
}

// WithIOLoadMax limits the allowed disk utilization fraction for dynamic compaction triggers.
func WithIOLoadMax(max float64) Option {
	return func(c *Config) { c.ioLoadMax = max }
}

// WithBloomFalsePositiveRate configures the Bloom filter false positive probability.
func WithBloomFalsePositiveRate(rate float64) Option {
	return func(c *Config) { c.bloomFalsePositiveRate = rate }
}

// WithSkipListDefaultLevel sets the initial height of the skip list.
func WithSkipListDefaultLevel(level uint) Option {
	return func(c *Config) { c.skipListDefaultLevel = level }
}

// WithSkipListMaxLevel sets the maximum height of the skip list.
func WithSkipListMaxLevel(maxLevel uint) Option {
	return func(c *Config) { c.skipListMaxLevel = maxLevel }
}

// WithSkipListP sets the probability for skip list level promotion.
func WithSkipListP(p float64) Option {
	return func(c *Config) { c.skipListP = p }
}

// WithManifestSizeThreshold defines the manifest size in bytes that triggers rotation.
func WithManifestSizeThreshold(threshold int64) Option {
	return func(c *Config) { c.manifestSizeThreshold = threshold }
}

// WithEnableTelemetry toggles OpenTelemetry metrics and traces.
func WithEnableTelemetry(enable bool) Option {
	return func(c *Config) { c.enableTelemetry = enable }
}

// WithExporterEndpoint sets the OTLP gRPC endpoint for telemetry export.
func WithExporterEndpoint(endpoint string) Option {
	return func(c *Config) { c.exporterEndpoint = endpoint }
}

// WithExporterInsecure disables TLS for the OTLP exporter.
func WithExporterInsecure(insecure bool) Option {
	return func(c *Config) { c.exporterInsecure = insecure }
}

// WithTelemetrySamplingRate configures the trace sampling rate (0.0-1.0).
func WithTelemetrySamplingRate(rate float64) Option {
	return func(c *Config) { c.telemetrySamplingRate = rate }
}

// WithNewWALFunc overrides the default WAL constructor.
func WithNewWALFunc(f NewWALFunc) Option {
	return func(c *Config) { c.newWALFunc = f }
}

// WithNewSSTableManagerFunc overrides the default SSTableManager constructor.
func WithNewSSTableManagerFunc(f NewSSTableManagerFunc) Option {
	return func(c *Config) { c.newSSTableManagerFunc = f }
}
