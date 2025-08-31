package rindb

import (
	"context"
)

// NewWALFunc defines the signature for a function that creates a WAL instance.
type NewWALFunc func(ctx context.Context, cfg Config) (*WAL, error)

// NewSSTableManagerFunc defines the signature for a function that creates an SSTableManager instance.
type NewSSTableManagerFunc func(ctx context.Context, cfg Config) (*SSTableManager, error)

type Config struct {
	// databaseDir specifies the directory where WAL and SSTables are stored
	databaseDir string

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
	fileNumberAllocator *FileNumberAllocator

	// newFileNumberAllocatorFunc constructs a FileNumberAllocator seeded with the given start.
	newFileNumberAllocatorFunc func(start uint64) *FileNumberAllocator

	// newManifestWriterFunc allows custom ManifestWriter initialization.
	newManifestWriterFunc func(ctx context.Context, path string) (ManifestWriter, error)
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
		databaseDir:                "rindat",
		maxMemtableSize:            1000,
		level0CompactionThreshold:  2,
		baseCompactionSizeMB:       10, // Default: Level 1 threshold = 10MB * (10^1) = 100MB
		levelSizeMultiplier:        10, // Default: Level N threshold = base * (multiplier^N)
		writeRateTrigger:           0,
		ioLoadMax:                  0,
		bloomFalsePositiveRate:     0.01,
		skipListDefaultLevel:       2,
		skipListMaxLevel:           32,
		skipListP:                  0.5,
		enableTelemetry:            false,
		exporterEndpoint:           "",
		exporterInsecure:           false,
		telemetrySamplingRate:      0.1, // Default to sample 10% of traces
		newWALFunc:                 DefaultNewWALFunc,
		newSSTableManagerFunc:      InitSSTableManager,
		fileNumberAllocator:        NewFileNumberAllocator(1),
		newFileNumberAllocatorFunc: NewFileNumberAllocator,
		newManifestWriterFunc:      NewManifestWriter,
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
	if c.newFileNumberAllocatorFunc == nil {
		panic("newFileNumberAllocatorFunc cannot be nil")
	}
	if c.newManifestWriterFunc == nil {
		panic("newManifestWriterFunc cannot be nil")
	}
}

func WithConfig(cfg Config) Option {
	return func(c *Config) { *c = cfg }
}

// Option functions
func WithDatabaseDir(dir string) Option {
	return func(c *Config) { c.databaseDir = dir }
}

func WithMaxMemtableSize(size uint) Option {
	return func(c *Config) { c.maxMemtableSize = size }
}

func WithLevel0CompactionThreshold(threshold int) Option {
	return func(c *Config) { c.level0CompactionThreshold = threshold }
}

func WithBaseCompactionSizeMB(size int) Option {
	return func(c *Config) { c.baseCompactionSizeMB = size }
}

func WithLevelSizeMultiplier(multiplier int) Option {
	return func(c *Config) { c.levelSizeMultiplier = multiplier }
}

func WithWriteRateTrigger(trigger float64) Option {
	return func(c *Config) { c.writeRateTrigger = trigger }
}

func WithIOLoadMax(max float64) Option {
	return func(c *Config) { c.ioLoadMax = max }
}

func WithBloomFalsePositiveRate(rate float64) Option {
	return func(c *Config) { c.bloomFalsePositiveRate = rate }
}

func WithSkipListDefaultLevel(level uint) Option {
	return func(c *Config) { c.skipListDefaultLevel = level }
}

func WithSkipListMaxLevel(maxLevel uint) Option {
	return func(c *Config) { c.skipListMaxLevel = maxLevel }
}

func WithSkipListP(p float64) Option {
	return func(c *Config) { c.skipListP = p }
}

func WithEnableTelemetry(enable bool) Option {
	return func(c *Config) { c.enableTelemetry = enable }
}

func WithExporterEndpoint(endpoint string) Option {
	return func(c *Config) { c.exporterEndpoint = endpoint }
}

func WithExporterInsecure(insecure bool) Option {
	return func(c *Config) { c.exporterInsecure = insecure }
}

func WithTelemetrySamplingRate(rate float64) Option {
	return func(c *Config) { c.telemetrySamplingRate = rate }
}

func WithNewWALFunc(f NewWALFunc) Option {
	return func(c *Config) { c.newWALFunc = f }
}

func WithNewSSTableManagerFunc(f NewSSTableManagerFunc) Option {
	return func(c *Config) { c.newSSTableManagerFunc = f }
}

// WithFileNumberAllocator sets a custom FileNumberAllocator.
func WithFileNumberAllocator(a *FileNumberAllocator) Option {
	return func(c *Config) { c.fileNumberAllocator = a }
}

// WithNewFileNumberAllocatorFunc sets the constructor for FileNumberAllocator.
func WithNewFileNumberAllocatorFunc(f func(start uint64) *FileNumberAllocator) Option {
	return func(c *Config) { c.newFileNumberAllocatorFunc = f }
}

// WithNewManifestWriterFunc sets the constructor for ManifestWriter.
func WithNewManifestWriterFunc(f func(ctx context.Context, path string) (ManifestWriter, error)) Option {
	return func(c *Config) { c.newManifestWriterFunc = f }
}
