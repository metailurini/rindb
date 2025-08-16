package rindb

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

	// bloomFalsePositiveRate sets the Bloom filter’s false positive rate.
	bloomFalsePositiveRate float64

	// skipListDefaultLevel is initial height of the skip list
	skipListDefaultLevel uint

	// skipListMaxLevel is maximum height of the skip list
	skipListMaxLevel uint

	// skipListP is probability for skip list level promotion
	skipListP float64

	// EnableTelemetry toggles OpenTelemetry collection
	EnableTelemetry bool

	// ExporterEndpoint configures OTLP gRPC endpoint
	ExporterEndpoint string

	// ExporterInsecure disables TLS for the OTLP exporter
	ExporterInsecure bool
}

// Option defines a functional option type for Config.
type Option func(*Config)

// NewConfig creates a new Config with default values and applies the provided options.
func NewConfig(opts ...Option) Config {
	cfg := DefaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() Config {
	return Config{
		databaseDir:               "rindat",
		maxMemtableSize:           1000,
		level0CompactionThreshold: 2,
		baseCompactionSizeMB:      10, // Default: Level 1 threshold = 10MB * (10^1) = 100MB
		levelSizeMultiplier:       10, // Default: Level N threshold = base * (multiplier^N)
		bloomFalsePositiveRate:    0.01,
		skipListDefaultLevel:      2,
		skipListMaxLevel:          32,
		skipListP:                 0.5,
		EnableTelemetry:           false,
		ExporterEndpoint:          "",
		ExporterInsecure:          false,
	}
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
	return func(c *Config) { c.EnableTelemetry = enable }
}

func WithExporterEndpoint(endpoint string) Option {
	return func(c *Config) { c.ExporterEndpoint = endpoint }
}

func WithExporterInsecure(insecure bool) Option {
	return func(c *Config) { c.ExporterInsecure = insecure }
}
