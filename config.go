package rindb

type Config struct {
	// databaseDir specifies the directory where WAL and SSTables are stored
	databaseDir string

	// maxMemtableSize triggers memtable flush to SSTable when this number of entries is reached
	maxMemtableSize uint

	// level0CompactionThreshold triggers compaction when level 0 has more than threshold
	level0CompactionThreshold int

	// bloomFalsePositiveRate sets the Bloom filter’s false positive rate.
	bloomFalsePositiveRate float64

	// skipListDefaultLevel is initial height of the skip list
	skipListDefaultLevel uint

	// skipListMaxLevel is maximum height of the skip list
	skipListMaxLevel uint

	// skipListP is probability for skip list level promotion
	skipListP float64
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
		bloomFalsePositiveRate:    0.01,
		skipListDefaultLevel:      2,
		skipListMaxLevel:          32,
		skipListP:                 0.5,
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
