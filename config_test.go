package rindb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestConfig_Default verifies that DefaultConfig returns the expected default values.
func TestConfig_Default(t *testing.T) {
	cfg := DefaultConfig()

	tests := []struct {
		name string
		got  any
		want any
	}{
		{"databaseDir", cfg.databaseDir, "rindat"},
		{"maxMemtableSize", cfg.maxMemtableSize, uint(1000)},
		{"level0CompactionThreshold", cfg.level0CompactionThreshold, 2},
		{"baseCompactionSizeMB", cfg.baseCompactionSizeMB, 10},
		{"levelSizeMultiplier", cfg.levelSizeMultiplier, 10},
		{"repairMode", cfg.repairMode, false},
		{"bloomFalsePositiveRate", cfg.bloomFalsePositiveRate, 0.01},
		{"skipListDefaultLevel", cfg.skipListDefaultLevel, uint(2)},
		{"skipListMaxLevel", cfg.skipListMaxLevel, uint(32)},
		{"skipListP", cfg.skipListP, 0.5},
		{"EnableTelemetry", cfg.enableTelemetry, false},
		{"ExporterEndpoint", cfg.exporterEndpoint, ""},
		{"ExporterInsecure", cfg.exporterInsecure, false},
		{"cacheBytes", cfg.cacheBytes, int64(64 << 20)},
		{"cacheShards", cfg.cacheShards, 64},
		{"cacheProbationFraction", cfg.cacheProbationFraction, 0.25},
		{"cacheCorruptTTL", cfg.cacheCorruptTTL, 5 * time.Minute},
		{"cacheTombstoneTTL", cfg.cacheTombstoneTTL, time.Duration(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("DefaultConfig() %s = %v, want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}

// TestConfig_NewWithOptions tests that NewConfig applies options correctly.
func TestConfig_NewWithOptions(t *testing.T) {
	tests := []struct {
		name   string
		opts   []Option
		verify func(*testing.T, Config)
	}{
		{
			name: "WithDatabaseDir",
			opts: []Option{WithDatabaseDir("/custom/path")},
			verify: func(t *testing.T, cfg Config) {
				if cfg.databaseDir != "/custom/path" {
					t.Errorf("databaseDir = %v, want %v", cfg.databaseDir, "/custom/path")
				}
			},
		},
		{
			name: "WithRepairMode",
			opts: []Option{WithRepairMode(true)},
			verify: func(t *testing.T, cfg Config) {
				if !cfg.repairMode {
					t.Errorf("repairMode = %v, want %v", cfg.repairMode, true)
				}
			},
		},
		{
			name: "WithMaxMemtableSize",
			opts: []Option{WithMaxMemtableSize(5000)},
			verify: func(t *testing.T, cfg Config) {
				if cfg.maxMemtableSize != 5000 {
					t.Errorf("maxMemtableSize = %v, want %v", cfg.maxMemtableSize, 5000)
				}
			},
		},
		{
			name: "WithLevel0CompactionThreshold",
			opts: []Option{WithLevel0CompactionThreshold(10)},
			verify: func(t *testing.T, cfg Config) {
				if cfg.level0CompactionThreshold != 10 {
					t.Errorf("level0CompactionThreshold = %v, want %v", cfg.level0CompactionThreshold, 10)
				}
			},
		},
		{
			name: "WithBaseCompactionSizeMB",
			opts: []Option{WithBaseCompactionSizeMB(20)},
			verify: func(t *testing.T, cfg Config) {
				if cfg.baseCompactionSizeMB != 20 {
					t.Errorf("baseCompactionSizeMB = %v, want %v", cfg.baseCompactionSizeMB, 20)
				}
			},
		},
		{
			name: "WithLevelSizeMultiplier",
			opts: []Option{WithLevelSizeMultiplier(5)},
			verify: func(t *testing.T, cfg Config) {
				if cfg.levelSizeMultiplier != 5 {
					t.Errorf("levelSizeMultiplier = %v, want %v", cfg.levelSizeMultiplier, 5)
				}
			},
		},
		{
			name: "WithBloomFalsePositiveRate",
			opts: []Option{WithBloomFalsePositiveRate(0.05)},
			verify: func(t *testing.T, cfg Config) {
				if cfg.bloomFalsePositiveRate != 0.05 {
					t.Errorf("bloomFalsePositiveRate = %v, want %v", cfg.bloomFalsePositiveRate, 0.05)
				}
			},
		},
		{
			name: "WithSkipListDefaultLevel",
			opts: []Option{WithSkipListDefaultLevel(5)},
			verify: func(t *testing.T, cfg Config) {
				if cfg.skipListDefaultLevel != 5 {
					t.Errorf("skipListDefaultLevel = %v, want %v", cfg.skipListDefaultLevel, 5)
				}
			},
		},
		{
			name: "WithSkipListMaxLevel",
			opts: []Option{WithSkipListMaxLevel(64)},
			verify: func(t *testing.T, cfg Config) {
				if cfg.skipListMaxLevel != 64 {
					t.Errorf("skipListMaxLevel = %v, want %v", cfg.skipListMaxLevel, 64)
				}
			},
		},
		{
			name: "WithSkipListP",
			opts: []Option{WithSkipListP(0.25)},
			verify: func(t *testing.T, cfg Config) {
				if cfg.skipListP != 0.25 {
					t.Errorf("skipListP = %v, want %v", cfg.skipListP, 0.25)
				}
			},
		},
		{
			name: "WithEnableTelemetry",
			opts: []Option{WithEnableTelemetry(true), WithExporterEndpoint("localhost:4317")},
			verify: func(t *testing.T, cfg Config) {
				if !cfg.enableTelemetry {
					t.Errorf("EnableTelemetry = %v, want %v", cfg.enableTelemetry, true)
				}
				if cfg.exporterEndpoint != "localhost:4317" {
					t.Errorf("ExporterEndpoint = %v, want %v", cfg.exporterEndpoint, "localhost:4317")
				}
			},
		},
		{
			name: "WithExporterEndpoint",
			opts: []Option{WithExporterEndpoint("localhost:4317")},
			verify: func(t *testing.T, cfg Config) {
				if cfg.exporterEndpoint != "localhost:4317" {
					t.Errorf("ExporterEndpoint = %v, want %v", cfg.exporterEndpoint, "localhost:4317")
				}
			},
		},
		{
			name: "WithExporterInsecure",
			opts: []Option{WithExporterInsecure(true)},
			verify: func(t *testing.T, cfg Config) {
				if !cfg.exporterInsecure {
					t.Errorf("ExporterInsecure = %v, want %v", cfg.exporterInsecure, true)
				}
			},
		},
		{
			name: "WithCacheBytes",
			opts: []Option{WithCacheBytes(1024)},
			verify: func(t *testing.T, cfg Config) {
				if cfg.cacheBytes != 1024 {
					t.Errorf("cacheBytes = %v, want %v", cfg.cacheBytes, 1024)
				}
			},
		},
		{
			name: "WithCacheShards",
			opts: []Option{WithCacheShards(8)},
			verify: func(t *testing.T, cfg Config) {
				if cfg.cacheShards != 8 {
					t.Errorf("cacheShards = %v, want %v", cfg.cacheShards, 8)
				}
			},
		},
		{
			name: "WithCacheProbationFraction",
			opts: []Option{WithCacheProbationFraction(0.3)},
			verify: func(t *testing.T, cfg Config) {
				if cfg.cacheProbationFraction != 0.3 {
					t.Errorf("cacheProbationFraction = %v, want %v", cfg.cacheProbationFraction, 0.3)
				}
			},
		},
		{
			name: "WithCacheCorruptTTL",
			opts: []Option{WithCacheCorruptTTL(time.Minute)},
			verify: func(t *testing.T, cfg Config) {
				if cfg.cacheCorruptTTL != time.Minute {
					t.Errorf("cacheCorruptTTL = %v, want %v", cfg.cacheCorruptTTL, time.Minute)
				}
			},
		},
		{
			name: "WithCacheTombstoneTTL",
			opts: []Option{WithCacheTombstoneTTL(time.Minute)},
			verify: func(t *testing.T, cfg Config) {
				if cfg.cacheTombstoneTTL != time.Minute {
					t.Errorf("cacheTombstoneTTL = %v, want %v", cfg.cacheTombstoneTTL, time.Minute)
				}
			},
		},
		{
			name: "MultipleOptions",
			opts: []Option{
				WithDatabaseDir("/multi/path"),
				WithMaxMemtableSize(2000),
				WithBloomFalsePositiveRate(0.02),
			},
			verify: func(t *testing.T, cfg Config) {
				if cfg.databaseDir != "/multi/path" {
					t.Errorf("databaseDir = %v, want %v", cfg.databaseDir, "/multi/path")
				}
				if cfg.maxMemtableSize != 2000 {
					t.Errorf("maxMemtableSize = %v, want %v", cfg.maxMemtableSize, 2000)
				}
				if cfg.bloomFalsePositiveRate != 0.02 {
					t.Errorf("bloomFalsePositiveRate = %v, want %v", cfg.bloomFalsePositiveRate, 0.02)
				}
			},
		},
		{
			name: "WithLogger",
			opts: []Option{WithLogger(simpleLogger{})},
			verify: func(t *testing.T, cfg Config) {
				if _, ok := cfg.logger.(simpleLogger); !ok {
					t.Errorf("logger type = %T, want simpleLogger", cfg.logger)
				}
			},
		},
		{
			name: "WithLogLevel",
			opts: []Option{WithLogLevel(LogLevelError)},
			verify: func(t *testing.T, cfg Config) {
				if cfg.logLevel != LogLevelError {
					t.Errorf("logLevel = %v, want %v", cfg.logLevel, LogLevelError)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig(tt.opts...)
			tt.verify(t, cfg)
		})
	}
}

type simpleLogger struct{}

func (simpleLogger) Debug(context.Context, string, ...any) {}
func (simpleLogger) Info(context.Context, string, ...any)  {}
func (simpleLogger) Warn(context.Context, string, ...any)  {}
func (simpleLogger) Error(context.Context, string, ...any) {}

func TestConfig_ValidatePanics(t *testing.T) {
	base := DefaultConfig()
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"empty databaseDir", func(c *Config) { c.databaseDir = "" }},
		{"zero maxMemtableSize", func(c *Config) { c.maxMemtableSize = 0 }},
		{"non-positive level0CompactionThreshold", func(c *Config) { c.level0CompactionThreshold = 0 }},
		{"non-positive baseCompactionSizeMB", func(c *Config) { c.baseCompactionSizeMB = 0 }},
		{"non-positive levelSizeMultiplier", func(c *Config) { c.levelSizeMultiplier = 0 }},
		{"negative writeRateTrigger", func(c *Config) { c.writeRateTrigger = -1 }},
		{"ioLoadMax out of range", func(c *Config) { c.ioLoadMax = 2 }},
		{"bloomFalsePositiveRate out of range", func(c *Config) { c.bloomFalsePositiveRate = 1 }},
		{"skipListDefaultLevel zero", func(c *Config) { c.skipListDefaultLevel = 0 }},
		{"skipListMaxLevel zero", func(c *Config) { c.skipListMaxLevel = 0 }},
		{"skipListDefaultLevel greater than max", func(c *Config) { c.skipListDefaultLevel = 5; c.skipListMaxLevel = 4 }},
		{"skipListP out of range", func(c *Config) { c.skipListP = 1 }},
		{"telemetry enabled without endpoint", func(c *Config) { c.enableTelemetry = true; c.exporterEndpoint = "" }},
		{"telemetrySamplingRate out of range", func(c *Config) { c.telemetrySamplingRate = 1.1 }},
		{"nil newWALFunc", func(c *Config) { c.newWALFunc = nil }},
		{"nil newSSTableManagerFunc", func(c *Config) { c.newSSTableManagerFunc = nil }},
		{"nil fileNumberAllocator", func(c *Config) { c.fileNumberAllocator = nil }},
		{"nil newManifestWriterFunc", func(c *Config) { c.newManifestWriterFunc = nil }},
		{"manifestSizeThreshold non-positive", func(c *Config) { c.manifestSizeThreshold = 0 }},
		{"cacheBytes negative", func(c *Config) { c.cacheBytes = -1 }},
		{"cacheShards negative", func(c *Config) { c.cacheShards = -1 }},
		{"cacheShards zero", func(c *Config) { c.cacheShards = 0 }},
		{"cacheProbationFraction out of range", func(c *Config) { c.cacheProbationFraction = 1 }},
		{"cacheCorruptTTL negative", func(c *Config) { c.cacheCorruptTTL = -1 }},
		{"cacheCorruptTTL zero", func(c *Config) { c.cacheCorruptTTL = 0 }},
		{"cacheTombstoneTTL negative", func(c *Config) { c.cacheTombstoneTTL = -1 }},
		{"nil fdLimiter", func(c *Config) { c.fdLimiter = nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			assert.Panics(t, func() { cfg.Validate() })
		})
	}
}
