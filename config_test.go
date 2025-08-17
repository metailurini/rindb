package rindb

import (
	"testing"
)

// TestDefaultConfig verifies that DefaultConfig returns the expected default values.
func TestDefaultConfig(t *testing.T) {
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
		{"bloomFalsePositiveRate", cfg.bloomFalsePositiveRate, 0.01},
		{"skipListDefaultLevel", cfg.skipListDefaultLevel, uint(2)},
		{"skipListMaxLevel", cfg.skipListMaxLevel, uint(32)},
		{"skipListP", cfg.skipListP, 0.5},
		{"EnableTelemetry", cfg.enableTelemetry, false},
		{"ExporterEndpoint", cfg.exporterEndpoint, ""},
		{"ExporterInsecure", cfg.exporterInsecure, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("DefaultConfig() %s = %v, want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}

// TestNewConfigWithOptions tests that NewConfig applies options correctly.
func TestNewConfigWithOptions(t *testing.T) {
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
			opts: []Option{WithEnableTelemetry(true)},
			verify: func(t *testing.T, cfg Config) {
				if !cfg.enableTelemetry {
					t.Errorf("EnableTelemetry = %v, want %v", cfg.enableTelemetry, true)
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig(tt.opts...)
			tt.verify(t, cfg)
		})
	}
}
