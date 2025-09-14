package diffharness

import (
	"encoding/json"
	"os"
)

// PhaseLogger records operation phase transitions.
type PhaseLogger interface {
	Log(op Op, seq uint64, phase Phase, i int) error
}

// PhaseLoggerFunc adapts a function to PhaseLogger.
type PhaseLoggerFunc func(op Op, seq uint64, phase Phase, i int) error

// Log implements PhaseLogger.
func (f PhaseLoggerFunc) Log(op Op, seq uint64, phase Phase, i int) error {
	return f(op, seq, phase, i)
}

// MultiLogger fans out logs to multiple loggers.
type MultiLogger []PhaseLogger

// Log implements PhaseLogger.
func (ml MultiLogger) Log(op Op, seq uint64, phase Phase, i int) error {
	for _, l := range ml {
		if l == nil {
			continue
		}
		if err := l.Log(op, seq, phase, i); err != nil {
			return err
		}
	}
	return nil
}

// HookSet bundles optional crash and telemetry hooks.
type HookSet struct {
	Crash     func(ops int) error
	Telemetry func(seq uint64, ops int)
}

// WithCrash sets the crash hook.
func (h *Harness) WithCrash(fn func(ops int) error) { h.hooks.Crash = fn }

// WithTelemetry sets the telemetry hook.
func (h *Harness) WithTelemetry(fn func(seq uint64, ops int)) { h.hooks.Telemetry = fn }

// WithHooks replaces the entire hook set.
func (h *Harness) WithHooks(hs HookSet) { h.hooks = hs }

// nopLogger discards all log entries.
var nopLogger PhaseLogger = PhaseLoggerFunc(func(Op, uint64, Phase, int) error { return nil })

// jsonLogger writes log entries to a JSONL file.
type jsonLogger struct {
	f   *os.File
	enc *json.Encoder
}

// newJSONLogger opens path for appending and returns a PhaseLogger.
func newJSONLogger(path string) (*jsonLogger, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &jsonLogger{f: f, enc: json.NewEncoder(f)}, nil
}

// Log implements PhaseLogger.
func (l *jsonLogger) Log(op Op, seq uint64, phase Phase, i int) error {
	entry := struct {
		I     int    `json:"i"`
		Seq   uint64 `json:"seq"`
		Op    Op     `json:"op"`
		Phase Phase  `json:"phase"`
	}{i, seq, op, phase}
	if err := l.enc.Encode(entry); err != nil {
		return err
	}
	return l.f.Sync()
}

// Close closes the underlying file.
func (l *jsonLogger) Close() error { return l.f.Close() }
