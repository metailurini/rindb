package rindb

import (
	"context"
	"io"
	"log"

	"go.opentelemetry.io/otel/trace"
)

// Logger defines logging methods used by RinDB. Implementations may
// incorporate structured logging and should be safe for concurrent use.
// The context may carry tracing information which is appended to messages.
type Logger interface {
	Debug(ctx context.Context, msg string, args ...any)
	Info(ctx context.Context, msg string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, args ...any)
}

// LogLevel represents the minimum severity threshold for emitted logs.
// Messages with a lower severity are discarded.
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// scopedLogger couples a Logger with a minimum severity threshold.
//
// It is used to provide per-instance logging without relying on package-level
// globals. Each database component can hold its own scopedLogger and emit
// messages according to its configured level.
type scopedLogger struct {
	Logger
	level LogLevel
}

func newScopedLogger(l Logger, level LogLevel) scopedLogger {
	if l == nil {
		l = nopLogger{}
	}
	return scopedLogger{Logger: l, level: level}
}

func (l scopedLogger) debug(ctx context.Context, msg string, args ...any) {
	if LogLevelDebug >= l.level {
		l.Logger.Debug(ctx, msg, args...)
	}
}

func (l scopedLogger) info(ctx context.Context, msg string, args ...any) {
	if LogLevelInfo >= l.level {
		l.Logger.Info(ctx, msg, args...)
	}
}

func (l scopedLogger) warn(ctx context.Context, msg string, args ...any) {
	if LogLevelWarn >= l.level {
		l.Logger.Warn(ctx, msg, args...)
	}
}

func (l scopedLogger) errorf(ctx context.Context, msg string, args ...any) {
	if LogLevelError >= l.level {
		l.Logger.Error(ctx, msg, args...)
	}
}

type stdLogger struct{ l *log.Logger }

// NewStdLogger wraps a standard library logger to satisfy Logger. A nil
// *log.Logger results in a logger that discards all output.
func NewStdLogger(l *log.Logger) Logger {
	if l == nil {
		l = log.New(io.Discard, "", 0)
	}
	return stdLogger{l: l}
}

func (s stdLogger) logf(ctx context.Context, level string, msg string, args ...any) {
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		sc := span.SpanContext()
		s.l.Printf("[%s] trace_id=%s span_id=%s "+msg+"\n", append([]any{level, sc.TraceID().String(), sc.SpanID().String()}, args...)...)
		return
	}
	s.l.Printf("[%s] "+msg+"\n", append([]any{level}, args...)...)
}

func (s stdLogger) Debug(ctx context.Context, msg string, args ...any) {
	s.logf(ctx, "debug", msg, args...)
}
func (s stdLogger) Info(ctx context.Context, msg string, args ...any) {
	s.logf(ctx, "info", msg, args...)
}
func (s stdLogger) Warn(ctx context.Context, msg string, args ...any) {
	s.logf(ctx, "warn", msg, args...)
}
func (s stdLogger) Error(ctx context.Context, msg string, args ...any) {
	s.logf(ctx, "error", msg, args...)
}

type nopLogger struct{}

func (nopLogger) Debug(context.Context, string, ...any) {}
func (nopLogger) Info(context.Context, string, ...any)  {}
func (nopLogger) Warn(context.Context, string, ...any)  {}
func (nopLogger) Error(context.Context, string, ...any) {}
