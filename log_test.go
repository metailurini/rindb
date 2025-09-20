package rindb

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type recordingLogger struct{ logs []string }

func (r *recordingLogger) Debug(ctx context.Context, msg string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf("debug:"+msg, args...))
}
func (r *recordingLogger) Info(ctx context.Context, msg string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf("info:"+msg, args...))
}
func (r *recordingLogger) Warn(ctx context.Context, msg string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf("warn:"+msg, args...))
}
func (r *recordingLogger) Error(ctx context.Context, msg string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf("error:"+msg, args...))
}

func TestLogLevelFiltering(t *testing.T) {
	t.Parallel()
	rl := &recordingLogger{}
	log := newScopedLogger(rl, LogLevelInfo)

	log.debug(context.Background(), "d")
	log.info(context.Background(), "i")
	log.warn(context.Background(), "w")

	if len(rl.logs) != 2 || rl.logs[0] != "info:i" || rl.logs[1] != "warn:w" {
		t.Fatalf("unexpected logs: %v", rl.logs)
	}

	rl.logs = nil
	log = newScopedLogger(rl, LogLevelDebug)
	log.debug(context.Background(), "d2")
	if len(rl.logs) != 1 || rl.logs[0] != "debug:d2" {
		t.Fatalf("expected debug log, got %v", rl.logs)
	}
}

func TestStdLogger_Writes(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := NewStdLogger(log.New(&buf, "", 0))

	ctx := context.Background()
	l.Debug(ctx, "d %d", 1)
	l.Info(ctx, "i")
	l.Warn(ctx, "w")
	l.Error(ctx, "e")

	got := buf.String()
	want := "[debug] d 1\n[info] i\n[warn] w\n[error] e\n"
	if got != want {
		t.Fatalf("unexpected output: %q", got)
	}
}

func TestStdLogger_TraceContext(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := NewStdLogger(log.New(&buf, "", 0))

	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "TestStdLogger_TraceContext")
	l.Info(ctx, "hello")
	span.End()

	out := buf.String()
	if !strings.Contains(out, "[info]") || !strings.Contains(out, "trace_id=") || !strings.Contains(out, "span_id=") || !strings.Contains(out, "hello") {
		t.Fatalf("unexpected output: %q", out)
	}
}

func TestNewStdLogger_NilLogger(t *testing.T) {
	t.Parallel()
	l := NewStdLogger(nil)
	// Should not panic when logging with a nil underlying logger.
	l.Info(context.Background(), "silent")
}
