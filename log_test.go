package rindb

import (
	"context"
	"fmt"
	"testing"
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
