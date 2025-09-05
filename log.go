package rindb

import (
	"context"
	"flag"
	"log"

	"go.opentelemetry.io/otel/trace"
)

// isRunningTests checks if the code is being run under 'go test'.
func isRunningTests() bool {
	return flag.Lookup("test.v") != nil
}

func logMsg(ctx context.Context, logType string, msg string, args ...any) {
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		sc := span.SpanContext()
		log.Printf("[%s] trace_id=%s span_id=%s "+msg+"\n", append([]any{logType, sc.TraceID().String(), sc.SpanID().String()}, args...)...)
		return
	}
	log.Printf("[%s] "+msg+"\n", append([]any{logType}, args...)...)
}

func debug(ctx context.Context, msg string, args ...any) {
	if isRunningTests() {
		logMsg(ctx, "debug", msg, args...)
	}
}

func info(ctx context.Context, msg string, args ...any)   { logMsg(ctx, "info", msg, args...) }
func warn(ctx context.Context, msg string, args ...any)   { logMsg(ctx, "warn", msg, args...) }
func errorf(ctx context.Context, msg string, args ...any) { logMsg(ctx, "errorf", msg, args...) }
