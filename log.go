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

func LOG(ctx context.Context, logType string, msg string, args ...any) {
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		sc := span.SpanContext()
		log.Printf("[%s] trace_id=%s span_id=%s "+msg+"\n", append([]any{logType, sc.TraceID().String(), sc.SpanID().String()}, args...)...)
		return
	}
	log.Printf("[%s] "+msg+"\n", append([]any{logType}, args...)...)
}

func DEBUG(ctx context.Context, msg string, args ...any) {
	if isRunningTests() {
		LOG(ctx, "DEBUG", msg, args...)
	}
}
func INFO(ctx context.Context, msg string, args ...any)  { LOG(ctx, "INFO", msg, args...) }
func WARN(ctx context.Context, msg string, args ...any)  { LOG(ctx, "WARN", msg, args...) }
func ERROR(ctx context.Context, msg string, args ...any) { LOG(ctx, "ERROR", msg, args...) }
