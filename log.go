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
		log.Printf("["+logType+"] trace_id="+sc.TraceID().String()+" span_id="+sc.SpanID().String()+" "+msg+"\n", args...)
		return
	}
	log.Printf("["+logType+"] "+msg+"\n", args...)
}

func DEBUG(ctx context.Context, msg string, args ...any) {
	if isRunningTests() {
		LOG(ctx, "DEBUG", msg, args...)
	}
}
func INFO(ctx context.Context, msg string, args ...any)  { LOG(ctx, "INFO", msg, args...) }
func WARN(ctx context.Context, msg string, args ...any)  { LOG(ctx, "WARN", msg, args...) }
func ERROR(ctx context.Context, msg string, args ...any) { LOG(ctx, "ERROR", msg, args...) }
