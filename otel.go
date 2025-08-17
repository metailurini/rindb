package rindb

import (
	"context"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// OtelInit initializes OpenTelemetry providers when enabled.
// The returned shutdown function should be called to flush data.
// If insecure is true, transport security is disabled for the OTLP exporters.
func OtelInit(ctx context.Context, enable bool, endpoint string, insecure bool, samplingRate float64) (func(context.Context) error, error) {
	if !enable {
		log.Printf("OpenTelemetry is disabled.")
		return func(context.Context) error { return nil }, nil
	}
	log.Printf("OpenTelemetry initialized with endpoint: %s, insecure: %t, samplingRate: %f", endpoint, insecure, samplingRate)

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceNameKey.String("rindb")),
	)
	if err != nil {
		return nil, err
	}

	traceOpts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(endpoint)}
	if insecure {
		traceOpts = append(traceOpts, otlptracegrpc.WithInsecure())
	}
	traceExp, err := otlptracegrpc.New(ctx, traceOpts...)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(samplingRate)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	metricOpts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(endpoint)}
	if insecure {
		metricOpts = append(metricOpts, otlpmetricgrpc.WithInsecure())
	}
	metricExp, err := otlpmetricgrpc.New(ctx, metricOpts...)
	if err != nil {
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	shutdown := func(ctx context.Context) error {
		log.Printf("Shutting down OpenTelemetry trace provider...")
		if err := tp.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down trace provider: %v", err)
			return err
		}
		log.Printf("Shutting down OpenTelemetry meter provider...")
		if err := mp.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down meter provider: %v", err)
			return err
		}
		log.Printf("OpenTelemetry shutdown complete.")
		return nil
	}
	return shutdown, nil
}
