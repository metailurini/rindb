# Diff Harness

The diff harness runs RinDB operations against a SQLite oracle to surface logic discrepancies. It can export traces and metrics via OpenTelemetry.

## Local Telemetry Stack

Build the harness binary for Linux:

```bash
GOOS=linux GOARCH=amd64 go build -o diffharness/bin/diffharness ./diffharness/cmd
```

Start Jaeger, the OpenTelemetry Collector, and Prometheus:

```bash
docker compose -f diffharness/docker-compose.yaml up -d jaeger otel-collector prometheus
```

The Jaeger UI is available at http://localhost:16686. Prometheus is available at http://localhost:9090.

Run the harness and send telemetry to the collector (which forwards traces to Jaeger and exposes metrics for Prometheus):

```bash
docker compose -f diffharness/docker-compose.yaml run --rm harness -n 1000
```

Additional command‑line flags may be appended after `harness` to control the run.

