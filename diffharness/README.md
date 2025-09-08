# Diff Harness

The diff harness runs RinDB operations against a SQLite oracle to surface logic discrepancies. It now supports exporting traces to a local Jaeger instance.

## Local Jaeger Setup

Start Jaeger using the provided compose file:

```bash
docker compose -f diffharness/docker-compose.yaml up -d
```

The Jaeger UI is available at http://localhost:16686 and accepts OTLP gRPC on `localhost:4317`.

Run the harness and send traces to Jaeger:

```bash
go run ./diffharness/cmd -jaeger=localhost:4317
```

To persist database files and logs, specify a working directory. By default a temporary directory is used and deleted when the run completes.

```bash
go run ./diffharness/cmd -dir=/tmp/dh-run -log=run.jsonl
```

