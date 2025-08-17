# Repository Guidelines

## Project Structure & Modules
- `*.go`: Core library (LSM/SSTable, WAL, memtable, iterators) with tests in `*_test.go` next to sources.
- `cmd/main.go`: Simple interactive CLI for local runs.
- `tool/spanname`: Custom `go vet` analyzer enforcing span naming (see below).
- `assets/`: Media; `notes/`: design notes; `testdata/`: ephemeral fixtures cleaned by tests.

## Build, Test, and Development
- `make check`: Formats (`go fmt`), builds `tool/spanname`, runs `go vet` with it, then `staticcheck`.
- `make test`: Cleans `testdata/` and runs all tests verbosely.
- `make test-coverage`: Runs tests with coverage and opens the HTML report.
- `go build -o rindb cmd/main.go`: Builds the sample CLI. Example: `./rindb` then `put key val`.

## Coding Style & Naming
- Language: Go 1.21+. Use `go fmt` and keep imports tidy; `make check` must pass.
- Names: Exported `CamelCase`, unexported `lowerCamelCase`; package names are short, lowercase, no stutter.
- Context: Accept `ctx context.Context` as the first param for ops; avoid storing contexts.
- Tracing: Span names must match the enclosing function or method per `tool/spanname`.
  Example: `tracer.Start(ctx, "Put")` or `tracer.Start(ctx, "DB.Put")`. Opt-out with `// spanname:ignore` on the func.

## Testing Guidelines
- Frameworks: Standard `testing` with `testify` for assertions.
- Style: Prefer table‑driven tests; name tests `TestXxx` in `*_test.go`.
- Data: Write temp files under `testdata/`. Tests clean this directory; do not commit generated artifacts.
- Run: `make test` locally; use `t.Helper()` for helpers and avoid global state between tests.

## Commit & Pull Requests
- Commits: Follow Conventional Commits (`feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`). Keep messages imperative and scoped.
- PRs: Describe motivation and approach, link related issues, include tests, and note any config/CLI changes (with examples). Ensure `make check` and `make test` pass.

## Configuration & Telemetry
- Database directory defaults to `rindat/`. Configure via options (e.g., `WithDatabaseDir("./mydb")`).
- OpenTelemetry: Toggle with `WithEnableTelemetry(true)` and set endpoint via `WithExporterEndpoint("host:4317")`; use `WithExporterInsecure(true)` for non‑TLS.
