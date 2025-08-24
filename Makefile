check:
	@$(MAKE) check-spanname
	@go fmt ./...
	@echo "Running staticcheck..."
	@go run honnef.co/go/tools/cmd/staticcheck@v0.5.0 ./...
	@echo "Done."

check-spanname:
	@go build -o ./bin/spanname ./tool/spanname
	@go vet -vettool=$$(pwd)/bin/spanname ./...

.PHONY: clean-testdata
clean-testdata:
	@rm -rf testdata/*

test: clean-testdata
	@echo "Running tests..."
	@go test -v ./...

test-integration-smoke: clean-testdata
	@echo "Running smoke integration tests..."
	@go test -v -tags "integration smoke" ./integration

test-integration-full: clean-testdata
	@echo "Running full integration tests..."
	@go test -v -tags integration ./integration

test-integration: test-integration-full

test-coverage: clean-testdata
	@echo "Running tests with coverage..."
	@go test -v -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out
	@rm coverage.out

test-pprof:
	@./scripts/run-pprof-tests.sh
