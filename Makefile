GO = go

check:
	@$(MAKE) check-spanname
	@$(GO) fmt ./...
	@echo "Running staticcheck..."
	@$(GO) run honnef.co/go/tools/cmd/staticcheck@v0.5.0 ./...
	@echo "Done."

check-spanname:
	@$(GO) build -o ./bin/spanname ./tool/spanname
	@$(GO) vet -vettool=$$(pwd)/bin/spanname ./...

.PHONY: clean-testdata
clean-testdata:
	@rm -rf testdata/*

test: clean-testdata
	@echo "Running tests..."
	@$(GO) test -v ./...

test-integration-smoke: clean-testdata
	@echo "Running smoke integration tests..."
	@$(GO) test -v -tags "integration smoke" ./integration

test-integration-full: clean-testdata
	@$(GO) clean -testcache
	@echo "Running full integration tests..."
	@$(GO) test -v -tags integration ./integration

test-integration: test-integration-full

test-coverage: clean-testdata
	@echo "Running tests with coverage..."
	@$(GO) test -v -coverprofile=coverage.txt ./...

test-pprof:
	@./scripts/run-pprof-tests.sh
