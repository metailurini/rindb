export GOTOOLCHAIN ?= go1.25.0
GO = go
UNAME_S := $(shell uname -s)

check:
	@$(MAKE) check-spanname
	@$(GO) fmt ./...
	@echo "Running staticcheck..."
	@$(GO) run honnef.co/go/tools/cmd/staticcheck@v0.6.1 ./...
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

view-coverage:
	@$(GO) tool cover -html=coverage.txt -o coverage.html
ifeq ($(OS),Windows_NT)
	@start coverage.html
else ifeq ($(UNAME_S),Linux)
	@xdg-open coverage.html
else ifeq ($(UNAME_S),Darwin)
	@open coverage.html
endif
