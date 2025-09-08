export GOTOOLCHAIN ?= go1.25.0
GO = go
UNAME_S := $(shell uname -s)
PKGS := $(shell $(GO) list ./... | grep -v -e '/cmd$$' -e '/cmd/' -e '/diffharness')

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

# === Unit tests ===
test: clean-testdata
	@echo "Running tests..."
	@$(GO) test -v ./...

test-coverage: clean-testdata
	@echo "Running tests with coverage..."
	@$(GO) test -v -coverprofile=coverage.txt $(PKGS)

# === Integration tests ===
test-integration-smoke: clean-testdata
	@echo "Running smoke integration tests..."
	@$(GO) test -v -tags "integration smoke" ./integration

test-integration-full: clean-testdata
	@$(GO) clean -testcache
	@echo "Running full integration tests..."
	@$(GO) test -v -tags integration ./integration

test-integration:
	@$(MAKE) test-integration-full

# === Diff harness tests ===
diffharness-build:
	@echo "Building diffharness for linux/amd64..."
	@cd diffharness && go mod tidy && GOOS=linux GOARCH=amd64 $(GO) build -o ./bin/diffharness ./cmd/main.go
	@echo "Done."

diffharness-up: diffharness-build
	@echo "Running docker compose up for diffharness..."
	@docker compose -f diffharness/docker-compose.yaml up -d
	@echo "Done."

diffharness-down:
	@echo "Running docker compose down for diffharness..."
	@docker compose -f diffharness/docker-compose.yaml down
	@echo "Done."


# === Pprof ===
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
