check:
	@$(MAKE) check-spanname
	@go fmt ./...
	@echo "Running staticcheck..."
	@go run honnef.co/go/tools/cmd/staticcheck@v0.5.0 ./...
	@echo "Done."

check-spanname:
	@go build -o ./bin/spanname ./tool/spanname
	@go vet -vettool=$$(pwd)/bin/spanname ./...

test:
	@rm -rf testdata/*
	@echo "Running tests..."
	@go test -v ./...

test-integration-smoke:
	@rm -rf testdata/*
	@echo "Running smoke integration tests..."
	@go test -v -tags "integration smoke" ./integration

test-integration-full:
	@rm -rf testdata/*
	@echo "Running full integration tests..."
	@go test -v -tags integration ./integration

test-integration: test-integration-full

test-coverage:
	@rm -rf testdata/*
	@echo "Running tests with coverage..."
	@go test -v -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out
	@rm coverage.out

test-pprof:
	@./scripts/run-pprof-tests.sh
