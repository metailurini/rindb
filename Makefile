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

test-integration:
	@rm -rf testdata/*
	@echo "Running integration tests..."
	@go test -v -tags integration ./integration

test-coverage:
	@rm -rf testdata/*
	@echo "Running tests with coverage..."
	@go test -v -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out
	@rm coverage.out


test-pprof:
	@./scripts/run-pprof-tests.sh
