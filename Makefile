test-coverage:
	@rm -rf testdata/*
	@echo "Running tests with coverage..."
	@go test -v -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out
	@rm coverage.out
