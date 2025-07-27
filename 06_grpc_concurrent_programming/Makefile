# Makefile for gRPC Concurrent Programming Project

# Variables
PROTO_DIR = proto
SERVER_DIR = server
CLIENT_DIR = client
EXAMPLES_DIR = examples
LOADTEST_DIR = loadtest

# Go modules
MOD_NAME = grpc-concurrent-programming

# Protobuf settings
PROTOC = protoc
PROTO_FILES = $(wildcard $(PROTO_DIR)/*.proto)

.PHONY: all clean proto build run-examples run-loadtest test help

# Default target
all: proto build

# Help
help:
	@echo "Available targets:"
	@echo "  all          - Generate protobuf files and build all components"
	@echo "  proto        - Generate Go files from .proto files"
	@echo "  build        - Build all Go modules"
	@echo "  run-examples - Run example application"
	@echo "  run-loadtest - Run load test suite"
	@echo "  test         - Run unit tests"
	@echo "  clean        - Clean generated files"
	@echo "  deps         - Install required dependencies"
	@echo "  help         - Show this help message"

# Install dependencies
deps:
	@echo "Installing dependencies..."
	go mod tidy
	@echo "Installing protoc plugins..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Generate protobuf files
proto:
	@echo "Generating protobuf files..."
	@mkdir -p $(PROTO_DIR)
	$(PROTOC) --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)
	@echo "Protobuf files generated successfully"

# Build all components
build: proto
	@echo "Building server..."
	go build -o bin/server ./$(SERVER_DIR)/...
	@echo "Building examples..."
	go build -o bin/examples ./$(EXAMPLES_DIR)/...
	@echo "Building load test..."
	go build -o bin/loadtest ./$(LOADTEST_DIR)/...
	@echo "Build completed successfully"

# Run examples
run-examples: build
	@echo "Running example application..."
	./bin/examples

# Run load test
run-loadtest: build
	@echo "Running load test suite..."
	./bin/loadtest

# Run unit tests
test:
	@echo "Running unit tests..."
	go test -v ./...
	@echo "Unit tests completed"

# Run benchmark tests
benchmark:
	@echo "Running benchmark tests..."
	go test -bench=. -benchmem ./...
	@echo "Benchmark tests completed"

# Clean generated files
clean:
	@echo "Cleaning generated files..."
	rm -rf bin/
	find . -name "*.pb.go" -delete
	find . -name "*_grpc.pb.go" -delete
	@echo "Clean completed"

# Format code
fmt:
	@echo "Formatting Go code..."
	go fmt ./...
	@echo "Code formatting completed"

# Lint code
lint:
	@echo "Running golint..."
	golint ./...
	@echo "Linting completed"

# Vet code
vet:
	@echo "Running go vet..."
	go vet ./...
	@echo "Vetting completed"

# Check all (format, vet, test)
check: fmt vet test

# Create bin directory
bin:
	mkdir -p bin

# Development server (auto-restart on changes)
dev:
	@echo "Starting development server with auto-restart..."
	@which air > /dev/null || (echo "Installing air..." && go install github.com/cosmtrek/air@latest)
	air -c .air.toml

# Docker build
docker-build:
	@echo "Building Docker image..."
	docker build -t grpc-concurrent-programming .
	@echo "Docker image built successfully"

# Docker run
docker-run: docker-build
	@echo "Running Docker container..."
	docker run -p 8080:8080 grpc-concurrent-programming

# Create development configuration
	@echo "Development configuration created"

# Initialize project
init: deps proto build
	@echo "Project initialization completed successfully!"
	@echo ""
	@echo "Quick start:"
	@echo "  make run-examples  - Run basic examples"
	@echo "  make run-loadtest  - Run performance tests"
	@echo "  make dev          - Start development mode"

# Performance profiling
profile:
	@echo "Running CPU profile..."
	go test -cpuprofile=cpu.prof -bench=. ./...
	@echo "Running memory profile..."
	go test -memprofile=mem.prof -bench=. ./...
	@echo "Profiling completed. Use 'go tool pprof' to analyze results"
