# Turbo Polars Build System

.PHONY: all build build-rust build-go test clean install-deps

# Default target
all: build

# Build everything
build: build-rust build-go

# Build Rust library and copy to lib/
build-rust:
	@echo "Building Rust library..."
	cd rust && cargo build --release
	@mkdir -p lib
	cp rust/target/release/libturbo_polars.a lib/
	cp rust/target/release/libturbo_polars.dylib lib/ 2>/dev/null || true
	@echo "Rust library copied to lib/"

# Build Rust library in debug mode
build-rust-debug:
	@echo "Building Rust library (debug)..."
	cd rust && cargo build
	@mkdir -p lib
	cp rust/target/debug/libturbo_polars.a lib/
	cp rust/target/debug/libturbo_polars.dylib lib/ 2>/dev/null || true
	@echo "Rust library (debug) copied to lib/"

# Build Go packages
build-go:
	@echo "Building Go packages..."
	go build ./...

# Run tests
test: build
	@echo "Running Go tests..."
	go test ./...
	@echo "Running benchmarks..."
	cd benchmarks && go test -bench=. -benchmem

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	cd rust && cargo clean
	rm -rf lib/
	go clean ./...

# Install development dependencies
install-deps:
	@echo "Installing dependencies..."
	@which cargo > /dev/null || (echo "Please install Rust: https://rustup.rs/" && exit 1)
	@which go > /dev/null || (echo "Please install Go: https://golang.org/dl/" && exit 1)
	cd rust && cargo fetch
	go mod download

# Help
help:
	@echo "Available targets:"
	@echo "  all          - Build everything (default)"
	@echo "  build        - Build Rust library and Go packages"
	@echo "  build-rust   - Build Rust library (release) and copy to lib/"
	@echo "  build-rust-debug - Build Rust library (debug) and copy to lib/"
	@echo "  build-go     - Build Go packages"
	@echo "  test         - Run tests and benchmarks"
	@echo "  clean        - Clean all build artifacts"
	@echo "  install-deps - Install/fetch dependencies"
	@echo "  help         - Show this help"