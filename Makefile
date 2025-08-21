# Makefile to mirror the CI pipeline locally
# Builds both Go and Rust components using the same commands as release.yml

.PHONY: all build build-go build-rust ci clean install-dist help

# Default target
all: build

# Build everything (Go + Rust)
build: build-go build-rust

# CI build target that mirrors the CI pipeline
ci: build-go-ci build-rust

# Build Go binary based on CARGO_DIST_TARGET environment variable
build-go-ci:
	@echo "Building Go binary for CI target: $(CARGO_DIST_TARGET)"
	@if [ -z "$(CARGO_DIST_TARGET)" ]; then \
		echo "Error: CARGO_DIST_TARGET not set"; \
		exit 1; \
	fi
	@case "$(CARGO_DIST_TARGET)" in \
		x86_64-unknown-linux-gnu) $(MAKE) build-go-linux-amd64 && $(MAKE) tar-go ;; \
		aarch64-unknown-linux-gnu) $(MAKE) build-go-linux-arm64 && $(MAKE) tar-go ;; \
		x86_64-apple-darwin) $(MAKE) build-go-darwin-amd64 && $(MAKE) tar-go ;; \
		aarch64-apple-darwin) $(MAKE) build-go-darwin-arm64 && $(MAKE) tar-go ;; \
		*) echo "Skipping Go build for unsupported target: $(CARGO_DIST_TARGET)" >&2; exit 0 ;; \
	esac
	@# Move tar to target directory and cleanup
	@mkdir -p target/ldrs_sf
	@if [ -f "go/ldrs-sf/ldrs-sf-$(CARGO_DIST_TARGET).tar.gz" ]; then \
		mv go/ldrs-sf/ldrs-sf-$(CARGO_DIST_TARGET).tar.gz target/ldrs_sf/ && \
		echo "Moved tar to target/ldrs_sf/"; \
	fi
	@rm -f go/ldrs-sf/ldrs-sf
	@echo "Cleaned up binary and tar from go/ldrs-sf/"

# Build the Go binary (ldrs-sf)
build-go:
	@echo "Building Go binary..."
	cd go/ldrs-sf && \
	go mod tidy && \
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ldrs-sf .

# Build the Go binary for specific platform (like CI does)
build-go-linux-amd64:
	@echo "Building Go binary for linux/amd64..."
	cd go/ldrs-sf && \
	go mod tidy && \
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
	go build -trimpath -ldflags="-s -w" -o ldrs-sf .

build-go-linux-arm64:
	@echo "Building Go binary for linux/arm64..."
	cd go/ldrs-sf && \
	go mod tidy && \
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
	go build -trimpath -ldflags="-s -w" -o ldrs-sf .

build-go-darwin-amd64:
	@echo "Building Go binary for darwin/amd64..."
	cd go/ldrs-sf && \
	go mod tidy && \
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 \
	go build -trimpath -ldflags="-s -w" -o ldrs-sf .

build-go-darwin-arm64:
	@echo "Building Go binary for darwin/arm64..."
	cd go/ldrs-sf && \
	go mod tidy && \
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 \
	go build -trimpath -ldflags="-s -w" -o ldrs-sf .

# Create tar archive of Go binary with target triple naming
tar-go:
	@echo "Creating tar archive for $(CARGO_DIST_TARGET)..."
	cd go/ldrs-sf && \
	tar -czf ldrs-sf-$(CARGO_DIST_TARGET).tar.gz ldrs-sf

# Install cargo-dist if not present
install-dist:
	@if ! command -v dist >/dev/null 2>&1; then \
		echo "Installing cargo-dist..."; \
		curl --proto '=https' --tlsv1.2 -LsSf https://github.com/axodotdev/cargo-dist/releases/download/v0.29.0/cargo-dist-installer.sh | sh; \
	else \
		echo "cargo-dist already installed"; \
	fi

# Build Rust components using cargo-dist (like CI does)
build-rust: install-dist
	@echo "Building Rust components with cargo-dist..."
	dist build --print=linkage

# Plan what dist would build (useful for debugging)
plan: install-dist
	@echo "Planning dist build..."
	dist plan --output-format=json

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	rm -f go/ldrs-sf/ldrs-sf
	rm -f go/ldrs-sf/ldrs-sf-*.tar.gz
	rm -rf target/distrib/
	rm -rf target/ldrs_sf/

# Test the builds
test-go:
	@echo "Testing Go components..."
	cd go/ldrs-sf && go test ./...

test-rust:
	@echo "Testing Rust components..."
	cargo test

test: test-go test-rust

# Help target
help:
	@echo "Available targets:"
	@echo "  all              - Build everything (default)"
	@echo "  build            - Build both Go and Rust components"
	@echo "  build-go         - Build Go binary for current platform"
	@echo "  build-go-*       - Build Go binary for specific platform"
	@echo "  build-rust       - Build Rust components with cargo-dist"
	@echo "  ci               - Build for CI (uses CARGO_DIST_TARGET)"
	@echo "  install-dist     - Install cargo-dist tool"
	@echo "  plan             - Show what dist would build"
	@echo "  test             - Run tests for both Go and Rust"
	@echo "  test-go          - Run Go tests"
	@echo "  test-rust        - Run Rust tests"
	@echo "  clean            - Clean build artifacts"
	@echo "  help             - Show this help message"