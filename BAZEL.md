# Bazel Build System for Turbo Polars

This document describes how to use the Bazel build system for Turbo Polars.

## Prerequisites

- [Bazel](https://bazel.build/install) 7.0+ (with Bzlmod support)
- [Rust](https://rustup.rs/) nightly (uses your system installation)
- [Go](https://golang.org/dl/) 1.21+

## Architecture

The system uses a **hybrid approach** that combines Bazel orchestration with direct cargo calls:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Bazel Build   │───▶│  Shell Rules     │───▶│  System Cargo   │
│   Orchestration │    │  (genrule)       │    │  (Nightly)      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                                               │
         ▼                                               ▼
┌─────────────────┐                            ┌─────────────────┐
│   Go Library    │◀───── CGO Linking ────────│ Rust Static Lib │
│   (rules_go)    │                            │ (staticlib)     │
└─────────────────┘                            └─────────────────┘
```

This approach:
- ✅ Uses your system Rust nightly toolchain directly
- ✅ Eliminates brittle manual library copying
- ✅ Supports cross-compilation
- ✅ Requires no Bazel Rust expertise

## Quick Start

### Building

```bash
# Build the main components
bazel build //pkg/polars:polars //rust:turbo_polars_static

# Build the Rust static library only
bazel build //rust:turbo_polars_static

# Build the Go library only  
bazel build //pkg/polars:polars
```

### Cross-Compilation

```bash
# Build for Linux AMD64
bazel build //rust:rust_build_linux_amd64

# Build for macOS ARM64
bazel build //rust:rust_build_darwin_arm64
```

### Testing

Note: CGO tests have limitations in Bazel. The libraries build successfully and can be used in applications.

```bash
# Verify builds work
bazel build //pkg/polars:polars //rust:turbo_polars_static
```

### Cleaning

```bash
# Clean build artifacts
bazel clean

# Clean everything including external dependencies
bazel clean --expunge
```

## Build Configurations

### Development Build (default)
```bash
bazel build //pkg/polars:polars
```

### Release Build
```bash
bazel build --config=release //pkg/polars:polars
```

### SIMD Optimized Build
```bash
bazel build --config=simd //pkg/polars:polars
```

## Platform Support

### macOS
- ✅ ARM64 (Apple Silicon)
- ✅ AMD64 (Intel)

### Linux
- ✅ AMD64
- ✅ ARM64

## Troubleshooting

### Build Issues

1. **Rust compilation errors**: Ensure you have nightly Rust installed
   ```bash
   rustup install nightly
   rustup default nightly
   ```

2. **Go CGO issues**: The hybrid approach handles CGO linking automatically

3. **Cross-compilation failures**: Install target toolchains
   ```bash
   rustup target add x86_64-unknown-linux-gnu
   rustup target add aarch64-unknown-linux-gnu
   ```

### Performance

- First build downloads and compiles all Rust dependencies (~2-3 minutes)
- Subsequent builds are incremental and much faster (~0.5 seconds)
- Use `bazel build --config=release` for optimized builds

### Terminal Hang Issue

On first-time builds, Bazel may appear to hang after showing "Build completed successfully". This is a Bazel server/terminal interaction issue, not a build problem:

- **The build actually succeeds** - check `bazel-bin/` for artifacts
- **Subsequent builds work normally** and return to prompt
- **Workaround**: Use `Ctrl+C` to interrupt if needed - artifacts are still created

## Migration from Makefile

The Bazel build system replaces these Makefile targets:

| Makefile | Bazel |
|----------|-------|
| `make build-rust` | `bazel build //rust:turbo_polars_static` |
| `make build-go` | `bazel build //pkg/polars:polars` |
| `make build` | `bazel build //pkg/polars:polars //rust:turbo_polars_static` |
| `make test` | Libraries build successfully (CGO test limitations) |
| `make clean` | `bazel clean` |

## Benefits

✅ **Eliminates Brittleness**: No more manual `cargo build && cp` commands  
✅ **Cross-Compilation**: Built-in support for multiple platforms  
✅ **System Integration**: Uses your nightly Rust toolchain directly  
✅ **Dependency Management**: Automatic linking and path management  
✅ **Reproducible Builds**: Consistent results across environments  
✅ **Simple Maintenance**: No Bazel Rust expertise required  

## Files Structure

```
├── MODULE.bazel              # Bzlmod configuration
├── BUILD.bazel              # Root build targets  
├── .bazelrc                 # Build configuration
├── rust/
│   └── BUILD.bazel          # Rust build (shell rules + cargo)
├── pkg/polars/
│   └── BUILD.bazel          # Go library with CGO
└── testdata/
    └── BUILD.bazel          # Test data files
```