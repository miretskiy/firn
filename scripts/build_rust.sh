#!/bin/bash
set -e

echo "🦀 Building Rust library..."

cd rust

# Set macOS deployment target to avoid version warnings
if [[ "$OSTYPE" == "darwin"* ]]; then
    export MACOSX_DEPLOYMENT_TARGET=15.0
fi

# Build for release
cargo build --release

echo "✅ Rust library built successfully"

# Copy library to a location Go can find it
LIB_NAME="libfirn"
TARGET_DIR="target/release"

if [[ "$OSTYPE" == "darwin"* ]]; then
    # Copy static library for Go linking
    cp "${TARGET_DIR}/${LIB_NAME}.a" "../lib/libfirn_darwin_arm64.a"
    echo "📦 Static library copied to: ../lib/libfirn_darwin_arm64.a"
    ls -la "../lib/libfirn_darwin_arm64.a"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Copy static library for Go linking
    cp "${TARGET_DIR}/${LIB_NAME}.a" "../lib/libfirn_linux_amd64.a"
    echo "📦 Static library copied to: ../lib/libfirn_linux_amd64.a"
    ls -la "../lib/libfirn_linux_amd64.a"
else
    echo "❌ Unsupported OS: $OSTYPE"
    exit 1
fi

echo "🎉 Build complete!"
