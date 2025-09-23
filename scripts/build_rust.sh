#!/bin/bash
set -e

echo "🦀 Building Rust library..."

cd rust

# Build for release
cargo build --release

echo "✅ Rust library built successfully"

# Copy library to a location Go can find it
LIB_NAME="libfirn"
TARGET_DIR="target/release"

if [[ "$OSTYPE" == "darwin"* ]]; then
    LIB_EXT="dylib"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    LIB_EXT="so"
else
    echo "❌ Unsupported OS: $OSTYPE"
    exit 1
fi

echo "📦 Library location: $TARGET_DIR/${LIB_NAME}.${LIB_EXT}"
ls -la "$TARGET_DIR/${LIB_NAME}.${LIB_EXT}"

echo "🎉 Build complete!"
