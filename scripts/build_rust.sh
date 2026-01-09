#!/bin/bash
set -e

echo "🦀 Building Rust library..."

cd rust
mkdir -p ../lib

# Set macOS deployment target to avoid version warnings
if [[ "$OSTYPE" == "darwin"* ]]; then
    export MACOSX_DEPLOYMENT_TARGET=14.0
fi

LIB_NAME="firn"

if [[ "$OSTYPE" == "darwin"* ]]; then
    # Build for release (native by default)
    cargo build --release
    TARGET_DIR="target/release"

    # 原先带有 darwin_arm64 后缀的命名保留为注释，仅使用原始库名
    # cp "${TARGET_DIR}/lib${LIB_NAME}.a" "../lib/libfirn_darwin_arm64.a"
    cp "${TARGET_DIR}/lib${LIB_NAME}.a" "../lib/lib${LIB_NAME}.a"
    echo "📦 Static library copied to: ../lib/lib${LIB_NAME}.a"
    ls -la "../lib/lib${LIB_NAME}.a"
    # 复制动态链接库（.dylib）
    if [ -f "${TARGET_DIR}/lib${LIB_NAME}.dylib" ]; then
        # cp "${TARGET_DIR}/lib${LIB_NAME}.dylib" "../lib/libfirn_darwin_arm64.dylib"
        cp "${TARGET_DIR}/lib${LIB_NAME}.dylib" "../lib/lib${LIB_NAME}.dylib"
        echo "📦 Dynamic library copied to: ../lib/lib${LIB_NAME}.dylib"
        ls -la "../lib/lib${LIB_NAME}.dylib"
        # install_name_tool -id "@rpath/libfirn_darwin_arm64.dylib" "../lib/libfirn_darwin_arm64.dylib"
        install_name_tool -id "@rpath/lib${LIB_NAME}.dylib" "../lib/lib${LIB_NAME}.dylib"
    fi
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Build for release (native by default)
    cargo build --release
    TARGET_DIR="target/release"

    # 原先带有 linux_amd64 后缀的命名保留为注释，仅使用原始库名
    # cp "${TARGET_DIR}/lib${LIB_NAME}.a" "../lib/libfirn_linux_amd64.a"
    cp "${TARGET_DIR}/lib${LIB_NAME}.a" "../lib/lib${LIB_NAME}.a"
    echo "📦 Static library copied to: ../lib/lib${LIB_NAME}.a"
    ls -la "../lib/lib${LIB_NAME}.a"
    # 复制动态链接库（.so）
    if [ -f "${TARGET_DIR}/lib${LIB_NAME}.so" ]; then
        # cp "${TARGET_DIR}/lib${LIB_NAME}.so" "../lib/libfirn_linux_amd64.so"
        cp "${TARGET_DIR}/lib${LIB_NAME}.so" "../lib/lib${LIB_NAME}.so"
        echo "📦 Dynamic library copied to: ../lib/lib${LIB_NAME}.so"
        ls -la "../lib/lib${LIB_NAME}.so"
    fi
elif [[ "$OSTYPE" == "msys"* || "$OSTYPE" == "cygwin"* || "$OSTYPE" == "win"* ]]; then
    # Build for MinGW/GNU target (commonly used with cgo + gcc on Windows)
    cargo build --release --target x86_64-pc-windows-gnu
    TARGET_DIR="target/x86_64-pc-windows-gnu/release"

    # 复制静态库（staticlib -> .a）
    if [ -f "${TARGET_DIR}/lib${LIB_NAME}.a" ]; then
        # cp "${TARGET_DIR}/lib${LIB_NAME}.a" "../lib/libfirn_windows_amd64.a"
        cp "${TARGET_DIR}/lib${LIB_NAME}.a" "../lib/lib${LIB_NAME}.a"
        echo "📦 Static library copied to: ../lib/lib${LIB_NAME}.a"
        ls -la "../lib/lib${LIB_NAME}.a"
    else
        echo "⚠️  Static library not found: ${TARGET_DIR}/lib${LIB_NAME}.a"
        ls -la "${TARGET_DIR}" || true
    fi

    # 复制动态链接库（cdylib -> .dll）
    if [ -f "${TARGET_DIR}/${LIB_NAME}.dll" ]; then
        # cp "${TARGET_DIR}/${LIB_NAME}.dll" "../lib/firn_windows_amd64.dll"
        cp "${TARGET_DIR}/${LIB_NAME}.dll" "../lib/${LIB_NAME}.dll"
        echo "📦 Dynamic library copied to: ../lib/${LIB_NAME}.dll"
        ls -la "../lib/${LIB_NAME}.dll"
    else
        echo "⚠️  DLL not found: ${TARGET_DIR}/${LIB_NAME}.dll"
    fi

    # 复制 DLL 导入库（import library for gcc -> .dll.a）
    if [ -f "${TARGET_DIR}/lib${LIB_NAME}.dll.a" ]; then
        # cp "${TARGET_DIR}/lib${LIB_NAME}.dll.a" "../lib/libfirn_windows_amd64.dll.a"
        cp "${TARGET_DIR}/lib${LIB_NAME}.dll.a" "../lib/lib${LIB_NAME}.dll.a"
        echo "📦 Import library copied to: ../lib/lib${LIB_NAME}.dll.a"
        ls -la "../lib/lib${LIB_NAME}.dll.a"
    else
        echo "⚠️  Import library not found: ${TARGET_DIR}/lib${LIB_NAME}.dll.a"
        # 并非所有构建都会生成（例如只产静态库时），所以这里仅提示
    fi
#elif [[ "$OSTYPE" == "msys"* || "$OSTYPE" == "cygwin"* || "$OSTYPE" == "win"* ]]; then
#    # Build for MSVC target to match Windows runners
#    cargo build --release --target x86_64-pc-windows-msvc
#    TARGET_DIR="target/x86_64-pc-windows-msvc/release"
#    cp "${TARGET_DIR}/${LIB_NAME}.lib" "../lib/firn_windows_amd64.lib"
#    echo "📦 Static library copied to: ../lib/firn_windows_amd64.lib"
#    ls -la "../lib/firn_windows_amd64.lib"
#    # 复制动态链接库（.dll）
#    if [ -f "${TARGET_DIR}/${LIB_NAME}.dll" ]; then
#        cp "${TARGET_DIR}/${LIB_NAME}.dll" "../lib/firn_windows_amd64.dll"
#        echo "📦 Dynamic library copied to: ../lib/firn_windows_amd64.dll"
#        ls -la "../lib/firn_windows_amd64.dll"
#    fi
else
    echo "❌ Unsupported OS: $OSTYPE"
    exit 1
fi

echo "✅ Rust library built successfully"
echo "🎉 Build complete!"
