\
#!/usr/bin/env bash
set -euo pipefail

# Windows (amd64) build script for Rust -> DLL used by Go/cgo.
#
# Matches Go side:
#   //go:build windows && amd64
#   #cgo LDFLAGS: -lfirn
#
# Assumes Cargo.toml:
#   [lib]
#   name = "firn"
#
# Produces canonical GNU outputs:
#   - firn.dll
#   - libfirn.dll.a   (import library for MinGW/GCC; required for cgo link)
#   - libfirn.a       (optional static library)
#
# Output location:
#   repo-root ./lib/

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"
CRATE_DIR="${CRATE_DIR:-rust}"
TARGET="${TARGET:-x86_64-pc-windows-gnu}"
PROFILE="${PROFILE:-release}"

cd "${REPO_ROOT}/${CRATE_DIR}"

echo "[build_rust_win] repo_root=${REPO_ROOT}"
echo "[build_rust_win] crate_dir=${CRATE_DIR}"
echo "[build_rust_win] target=${TARGET}"
echo "[build_rust_win] profile=${PROFILE}"

# On Windows hosts, guard against mixing MSVC and GNU/MinGW toolchains
if [[ "$OSTYPE" == "msys"* || "$OSTYPE" == "cygwin"* || "$OSTYPE" == "win"* ]]; then
  rust_host="$(rustc -vV | sed -n 's/^host: //p')"
  echo "[build_rust_win] rustc host=${rust_host}"

  # Ensure we are building a GNU (MinGW) target, not MSVC
  if [[ "$TARGET" != *-pc-windows-gnu ]]; then
    echo "[build_rust_win] ERROR: On Windows host, TARGET must be a *-pc-windows-gnu triple to use MinGW/GNU toolchain. Got: ${TARGET}"
    exit 1
  fi

  # Prevent using MSVC Rust toolchain with a GNU target
  if [[ "${rust_host}" == *-pc-windows-msvc ]]; then
    echo "[build_rust_win] ERROR: Detected MSVC Rust toolchain (${rust_host}) on Windows, which will conflict with GNU/MinGW target."
    echo "[build_rust_win] Please install and use the x86_64-pc-windows-gnu Rust toolchain (e.g. 'rustup toolchain install stable-x86_64-pc-windows-gnu')."
    exit 1
  fi
fi

cargo build --release --target "${TARGET}"

TARGET_DIR="target/${TARGET}/${PROFILE}"

SRC_DLL="${TARGET_DIR}/firn.dll"
SRC_IMPLIB="${TARGET_DIR}/libfirn.dll.a"
SRC_STATIC="${TARGET_DIR}/libfirn.a"

OUT_DIR="${REPO_ROOT}/lib"
mkdir -p "${OUT_DIR}"

if [[ ! -f "${SRC_DLL}" ]]; then
  echo "[build_rust_win] ERROR: missing ${SRC_DLL}"
  exit 1
fi
if [[ ! -f "${SRC_IMPLIB}" ]]; then
  echo "[build_rust_win] ERROR: missing ${SRC_IMPLIB}"
  echo "  This file is required for Go/cgo + MinGW linking with -lfirn."
  exit 1
fi

cp -f "${SRC_DLL}" "${OUT_DIR}/firn.dll"
cp -f "${SRC_IMPLIB}" "${OUT_DIR}/libfirn.dll.a"

if [[ -f "${SRC_STATIC}" ]]; then
  cp -f "${SRC_STATIC}" "${OUT_DIR}/libfirn.a"
else
  echo "[build_rust_win] WARN: ${SRC_STATIC} not found (ok if you only need DLL + import lib)."
fi

echo "[build_rust_win] OK: artifacts in ${OUT_DIR}"
ls -la "${OUT_DIR}" | sed -n '1,200p'
