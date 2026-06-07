#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TARGET="${WASM_TARGET:-wasm32-unknown-unknown}"

if ! rustup target list --installed | grep -qx "$TARGET"; then
  echo "Installing Rust target $TARGET..."
  rustup target add "$TARGET"
fi

echo "Building fusion-logic for $TARGET (W1 pure logic API)..."
cargo build -p fusion-logic --release --target "$TARGET" --features wasm

OUT="$ROOT/target/$TARGET/release"
echo "WASM artifact: $OUT/fusion_logic.wasm (or fusion_logic.wasm depending on platform naming)"
ls -la "$OUT"/fusion_logic* 2>/dev/null || ls -la "$OUT"/*.wasm 2>/dev/null || true

echo "Running fusion-logic unit tests (host)..."
cargo test -p fusion-logic
