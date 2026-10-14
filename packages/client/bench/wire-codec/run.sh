#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../../.."
export TMPDIR=/home/gabriel/.cache/sdk-wave1/tmp
mkdir -p "$TMPDIR" packages/client/bench/wire-codec/output
pnpm --filter @gonvex/protocol build
CARGO_BUILD_JOBS=2 cargo build --manifest-path rust/Cargo.toml -p gonvex-protocol --example wire-codec --release
esbuild=(node_modules/.pnpm/esbuild@*/node_modules/esbuild/bin/esbuild)
"${esbuild[0]}" packages/client/bench/wire-codec/browser.mjs --bundle --format=iife --outfile=packages/client/bench/wire-codec/output/bundle.js
node --expose-gc packages/client/bench/wire-codec/run.mjs
