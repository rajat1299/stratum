#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_ROOT="${TMPDIR:-/tmp}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${TMP_ROOT%/}/stratum-local-verification-target}"

run() {
  printf '\n> %s\n' "$*"
  "$@"
}

run_in() {
  local dir="$1"
  shift
  printf '\n> (cd %s && %s)\n' "$dir" "$*"
  (cd "$dir" && "$@")
}

run cargo fmt --manifest-path "$ROOT/Cargo.toml" --all -- --check
run cargo test --manifest-path "$ROOT/Cargo.toml" --locked --lib --bins

run_in "$ROOT/sdk" bun install --frozen-lockfile
run_in "$ROOT/sdk" bun run typecheck
run_in "$ROOT/sdk" bun run test:run
run_in "$ROOT/sdk" bun run build

run_in "$ROOT" pnpm install --frozen-lockfile

rm -rf "$ROOT/sdk/typescript/dist"
run_in "$ROOT/web" pnpm typecheck
run_in "$ROOT/web" pnpm test:run
run_in "$ROOT/web" pnpm build

run_in "$ROOT" git diff --check
