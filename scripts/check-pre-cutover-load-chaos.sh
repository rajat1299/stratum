#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"

run_cargo_test() {
  local label="$1"
  shift
  printf '==> %s\n' "$label"
  (
    cd "$repo_root"
    cargo test --locked "$@"
  )
}

run_cargo_test \
  "durable-cloud FS pre-cutover load and redaction" \
  server::routes_fs::tests::durable_cloud_pre_cutover \
  --lib \
  -- \
  --nocapture

run_cargo_test \
  "durable VCS pre-cutover recovery chaos" \
  server::routes_vcs::tests::durable_pre_cutover \
  --lib \
  -- \
  --nocapture

run_cargo_test \
  "recovery scheduler pre-cutover bounded chaos" \
  server::tests::durable_recovery_scheduler_pre_cutover \
  --lib \
  -- \
  --nocapture

run_cargo_test \
  "object cleanup pre-cutover non-destructive load" \
  backend::object_cleanup::tests::pre_cutover \
  --lib \
  -- \
  --nocapture

run_cargo_test \
  "idempotency pre-cutover retry and retention pressure" \
  idempotency::tests::pre_cutover \
  --lib \
  -- \
  --nocapture

if [[ "${STRATUM_PRE_CUTOVER_LIVE:-}" == "1" ]]; then
  printf '==> optional live Postgres gate\n'
  "$repo_root/scripts/ci-live-postgres-gate.sh"

  printf '==> optional live R2 gate\n'
  "$repo_root/scripts/ci-live-r2-gate.sh"
else
  printf '==> skipped optional live provider gates; set STRATUM_PRE_CUTOVER_LIVE=1 to run them\n'
fi
