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
    env \
      -u STRATUM_POSTGRES_TEST_URL \
      -u STRATUM_POSTGRES_TEST_PASSWORD \
      -u STRATUM_POSTGRES_TEST_REQUIRED \
      -u STRATUM_POSTGRES_MIGRATIONS_REQUIRED \
      -u STRATUM_POSTGRES_MIGRATIONS_ADOPT_SMOKE \
      -u PGPASSWORD \
      -u PGPASSFILE \
      -u PGSERVICE \
      -u PGSERVICEFILE \
      -u GITHUB_ACTIONS \
      -u STRATUM_R2_TEST_ENABLED \
      -u STRATUM_R2_TEST_REQUIRED \
      -u STRATUM_R2_BUCKET \
      -u STRATUM_R2_ENDPOINT \
      -u STRATUM_R2_ACCESS_KEY_ID \
      -u STRATUM_R2_SECRET_ACCESS_KEY \
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

run_cargo_test \
  "R2 adapter error redaction" \
  remote::blob::tests::r2_operation_errors_are_redacted \
  --lib \
  -- \
  --nocapture

run_cargo_test \
  "server startup durable fail-closed gates" \
  --test \
  server_startup \
  durable \
  -- \
  --nocapture

run_cargo_test \
  "server startup durable Postgres-feature gates" \
  --features \
  postgres \
  --test \
  server_startup \
  durable \
  -- \
  --nocapture

if [[ "${STRATUM_PRE_CUTOVER_LIVE:-}" == "1" ]]; then
  printf '==> optional live Postgres gate\n'
  "$repo_root/scripts/ci-live-postgres-gate.sh"

  printf '==> optional live R2 gate\n'
  "$repo_root/scripts/ci-live-r2-gate.sh"

  printf '==> optional live durable-cloud startup gate\n'
  "$repo_root/scripts/ci-live-durable-cloud-gate.sh"
else
  printf '==> skipped optional live provider gates; set STRATUM_PRE_CUTOVER_LIVE=1 to run them\n'
fi
