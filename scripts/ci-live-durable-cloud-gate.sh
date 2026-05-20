#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"

summary_file="${GITHUB_STEP_SUMMARY:-}"
required="${STRATUM_LIVE_GATE_REQUIRED:-0}"
skip_provider_wrappers="${STRATUM_DURABLE_CLOUD_SKIP_PROVIDER_WRAPPERS:-0}"
output_file=""

cleanup() {
  if [[ -n "$output_file" ]]; then
    rm -f "$output_file"
  fi
}
trap cleanup EXIT

mask_value() {
  local value="$1"
  if [[ "${GITHUB_ACTIONS:-}" == "true" && -n "$value" ]]; then
    value="${value//'%'/%25}"
    value="${value//$'\r'/%0D}"
    value="${value//$'\n'/%0A}"
    printf '::add-mask::%s\n' "$value"
  fi
}

write_summary() {
  local status="$1"
  local detail="$2"
  if [[ -n "$summary_file" ]]; then
    {
      printf '### Live durable-cloud startup gate\n\n'
      printf -- '- Status: %s\n' "$status"
      printf -- '- Detail: %s\n' "$detail"
    } >>"$summary_file"
  fi
}

mask_value "${STRATUM_POSTGRES_TEST_URL:-}"
mask_value "${STRATUM_POSTGRES_TEST_PASSWORD:-}"
mask_value "${PGPASSWORD:-}"
mask_value "${PGPASSFILE:-}"
mask_value "${PGSERVICE:-}"
mask_value "${PGSERVICEFILE:-}"
mask_value "${STRATUM_R2_BUCKET:-}"
mask_value "${STRATUM_R2_ENDPOINT:-}"
mask_value "${STRATUM_R2_ACCESS_KEY_ID:-}"
mask_value "${STRATUM_R2_SECRET_ACCESS_KEY:-}"
mask_value "${STRATUM_R2_REGION:-}"
mask_value "${STRATUM_R2_PREFIX:-}"

missing=0
for var_name in \
  STRATUM_POSTGRES_TEST_URL \
  STRATUM_R2_BUCKET \
  STRATUM_R2_ENDPOINT \
  STRATUM_R2_ACCESS_KEY_ID \
  STRATUM_R2_SECRET_ACCESS_KEY
do
  if [[ -z "${!var_name:-}" ]]; then
    missing=1
  fi
done

if [[ "$missing" == "1" ]]; then
  if [[ "$required" == "1" ]]; then
    write_summary "failed live" "Required live Postgres or R2 configuration was incomplete."
    echo "durable-cloud live gate failed; required provider configuration is incomplete." >&2
    echo "failed live" >&2
    exit 2
  fi
  write_summary "skipped live" "Live Postgres or R2 configuration was incomplete."
  echo "skipped live"
  exit 0
fi

if [[ -z "${PGPASSWORD:-}" && -n "${STRATUM_POSTGRES_TEST_PASSWORD:-}" ]]; then
  export PGPASSWORD="$STRATUM_POSTGRES_TEST_PASSWORD"
fi

cd "$repo_root"
output_file="$(mktemp)"

if [[ "$skip_provider_wrappers" != "1" ]]; then
  if ! STRATUM_LIVE_GATE_REQUIRED=1 ./scripts/ci-live-postgres-gate.sh >"$output_file" 2>&1; then
    write_summary "failed live" "Live Postgres wrapper failed; command output was redacted."
    echo "durable-cloud live gate failed in Postgres wrapper; command output redacted." >&2
    echo "failed live" >&2
    exit 1
  fi

  : >"$output_file"

  if ! STRATUM_LIVE_GATE_REQUIRED=1 ./scripts/ci-live-r2-gate.sh >"$output_file" 2>&1; then
    write_summary "failed live" "Live R2 wrapper failed; command output was redacted."
    echo "durable-cloud live gate failed in R2 wrapper; command output redacted." >&2
    echo "failed live" >&2
    exit 1
  fi
fi

: >"$output_file"

if ! STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_R2_TEST_REQUIRED=1 cargo test --locked --features postgres --test server_startup \
  postgres_process_tests::durable_core_runtime_complete_env_opens_durable_stores_without_local_state \
  -- --exact --nocapture >"$output_file" 2>&1; then
  write_summary "failed live" "Durable-cloud startup tests failed; command output was redacted."
  echo "durable-cloud live gate failed in startup tests; command output redacted." >&2
  echo "failed live" >&2
  exit 1
fi

write_summary "passed live" "Live durable-cloud startup checks passed."
echo "passed live"
