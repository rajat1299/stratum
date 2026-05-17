#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"

summary_file="${GITHUB_STEP_SUMMARY:-}"
required="${STRATUM_LIVE_GATE_REQUIRED:-0}"
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
      printf '### Live Postgres gate\n\n'
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

if [[ -z "${STRATUM_POSTGRES_TEST_URL:-}" ]]; then
  if [[ "$required" == "1" ]]; then
    write_summary "failed live" "Required live Postgres URL was missing."
    echo "Postgres live gate failed; required URL is missing." >&2
    echo "failed live" >&2
    exit 2
  fi
  write_summary "skipped live" "Live Postgres URL was missing."
  echo "skipped live"
  exit 0
fi

if [[ -z "${PGPASSWORD:-}" && -n "${STRATUM_POSTGRES_TEST_PASSWORD:-}" ]]; then
  export PGPASSWORD="$STRATUM_POSTGRES_TEST_PASSWORD"
fi

cd "$repo_root"
output_file="$(mktemp)"

if ! STRATUM_POSTGRES_MIGRATIONS_REQUIRED=1 STRATUM_POSTGRES_REDACT_ERRORS=1 ./scripts/check-postgres-migrations.sh >"$output_file" 2>&1; then
  write_summary "failed live" "Postgres migration smoke checks failed; command output was redacted."
  echo "Postgres live gate failed; command output redacted." >&2
  echo "failed live" >&2
  exit 1
fi

: >"$output_file"

if ! STRATUM_POSTGRES_TEST_REQUIRED=1 cargo test --locked --features postgres backend::postgres --lib -- --test-threads=1 --nocapture >"$output_file" 2>&1; then
  write_summary "failed live" "Postgres backend live tests failed; command output was redacted."
  echo "Postgres live gate failed; command output redacted." >&2
  echo "failed live" >&2
  exit 1
fi

write_summary "passed live" "Live Postgres checks passed."
echo "passed live"
