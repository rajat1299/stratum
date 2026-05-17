#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"

if [[ -z "${STRATUM_POSTGRES_TEST_URL:-}" ]]; then
  if [[ "${STRATUM_POSTGRES_MIGRATIONS_REQUIRED:-}" == "1" || "${STRATUM_POSTGRES_MIGRATIONS_ADOPT_SMOKE:-}" == "1" || "${GITHUB_ACTIONS:-}" == "true" ]]; then
    echo "STRATUM_POSTGRES_TEST_URL is required for Postgres migration smoke checks." >&2
    exit 2
  fi
  echo "Skipping Postgres migration smoke checks; STRATUM_POSTGRES_TEST_URL is unset."
  exit 0
fi

shopt -s nocasematch
if [[ "$STRATUM_POSTGRES_TEST_URL" =~ ://[^/?#@]+:[^/?#@]+@ || "$STRATUM_POSTGRES_TEST_URL" =~ [\?\&]password= || "$STRATUM_POSTGRES_TEST_URL" =~ (^|[[:space:]])password[[:space:]]*= ]]; then
  shopt -u nocasematch
  echo "STRATUM_POSTGRES_TEST_URL must not include a password; use PGPASSWORD, PGPASSFILE, or PGSERVICE." >&2
  exit 2
fi
shopt -u nocasematch

if ! command -v psql >/dev/null 2>&1; then
  echo "psql is required to run Postgres migration smoke checks." >&2
  exit 127
fi

if [[ "${STRATUM_POSTGRES_REDACT_ERRORS:-}" == "1" ]]; then
  output_file="$(mktemp)"
  trap 'rm -f "$output_file"' EXIT
  if ! psql "$STRATUM_POSTGRES_TEST_URL" \
    -v ON_ERROR_STOP=1 \
    -f "$repo_root/tests/postgres/0001_durable_backend_foundation_smoke.sql" \
    >"$output_file" 2>&1; then
    echo "Postgres migration smoke checks failed." >&2
    exit 1
  fi
  cat "$output_file"
else
  psql "$STRATUM_POSTGRES_TEST_URL" \
    -v ON_ERROR_STOP=1 \
    -f "$repo_root/tests/postgres/0001_durable_backend_foundation_smoke.sql"
fi

if [[ "${STRATUM_POSTGRES_MIGRATIONS_ADOPT_SMOKE:-}" == "1" ]]; then
  (
    cd "$repo_root"
    STRATUM_POSTGRES_TEST_REQUIRED=1 \
      cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
  )
fi
