#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"

if [[ -z "${STRATUM_POSTGRES_TEST_URL:-}" ]]; then
  if [[ "${STRATUM_POSTGRES_MIGRATIONS_REQUIRED:-}" == "1" || "${GITHUB_ACTIONS:-}" == "true" ]]; then
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

psql "$STRATUM_POSTGRES_TEST_URL" \
  -v ON_ERROR_STOP=1 \
  -f "$repo_root/tests/postgres/0001_durable_backend_foundation_smoke.sql"
