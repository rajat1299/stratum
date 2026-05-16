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
    printf '::add-mask::%s\n' "$value"
  fi
}

write_summary() {
  local status="$1"
  local detail="$2"
  if [[ -n "$summary_file" ]]; then
    {
      printf '### Live R2 gate\n\n'
      printf '- Status: %s\n' "$status"
      printf '- Detail: %s\n' "$detail"
    } >>"$summary_file"
  fi
}

mask_value "${STRATUM_R2_BUCKET:-}"
mask_value "${STRATUM_R2_ENDPOINT:-}"
mask_value "${STRATUM_R2_ACCESS_KEY_ID:-}"
mask_value "${STRATUM_R2_SECRET_ACCESS_KEY:-}"
mask_value "${STRATUM_R2_REGION:-}"
mask_value "${STRATUM_R2_PREFIX:-}"

missing=0
for var_name in \
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
    write_summary "failed live" "Required live R2 environment was incomplete."
    echo "R2 live gate failed; required configuration is incomplete." >&2
    echo "failed live" >&2
    exit 2
  fi
  write_summary "skipped live" "Live R2 environment was incomplete."
  echo "skipped live"
  exit 0
fi

cd "$repo_root"
output_file="$(mktemp)"

if ! STRATUM_R2_TEST_REQUIRED=1 ./scripts/check-r2-object-store.sh >"$output_file" 2>&1; then
  write_summary "failed live" "R2 live checks failed; command output was redacted."
  echo "R2 live gate failed; command output redacted." >&2
  echo "failed live" >&2
  exit 1
fi

write_summary "passed live" "Live R2 checks passed."
echo "passed live"
