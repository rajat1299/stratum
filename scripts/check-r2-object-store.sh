#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"

if [[ "${STRATUM_R2_TEST_ENABLED:-}" != "1" && "${STRATUM_R2_TEST_REQUIRED:-}" != "1" ]]; then
  echo "Skipping R2 object-store integration checks; set STRATUM_R2_TEST_ENABLED=1 to run."
  exit 0
fi

missing=()
for var_name in \
  STRATUM_R2_BUCKET \
  STRATUM_R2_ENDPOINT \
  STRATUM_R2_ACCESS_KEY_ID \
  STRATUM_R2_SECRET_ACCESS_KEY
do
  if [[ -z "${!var_name:-}" ]]; then
    missing+=("$var_name")
  fi
done

if ((${#missing[@]} > 0)); then
  printf 'Missing required R2 object-store environment variables: %s\n' "${missing[*]}" >&2
  exit 2
fi

validate_positive_bounded_int() {
  local var_name="$1"
  local max="$2"
  local value="${!var_name:-}"
  if [[ -z "$value" ]]; then
    return 0
  fi
  if [[ ! "$value" =~ ^[0-9]+$ || "$value" == "0" || "$value" -gt "$max" ]]; then
    printf 'Invalid %s; expected a positive bounded integer\n' "$var_name" >&2
    exit 2
  fi
}

validate_positive_bounded_int STRATUM_R2_REQUEST_TIMEOUT_MS 300000
validate_positive_bounded_int STRATUM_R2_CONNECT_TIMEOUT_MS 300000
validate_positive_bounded_int STRATUM_R2_MAX_ATTEMPTS 10
validate_positive_bounded_int STRATUM_R2_RETRY_BASE_DELAY_MS 300000
validate_positive_bounded_int STRATUM_R2_RETRY_MAX_DELAY_MS 300000

validate_endpoint_posture() {
  local endpoint="${STRATUM_R2_ENDPOINT:-}"
  local allow_local="${STRATUM_R2_ALLOW_INSECURE_LOCAL_ENDPOINT:-}"
  if [[ "$endpoint" == https://* ]]; then
    return 0
  fi
  if [[ ! "$endpoint" =~ ^http://(localhost|127\.0\.0\.1)([:/]|$) && ! "$endpoint" =~ ^http://\[::1\]([:/]|$) ]]; then
    printf 'Invalid STRATUM_R2_ENDPOINT; expected https endpoint\n' >&2
    exit 2
  fi

  if [[ "$allow_local" != "1" ]]; then
    printf 'Invalid STRATUM_R2_ENDPOINT; plaintext loopback endpoints require STRATUM_R2_ALLOW_INSECURE_LOCAL_ENDPOINT=1\n' >&2
    exit 2
  fi
}

validate_endpoint_posture

export STRATUM_R2_REQUEST_TIMEOUT_MS="${STRATUM_R2_REQUEST_TIMEOUT_MS:-30000}"
export STRATUM_R2_CONNECT_TIMEOUT_MS="${STRATUM_R2_CONNECT_TIMEOUT_MS:-5000}"
export STRATUM_R2_MAX_ATTEMPTS="${STRATUM_R2_MAX_ATTEMPTS:-3}"
export STRATUM_R2_RETRY_BASE_DELAY_MS="${STRATUM_R2_RETRY_BASE_DELAY_MS:-100}"
export STRATUM_R2_RETRY_MAX_DELAY_MS="${STRATUM_R2_RETRY_MAX_DELAY_MS:-5000}"

cd "$repo_root"
cargo test --locked remote::blob::tests::r2_blob_store_live_integration -- --nocapture
