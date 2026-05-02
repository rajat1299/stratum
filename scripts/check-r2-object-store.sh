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

cd "$repo_root"
cargo test --locked remote::blob::tests::r2_blob_store_live_integration -- --nocapture
