#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail() {
  printf 'local golden-path demo failed: %s\n' "$1" >&2
  exit 1
}

require_command() {
  local name="$1"
  command -v "$name" >/dev/null 2>&1 || fail "missing required command: $name"
}

require_command cargo
require_command bun
require_command curl
require_command jq

: "${STRATUM_AGENT_TOKEN:?Set STRATUM_AGENT_TOKEN to the backing token printed by addagent.}"
export STRATUM_AGENT_TOKEN

export STRATUM_URL="${STRATUM_URL:-http://127.0.0.1:3000}"
export STRATUM_ADMIN_USER="${STRATUM_ADMIN_USER:-root}"
export STRATUM_REVIEWER_USER="${STRATUM_REVIEWER_USER:-alice}"

STRATUM_DEMO_ENV="${STRATUM_DEMO_ENV:-.stratum-demo/incident-workspace.env}"
STRATUM_DEMO_RESULT="${STRATUM_DEMO_RESULT:-.stratum-demo/incident-change-request.json}"

if [[ -e "$STRATUM_DEMO_ENV" ]]; then
  fail "refusing to overwrite existing ${STRATUM_DEMO_ENV}; remove .stratum-demo for a fresh run"
fi

printf 'Checking Stratum server at %s...\n' "$STRATUM_URL"
curl -fsS "${STRATUM_URL%/}/health" >/dev/null

printf 'Seeding local incident workspace...\n'
cargo run --release --bin stratumctl -- \
  --url "$STRATUM_URL" \
  --user "$STRATUM_ADMIN_USER" \
  workspace seed-demo \
  --env-out "$STRATUM_DEMO_ENV"

set -a
# shellcheck source=/dev/null
source "$STRATUM_DEMO_ENV"
set +a

export STRATUM_ADMIN_USER

mkdir -p "$(dirname "$STRATUM_DEMO_RESULT")"

printf 'Running checked-in agent incident change-request example...\n'
bun run --cwd sdk/agents example:incident | tee "$STRATUM_DEMO_RESULT"

json_field() {
  local field="$1"
  local value
  value="$(jq -r --arg field "$field" '.[$field] // empty' "$STRATUM_DEMO_RESULT")"
  [[ -n "$value" ]] || fail "missing ${field} in ${STRATUM_DEMO_RESULT}"
  printf '%s' "$value"
}

export STRATUM_CHANGE_REQUEST_ID
export STRATUM_BASELINE_COMMIT
export STRATUM_UPDATE_COMMIT
STRATUM_CHANGE_REQUEST_ID="$(json_field changeRequestId)"
STRATUM_BASELINE_COMMIT="$(json_field baselineCommit)"
STRATUM_UPDATE_COMMIT="$(json_field updateCommit)"

repo_header=()
if [[ -n "${STRATUM_REPO:-}" ]]; then
  repo_header=(-H "X-Stratum-Repo: $STRATUM_REPO")
fi

api_url() {
  printf '%s%s' "${STRATUM_URL%/}" "$1"
}

put_viewed_file() {
  local path="$1"
  curl -fsS -X PUT "$(api_url "/change-requests/${STRATUM_CHANGE_REQUEST_ID}/viewed-files")" \
    "${repo_header[@]}" \
    -H "Authorization: User ${STRATUM_ADMIN_USER}" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: local-golden-path-viewed-${path//[^A-Za-z0-9]/-}" \
    --data "$(jq -cn --arg path "$path" '{path:$path, viewed:true}')" | jq
}

printf 'Marking generated files viewed on change request %s...\n' "$STRATUM_CHANGE_REQUEST_ID"
put_viewed_file "/incidents/checkout-latency/root-cause.md"
put_viewed_file "/incidents/checkout-latency/remediation.md"

printf 'Approving as %s...\n' "$STRATUM_REVIEWER_USER"
curl -fsS -X POST "$(api_url "/change-requests/${STRATUM_CHANGE_REQUEST_ID}/approvals")" \
  "${repo_header[@]}" \
  -H "Authorization: User ${STRATUM_REVIEWER_USER}" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: local-golden-path-approve-1" \
  --data "$(jq -cn --arg comment "Reviewed for the local private-beta golden path." '{comment:$comment}')" | jq

printf 'Merging as %s...\n' "$STRATUM_ADMIN_USER"
curl -fsS -X POST "$(api_url "/change-requests/${STRATUM_CHANGE_REQUEST_ID}/merge")" \
  "${repo_header[@]}" \
  -H "Authorization: User ${STRATUM_ADMIN_USER}" \
  -H "Idempotency-Key: local-golden-path-merge-1" | jq

printf 'Recent audit evidence after merge...\n'
curl -fsS "$(api_url "/audit?limit=25")" \
  -H "Authorization: User ${STRATUM_ADMIN_USER}" | \
  jq '.events[] | {action, resource, route, details}'

printf 'Reverting main to baseline commit %s...\n' "$STRATUM_BASELINE_COMMIT"
curl -fsS -X POST "$(api_url "/vcs/revert")" \
  "${repo_header[@]}" \
  -H "Authorization: User ${STRATUM_ADMIN_USER}" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: local-golden-path-revert-1" \
  --data "$(jq -cn --arg hash "$STRATUM_BASELINE_COMMIT" '{hash:$hash}')" | jq

printf 'Recent audit evidence after revert...\n'
curl -fsS "$(api_url "/audit?limit=25")" \
  -H "Authorization: User ${STRATUM_ADMIN_USER}" | \
  jq '.events[] | {action, resource, route, details}'

printf 'Local golden path complete. Evidence JSON: %s\n' "$STRATUM_DEMO_RESULT"
