#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GETTING_STARTED="$ROOT/docs/getting-started.md"
DEMO="$ROOT/docs/agent-workspace-demo.md"
RUNNER="$ROOT/scripts/run-local-golden-path-demo.sh"

fail() {
  printf 'golden-path doc check failed: %s\n' "$1" >&2
  exit 1
}

require_file() {
  local path="$1"
  [[ -f "$path" ]] || fail "missing file: ${path#$ROOT/}"
}

require_executable() {
  local path="$1"
  [[ -x "$path" ]] || fail "expected executable file: ${path#$ROOT/}"
}

require_contains() {
  local path="$1"
  local needle="$2"
  grep -Fq -- "$needle" "$path" || fail "${path#$ROOT/} missing: $needle"
}

require_not_contains() {
  local path="$1"
  local needle="$2"
  if grep -Fq -- "$needle" "$path"; then
    fail "${path#$ROOT/} still contains forbidden text: $needle"
  fi
}

require_file "$GETTING_STARTED"
require_file "$DEMO"
require_file "$RUNNER"
require_executable "$RUNNER"

for needle in \
  "## 15-Minute Private Beta Golden Path" \
  "under 15 minutes" \
  'export STRATUM_DATA_DIR="$PWD/.demo/incident-workspace"' \
  "cargo run --release --bin stratum" \
  "addagent incident-bot" \
  "STRATUM_LISTEN=127.0.0.1:3000" \
  "workspace seed-demo" \
  "source .stratum-demo/incident-workspace.env" \
  "bun run --cwd sdk/agents example:incident" \
  "scripts/run-local-golden-path-demo.sh" \
  "STRATUM_CHANGE_REQUEST_ID" \
  "STRATUM_BASELINE_COMMIT" \
  "viewed-files" \
  "/approvals" \
  "/merge" \
  "/audit?limit=25" \
  "/vcs/revert" \
  "docs/agent-workspace-demo.md"
do
  require_contains "$GETTING_STARTED" "$needle"
done

for needle in \
  "bun run --cwd sdk/agents example:incident" \
  ".stratum-demo/incident-change-request.json" \
  "STRATUM_CHANGE_REQUEST_ID" \
  "viewed-files" \
  "/approvals" \
  "/merge" \
  "/audit?limit=25" \
  "/vcs/revert" \
  "under 15 minutes"
do
  require_contains "$DEMO" "$needle"
done

for needle in \
  "workspace seed-demo" \
  "bun run --cwd sdk/agents example:incident" \
  "STRATUM_CHANGE_REQUEST_ID" \
  "STRATUM_BASELINE_COMMIT" \
  "STRATUM_UPDATE_COMMIT" \
  "viewed-files" \
  "/approvals" \
  "/merge" \
  "/audit?limit=25" \
  "/vcs/revert"
do
  require_contains "$RUNNER" "$needle"
done

require_not_contains "$DEMO" "--session-ref agent/incident-demo/session"

printf 'golden-path docs are wired to the checked-in incident example and review flow.\n'
