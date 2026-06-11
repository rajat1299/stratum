#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"

runbook="$repo_root/docs/durable-deployment-runbook.md"
status_doc="$repo_root/docs/project-status.md"
contract_doc="$repo_root/docs/private-beta-contract.md"

missing=0

require_file() {
  local file="$1"
  if [[ ! -f "$file" ]]; then
    printf 'Missing required file: %s\n' "$file" >&2
    missing=1
  fi
}

require_contains() {
  local file="$1"
  local needle="$2"
  local label="$3"
  if [[ ! -f "$file" ]]; then
    return
  fi
  if ! grep -Fq -- "$needle" "$file"; then
    printf 'Missing %s in %s: %s\n' "$label" "$file" "$needle" >&2
    missing=1
  fi
}

require_file "$runbook"
require_file "$status_doc"
require_file "$contract_doc"

for needle in \
  '# Durable Deployment Runbook' \
  'Task 13 / Durable Deployment Runbook' \
  'Cloudflare/Postgres/R2 posture' \
  'STRATUM_BACKEND=durable' \
  'STRATUM_CORE_RUNTIME=durable-cloud' \
  'STRATUM_POSTGRES_URL' \
  'STRATUM_POSTGRES_TEST_URL' \
  'STRATUM_POSTGRES_TEST_PASSWORD' \
  'PGPASSWORD' \
  'STRATUM_POSTGRES_SCHEMA' \
  'STRATUM_DURABLE_MIGRATION_MODE=status' \
  'STRATUM_DURABLE_MIGRATION_MODE=apply' \
  'STRATUM_DURABLE_MIGRATION_MODE=adopt' \
  'STRATUM_R2_BUCKET' \
  'STRATUM_R2_ENDPOINT' \
  'STRATUM_R2_ACCESS_KEY_ID' \
  'STRATUM_R2_SECRET_ACCESS_KEY' \
  'STRATUM_R2_REGION' \
  'STRATUM_R2_PREFIX' \
  'STRATUM_R2_ALLOW_INSECURE_LOCAL_ENDPOINT' \
  'STRATUM_DURABLE_AUTH_SESSION_READY=1' \
  'STRATUM_DURABLE_POLICY_READY=1' \
  'STRATUM_DURABLE_REPO_ROUTING_READY=1' \
  'STRATUM_DURABLE_RECOVERY_READY=1' \
  'STRATUM_DURABLE_CORE_REPO_ID' \
  'STRATUM_IDEMPOTENCY_COMPLETED_RETENTION_SECONDS' \
  'STRATUM_IDEMPOTENCY_PENDING_STALE_SECONDS' \
  'STRATUM_IDEMPOTENCY_MAX_RECORDS_PER_SCOPE' \
  'STRATUM_POSTGRES_POOL_MAX_SIZE' \
  'STRATUM_R2_REQUEST_TIMEOUT_MS' \
  '.env.live-gates' \
  'ACCOUNT_ID' \
  'API_TOKEN' \
  'token_value' \
  './scripts/check-postgres-migrations.sh' \
  './scripts/check-r2-object-store.sh' \
  './scripts/ci-live-durable-cloud-gate.sh' \
  'STRATUM_LIVE_GATE_REQUIRED=1' \
  'GET /v1/capabilities' \
  'routes.search.semantic' \
  'routes.vcs.recovery' \
  'routes.audit' \
  'routes.runs' \
  'routes.execute' \
  'sources.provider_mounts' \
  '{"error":"stratum: operation not supported: durable-cloud route is not supported yet"}' \
  'STRATUM_DURABLE_COMMIT_ROUTE=1' \
  'do not fall back to local `.vfs`' \
  'Rollback checklist' \
  'Signoff checklist'
do
  require_contains "$runbook" "$needle" "durable deployment runbook contract"
done

require_contains "$status_doc" '## Task 13 / Durable Deployment Runbook' 'project status task entry'
require_contains "$contract_doc" 'docs/durable-deployment-runbook.md' 'private beta contract runbook link'

if [[ "$missing" -ne 0 ]]; then
  exit 1
fi

echo "Durable deployment runbook contract passed."
