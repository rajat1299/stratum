# Live Gates In CI Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add PR-safe and protected/manual/scheduled CI coverage for live Postgres and R2 gates without exposing credentials or weakening existing local CI.

**Architecture:** Keep the existing local-service Postgres jobs and optional no-secret R2 skip behavior as normal PR coverage. Add CI-only live wrappers that fail closed when required live secrets are missing, mask configured secret values, capture failure output so raw backend/provider errors are not logged, and write GitHub step summaries for `skipped`, `passed live`, and `failed live` states. Wire those wrappers into protected-branch, scheduled, and manual workflow contexts while leaving pull-request runs visibly skipped.

**Tech Stack:** GitHub Actions, Bash, Rust 2024, Cargo with optional `postgres` feature, existing Postgres migration smoke script, existing R2 object-store round-trip script, and repo docs.

---

## Reference Material Used

- `markdownfs_v2_cto_architecture_plan.md`
- `docs/project-status.md`
- `docs/plans/2026-05-15-backend-roadmap.md`
- `docs/plans/2026-05-15-capability-manifest-endpoint.md`
- `docs/plans/2026-05-14-hosted-storage-operations-hardening.md`
- `docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md`
- `.github/workflows/rust-ci.yml`
- `scripts/check-postgres-migrations.sh`
- `scripts/check-r2-object-store.sh`
- `docs/http-api-guide.md`
- GitHub Actions docs for secret behavior, job/step conditionals, `workflow_dispatch`, and fork pull-request secret withholding.

## Current Baseline

- `.github/workflows/rust-ci.yml` runs on `pull_request` and pushes to `main` or `v2/**`.
- Existing non-live CI includes format, default clippy, local Postgres migration smoke through a service container, Postgres backend clippy/tests through a service container, R2 script syntax plus optional skip, default tests, optional FUSE compile, and cargo audit.
- `scripts/check-postgres-migrations.sh` skips locally when `STRATUM_POSTGRES_TEST_URL` is unset, but fails in required mode or any GitHub Actions run without that URL.
- `scripts/check-r2-object-store.sh` skips unless `STRATUM_R2_TEST_ENABLED=1` or `STRATUM_R2_TEST_REQUIRED=1`, validates required R2 env names, then runs the live R2 test.
- Optional local live gates currently skip when env is unset. Required live Postgres and R2 gates fail before live work when required env is missing.

## GitHub Secret Contract

Configure these repository or environment secrets before expecting the live contexts to pass:

- `STRATUM_LIVE_POSTGRES_TEST_URL`: password-free Postgres connection string for the live test database.
- `STRATUM_LIVE_POSTGRES_TEST_PASSWORD`: password for that live test database; the workflow maps it to both `STRATUM_POSTGRES_TEST_PASSWORD` and `PGPASSWORD`.
- `STRATUM_LIVE_R2_BUCKET`: live R2/S3-compatible bucket name. Treat as sensitive in CI logs.
- `STRATUM_LIVE_R2_ENDPOINT`: live R2/S3-compatible HTTPS endpoint. Treat as sensitive in CI logs.
- `STRATUM_LIVE_R2_ACCESS_KEY_ID`: live R2 access key id.
- `STRATUM_LIVE_R2_SECRET_ACCESS_KEY`: live R2 secret access key.

Optional live R2 tuning secrets or variables may be supplied when needed:

- `STRATUM_LIVE_R2_REGION`
- `STRATUM_LIVE_R2_PREFIX`
- `STRATUM_LIVE_R2_REQUEST_TIMEOUT_MS`
- `STRATUM_LIVE_R2_CONNECT_TIMEOUT_MS`
- `STRATUM_LIVE_R2_MAX_ATTEMPTS`
- `STRATUM_LIVE_R2_RETRY_BASE_DELAY_MS`
- `STRATUM_LIVE_R2_RETRY_MAX_DELAY_MS`

Do not place passwords inside `STRATUM_LIVE_POSTGRES_TEST_URL`. The scripts must reject password-bearing URLs and must not print DB URLs, passwords, R2 endpoints, bucket names, access keys, secret keys, object keys, or raw backend/provider errors.

## Task 1: Add CI Live Gate Wrappers

**Files:**
- Create: `scripts/ci-live-postgres-gate.sh`
- Create: `scripts/ci-live-r2-gate.sh`
- Modify: `scripts/check-postgres-migrations.sh`

**Step 1: Write wrapper behavior**

Add two Bash wrappers with these shared behaviors:

- `set -euo pipefail`.
- `STRATUM_LIVE_GATE_REQUIRED=1` means missing required env fails; otherwise missing env skips.
- Mask every non-empty secret-bearing value with `::add-mask::` when `GITHUB_ACTIONS=true`.
- Write a short section to `$GITHUB_STEP_SUMMARY` when available.
- Print only fixed status lines to stdout/stderr:
  - `skipped live`
  - `passed live`
  - `failed live`
- Never print secret values, endpoint values, bucket names, DB URLs, object keys, or captured raw command output on failure.

**Step 2: Implement Postgres wrapper**

Required env for required live mode:

- `STRATUM_POSTGRES_TEST_URL`
- one of `STRATUM_POSTGRES_TEST_PASSWORD` or `PGPASSWORD`

The wrapper must run these commands with required-live semantics:

```bash
STRATUM_POSTGRES_MIGRATIONS_REQUIRED=1 \
STRATUM_POSTGRES_REDACT_ERRORS=1 \
  ./scripts/check-postgres-migrations.sh

STRATUM_POSTGRES_TEST_REQUIRED=1 \
  cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Capture command output to a temporary file. On success, the wrapper may print safe normal command output. On failure, suppress captured output and print only a redacted fixed failure message plus the live-gate status.

**Step 3: Add redacted migration script mode**

Update `scripts/check-postgres-migrations.sh` so `STRATUM_POSTGRES_REDACT_ERRORS=1` captures `psql` output and, on failure, prints only:

```text
Postgres migration smoke checks failed.
```

The default local behavior can keep current raw `psql` output for developer debugging unless redacted mode is set. Existing skip, required-missing, password-bearing URL, and `psql` missing behavior must remain unchanged.

**Step 4: Implement R2 wrapper**

Required env for required live mode:

- `STRATUM_R2_BUCKET`
- `STRATUM_R2_ENDPOINT`
- `STRATUM_R2_ACCESS_KEY_ID`
- `STRATUM_R2_SECRET_ACCESS_KEY`

The wrapper must run:

```bash
STRATUM_R2_TEST_REQUIRED=1 ./scripts/check-r2-object-store.sh
```

Capture command output. On success, the wrapper may print safe normal command output. On failure, suppress captured output and print only a redacted fixed failure message plus the live-gate status.

**Step 5: Verify and commit**

Run:

```bash
bash -n scripts/check-postgres-migrations.sh scripts/check-r2-object-store.sh scripts/ci-live-postgres-gate.sh scripts/ci-live-r2-gate.sh
STRATUM_LIVE_GATE_REQUIRED=0 STRATUM_POSTGRES_TEST_URL= ./scripts/ci-live-postgres-gate.sh
STRATUM_LIVE_GATE_REQUIRED=1 STRATUM_POSTGRES_TEST_URL= ./scripts/ci-live-postgres-gate.sh
STRATUM_LIVE_GATE_REQUIRED=0 STRATUM_R2_TEST_ENABLED= ./scripts/ci-live-r2-gate.sh
STRATUM_LIVE_GATE_REQUIRED=1 env -u STRATUM_R2_BUCKET ./scripts/ci-live-r2-gate.sh
git diff --check
```

Expected:

- Syntax check passes.
- Optional wrapper runs skip cleanly.
- Required wrapper runs fail before live work when env is missing.
- No command prints a secret placeholder value supplied by tests.

Commit:

```bash
git add scripts/check-postgres-migrations.sh scripts/ci-live-postgres-gate.sh scripts/ci-live-r2-gate.sh
git commit -m "ci: add redacted live gate wrappers"
```

## Task 2: Wire Live Jobs Into Rust CI

**Files:**
- Modify: `.github/workflows/rust-ci.yml`

**Step 1: Add live triggers**

Add:

```yaml
workflow_dispatch:
schedule:
  - cron: "17 8 * * *"
```

Keep existing `pull_request` and push branch triggers unchanged.

**Step 2: Add visible PR/non-live summary**

Add a lightweight job that always runs and writes a `$GITHUB_STEP_SUMMARY` table for the live gates:

- On `pull_request`: Postgres and R2 are `skipped`; reason is `pull_request runs do not use live secrets`.
- On non-protected non-scheduled non-manual pushes: Postgres and R2 are `skipped`; reason is `not a protected, scheduled, or manual live context`.
- On live contexts: Postgres and R2 are `required`; reason is `protected, scheduled, or manual live context`.

Do not reference `secrets.*` in an `if:` expression. GitHub does not allow direct secret references in conditionals; use job `env` and wrapper checks instead.

**Step 3: Add required live Postgres job**

Add a `live-postgres` job that runs only for:

```yaml
if: ${{ github.event_name == 'schedule' || github.event_name == 'workflow_dispatch' || github.ref_protected == true }}
```

Steps:

- checkout with `persist-credentials: false`
- install Rust stable
- install PostgreSQL client
- run `bash -n` for the Postgres scripts
- run `./scripts/ci-live-postgres-gate.sh`

Map secrets to env:

```yaml
STRATUM_LIVE_GATE_REQUIRED: "1"
STRATUM_POSTGRES_TEST_URL: ${{ secrets.STRATUM_LIVE_POSTGRES_TEST_URL }}
STRATUM_POSTGRES_TEST_PASSWORD: ${{ secrets.STRATUM_LIVE_POSTGRES_TEST_PASSWORD }}
PGPASSWORD: ${{ secrets.STRATUM_LIVE_POSTGRES_TEST_PASSWORD }}
```

**Step 4: Add required live R2 job**

Add a `live-r2` job with the same live-context `if`.

Steps:

- checkout with `persist-credentials: false`
- install Rust stable
- run `bash -n` for R2 scripts
- run `./scripts/ci-live-r2-gate.sh`

Map secrets to env:

```yaml
STRATUM_LIVE_GATE_REQUIRED: "1"
STRATUM_R2_BUCKET: ${{ secrets.STRATUM_LIVE_R2_BUCKET }}
STRATUM_R2_ENDPOINT: ${{ secrets.STRATUM_LIVE_R2_ENDPOINT }}
STRATUM_R2_ACCESS_KEY_ID: ${{ secrets.STRATUM_LIVE_R2_ACCESS_KEY_ID }}
STRATUM_R2_SECRET_ACCESS_KEY: ${{ secrets.STRATUM_LIVE_R2_SECRET_ACCESS_KEY }}
STRATUM_R2_REGION: ${{ secrets.STRATUM_LIVE_R2_REGION }}
STRATUM_R2_PREFIX: ${{ secrets.STRATUM_LIVE_R2_PREFIX }}
STRATUM_R2_REQUEST_TIMEOUT_MS: ${{ secrets.STRATUM_LIVE_R2_REQUEST_TIMEOUT_MS }}
STRATUM_R2_CONNECT_TIMEOUT_MS: ${{ secrets.STRATUM_LIVE_R2_CONNECT_TIMEOUT_MS }}
STRATUM_R2_MAX_ATTEMPTS: ${{ secrets.STRATUM_LIVE_R2_MAX_ATTEMPTS }}
STRATUM_R2_RETRY_BASE_DELAY_MS: ${{ secrets.STRATUM_LIVE_R2_RETRY_BASE_DELAY_MS }}
STRATUM_R2_RETRY_MAX_DELAY_MS: ${{ secrets.STRATUM_LIVE_R2_RETRY_MAX_DELAY_MS }}
```

**Step 5: Verify and commit**

Run:

```bash
bash -n scripts/check-postgres-migrations.sh scripts/check-r2-object-store.sh scripts/ci-live-postgres-gate.sh scripts/ci-live-r2-gate.sh
git diff --check
```

If a local workflow syntax tool is available, also run it against `.github/workflows/rust-ci.yml`; otherwise inspect the YAML indentation manually.

Commit:

```bash
git add .github/workflows/rust-ci.yml
git commit -m "ci: require live storage gates in protected contexts"
```

## Task 3: Document Live CI Contract

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update HTTP API guide operational docs**

Add a short `Live CI Gates` subsection near backend durability status documenting:

- PR/fork CI skips live gates and relies on existing local/unit gates.
- Protected branch, scheduled, and manual live jobs require the live secrets.
- Exact required GitHub secret names.
- Exact local commands for optional skip and required live execution.
- Live CI wrappers suppress raw failure logs to avoid DB URL, password, endpoint, bucket, key, object-key, and raw backend/provider-error leakage.
- Live failures block only the live contexts; existing non-live jobs are unchanged.

**Step 2: Update project status**

Add a completed slice entry for Live Gates In CI with:

- workflow/script files changed
- PR-safe skip behavior
- required live behavior
- current local verification results
- note that real live Postgres/R2 credentials were not available locally unless they are actually present during verification

**Step 3: Verify and commit**

Run:

```bash
git diff --check
```

Commit:

```bash
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: record live storage gate contract"
```

## Task 4: Reviews, Fixes, And Full Verification

**Files:**
- Inspect all changed files.

**Step 1: Spec/correctness review**

Ask a fresh reviewer to compare the implementation against this plan and the slice acceptance criteria:

- PR CI without secrets skips live gates cleanly and visibly.
- Protected/scheduled/manual contexts fail closed when secrets are missing.
- Required live Postgres runs migration smoke plus `STRATUM_POSTGRES_TEST_REQUIRED=1 cargo test --locked --features postgres backend::postgres --lib -- --nocapture`.
- Required live R2 runs the object-store round trip.
- CI summaries distinguish `skipped`, `passed live`, and `failed live`.
- Logs do not print credentials, endpoints, bucket names, object keys, or raw backend/provider errors.
- Existing non-live CI jobs are not weakened.

Fix all Critical/Important findings locally. Re-review non-trivial fixes.

**Step 2: Code-quality/security review**

Ask a fresh reviewer to review maintainability, shell safety, GitHub Actions semantics, secret masking, redaction coverage, and CI blast radius.

Fix all Critical/Important findings locally. Re-review non-trivial fixes.

**Step 3: Required verification**

Run:

```bash
cargo fmt --all -- --check
git diff --check
bash -n scripts/check-postgres-migrations.sh scripts/check-r2-object-store.sh scripts/ci-live-postgres-gate.sh scripts/ci-live-r2-gate.sh
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

Also run wrapper missing-secret checks:

```bash
STRATUM_LIVE_GATE_REQUIRED=0 STRATUM_POSTGRES_TEST_URL= ./scripts/ci-live-postgres-gate.sh
STRATUM_LIVE_GATE_REQUIRED=1 STRATUM_POSTGRES_TEST_URL= ./scripts/ci-live-postgres-gate.sh
STRATUM_LIVE_GATE_REQUIRED=0 STRATUM_R2_TEST_ENABLED= ./scripts/ci-live-r2-gate.sh
STRATUM_LIVE_GATE_REQUIRED=1 env -u STRATUM_R2_BUCKET ./scripts/ci-live-r2-gate.sh
```

Expected: optional wrapper runs skip cleanly; required missing-env runs fail with redacted fixed messages.

If live credentials are available, run the live commands exactly as CI runs them:

```bash
STRATUM_LIVE_GATE_REQUIRED=1 ./scripts/ci-live-postgres-gate.sh
STRATUM_LIVE_GATE_REQUIRED=1 ./scripts/ci-live-r2-gate.sh
```

**Step 4: Final commit if fixes were needed**

Commit any review or verification fixes with a focused message.

**Step 5: Push and merge**

After all gates pass:

```bash
git status --short --branch
git push origin v2/foundation
cd /Users/rajattiwari/virtualfilesystem/lattice
git status --short --branch
git fetch origin
git merge --ff-only v2/foundation
git push origin main
```

Do not remove or revert unrelated untracked files in the main worktree.
