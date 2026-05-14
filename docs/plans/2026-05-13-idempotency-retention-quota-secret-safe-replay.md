# Idempotency Retention Quota And Secret Safe Replay Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Bound idempotency storage growth, make stale pending records recoverable, enforce deterministic redacted quotas, and prevent raw-secret replay persistence.

**Architecture:** Extend the existing Stratum idempotency contract instead of replacing it: reservations remain keyed by scope plus hashed idempotency key, and existing replay semantics stay compatible for FS, VCS, review, run, and workspace creation. Add retention policy, replay classification, quota identity, and bounded sweep/takeover APIs at the store layer, then wire route helpers to classify responses before persistence. Durable-cloud startup stays fail-closed unless explicit retention and quota settings are configured.

**Tech Stack:** Rust, Tokio, async-trait, Axum, existing `IdempotencyStore`, in-memory/local idempotency stores, Postgres `idempotency_records`, existing audit store, durable recovery/object-cleanup root scans, and server startup gates.

---

## Required Context

Read before implementation:

- `markdownfs_v2_cto_architecture_plan.md`
- `docs/project-status.md`
- `docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md`
- `docs/plans/2026-05-13-final-object-deletion-unreachable-object-gc.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`

Use `extract pieces.md` only for bounded worker/status ideas: pending/active/backing-off/poison vocabulary, stale-active age classification, bounded drain summaries, and redacted diagnostics. Do not import SMFS latest-wins queue semantics. Stratum idempotency remains keyed by repo/scope/principal/workspace/idempotency identity and request fingerprint.

Current facts to preserve:

- `IdempotencyStore::begin` returns `Execute`, `Replay`, `Conflict`, or `InProgress`.
- `complete` persists a completed replay only for the matching pending reservation token.
- `complete_or_match` supports recovery finishing an already-completed exact replay.
- Existing raw `Idempotency-Key` headers are never stored; only `key_hash` is persisted.
- Existing workspace-token issuance rejects idempotency keys because success returns raw `workspace_token`.
- GC currently treats pending idempotency records as repo blockers and extracts commit ids from retained completed replay JSON.

## Non-Negotiable Constraints

- No raw workspace token, bearer token, idempotency key, request secret, commit message, review body/comment/reason, backing path, raw SQL error, object key, or object-store error may be persisted through idempotency replay.
- Secret-bearing replay is rejected by default. Workspace-token issuance remains non-idempotent in this slice.
- Sweeps must not remove records that unresolved recovery or object GC still needs as roots.
- Quota failures must be deterministic, content-free, and audited when route context has an audit store and authenticated actor.
- Broad durable-cloud startup must fail before opening local `.vfs` or durable stores if retention/quota configuration is absent or invalid.
- Existing local singleton idempotency compatibility remains: old unqualified scopes replay locally; explicit hosted repo contexts use `repo:{repo_id}:...` scopes.

## Task 1: Core Idempotency Policy, Classification, And Local Store Semantics

**Files:**

- Modify: `src/idempotency.rs`

**Step 1: Write failing core tests**

Add tests for:

- completed records older than the completed TTL are swept, while unexpired records remain;
- pending records younger than stale TTL return `InProgress` or `Conflict`;
- pending records older than stale TTL support deterministic takeover for same fingerprint and deterministic abort for mismatched fingerprint;
- stale reservation tokens cannot complete or abort a later takeover reservation;
- per-scope and parsed repo/workspace quotas reject new reservations before inserting pending state;
- quota errors do not include raw keys, request fingerprints, response JSON, repo secrets, or request bodies;
- local persisted v1 records decode as secret-free completed records and get bounded migration timestamps;
- `ReplayClassification::SecretBearing` is rejected before persistence;
- `ReplayClassification::Partial` replays but is distinguishable in the retained projection/debug output;
- retained repo listing includes classification and timestamps without leaking response bodies.

Run:

```bash
cargo test --locked idempotency --lib -- --nocapture
```

Expected RED: tests fail because there is no policy, classification, quota, timestamp, or sweep/takeover API.

**Step 2: Add core types**

Add small focused types:

- `IdempotencyReplayClassification` with `SecretFree`, `Partial`, and `SecretBearing`.
- `IdempotencyRetentionPolicy` with `completed_ttl`, `pending_stale_after`, `max_records_per_scope`, optional parsed `max_records_per_repo`, optional parsed `max_records_per_workspace`, optional `max_records_per_principal`.
- `IdempotencyQuotaIdentity` with `scope`, optional `repo_id`, optional `workspace_id`, optional `principal_uid`.
- `IdempotencySweepRequest` with `now`, limit, policy, repo id, and explicit retained root/blocker input.
- `IdempotencySweepSummary` with scanned, swept_completed, aborted_pending, retained_for_roots, quota_blocked, stale_pending, remaining, and redacted reason counters.

Keep `Debug` implementations content-free.

**Step 3: Extend store records**

Add timestamps and classification to in-memory state:

- pending: `request_fingerprint`, `reservation_token`, `reserved_at_unix_seconds`, `quota_identity`.
- completed: `request_fingerprint`, `status_code`, `response_body`, `completed_at_unix_seconds`, `classification`, `quota_identity`.

Keep existing `begin`, `complete`, `complete_or_match`, and `abort` signatures for compatibility. Add policy-aware default methods rather than forcing every current caller to change in one step:

- `begin_with_policy(scope, key, request_fingerprint, quota_identity, policy)`.
- `complete_with_classification(reservation, status_code, body, classification)`.
- `complete_or_match_with_classification(...)`.
- `sweep_retention(request)`.

Plain `begin` should call policy-aware begin with an unlimited local policy. Plain `complete` should classify as `SecretFree` so existing tests continue to pass until route helpers are updated.

**Step 4: Implement stale pending behavior**

In policy-aware begin:

- if completed exists and fingerprint matches, return replay;
- if completed exists and fingerprint differs, return conflict;
- if pending exists and age is younger than `pending_stale_after`, preserve current behavior;
- if pending exists and age is stale and fingerprint matches, replace the reservation token and timestamp, then return `Execute` for takeover;
- if pending exists and age is stale and fingerprint differs, abort the stale row and return `Conflict` without inserting the new request;
- quota checks run only before creating a new pending row or takeover row.

**Step 5: Implement local persistence migration**

Bump the local idempotency store version. Decode v1 records as completed `SecretFree` records with conservative migration timestamps. Encode v2 records with timestamp and classification fields. Pending rows stay process-local for local file storage.

**Step 6: Implement bounded sweep**

Sweep must:

- scan a bounded number of rows;
- remove completed rows only after `completed_ttl`;
- remove stale pending rows only when explicitly allowed by request policy and not listed as retained roots;
- preserve rows whose replay body contains commit ids still required by recovery/GC root input;
- return redacted counters only.

**Step 7: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked idempotency --lib -- --nocapture
git diff --check
```

Commit:

```bash
git add src/idempotency.rs
git commit -m "feat: add idempotency retention policy"
```

## Task 2: Postgres Retention, Quota, And Migration Conformance

**Files:**

- Create: `migrations/postgres/0011_idempotency_retention_quota.sql`
- Modify: `src/backend/postgres.rs`
- Modify: `src/backend/postgres_migrations.rs`
- Modify: `tests/postgres/0001_durable_backend_foundation_smoke.sql`

**Step 1: Write failing Postgres tests**

Extend `run_idempotency_contracts` with live adapter tests for:

- replay classification persists and rejects `SecretBearing`;
- stale pending same-fingerprint takeover replaces the reservation token;
- stale pending different-fingerprint aborts stale state and returns deterministic conflict;
- completed TTL sweep deletes only expired safe rows;
- sweep preserves rows listed as recovery/GC roots;
- per-scope and parsed repo/workspace quota failures are deterministic;
- quota and sweep queries do not return raw key material;
- row locks/concurrent duplicate begin still produce one winner and one in-progress/conflict outcome.

Run:

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected RED: migration/adapter fields and APIs do not exist.

**Step 2: Add migration**

Add columns to `idempotency_records`:

- `replay_classification TEXT NOT NULL DEFAULT 'secret_free' CHECK (...)`
- `repo_id TEXT REFERENCES repos(id) ON DELETE CASCADE`
- `workspace_id UUID`
- `principal_uid INTEGER`
- `response_body_bytes BIGINT NOT NULL DEFAULT 0 CHECK (response_body_bytes >= 0)`
- `last_quota_checked_at TIMESTAMPTZ`

Add indexes for bounded policy work:

- `(repo_id, state, completed_at, reserved_at)`
- `(scope, state, completed_at, reserved_at)`
- `(workspace_id, state, completed_at, reserved_at)` where workspace is not null
- `(principal_uid, state, completed_at, reserved_at)` where principal is not null

Backfill existing completed rows as `secret_free`, `response_body_bytes = octet_length(response_body_json::text)`, and nullable identity columns.

**Step 3: Update readiness**

`PostgresMetadataStore::ensure_control_plane_ready` must select all new columns so drifted schemas fail before stores are opened.

**Step 4: Implement policy-aware Postgres methods**

Use a transaction for policy-aware `begin_with_policy`:

- lock or insert by `(scope, key_hash)`;
- classify existing row with stale-pending policy;
- enforce quotas with bounded `COUNT(*)` queries before insert/takeover;
- insert/takeover pending with explicit identity columns;
- return reservation token from the current row.

Use guarded updates for `complete_with_classification` and `complete_or_match_with_classification`, preserving `xmin::text` fencing. Reject `SecretBearing` before executing SQL.

Implement `sweep_retention` with bounded `DELETE ... WHERE ... RETURNING` inside a transaction. Do not sweep rows whose `(scope, key_hash)` or commit/root identity is included in the request blockers.

**Step 5: Migration smoke**

Update the smoke test to assert:

- new columns exist;
- replay classification accepts only known values;
- malformed quota/accounting fields fail;
- old rows can be backfilled.

**Step 6: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
git diff --check
```

Commit:

```bash
git add src/backend/postgres.rs src/backend/postgres_migrations.rs migrations/postgres/0011_idempotency_retention_quota.sql tests/postgres/0001_durable_backend_foundation_smoke.sql
git commit -m "feat: persist idempotency retention quotas"
```

## Task 3: Route-Level Secret-Safe Replay Classification And Quota Auditing

**Files:**

- Modify: `src/server/idempotency.rs`
- Modify: `src/server/routes_workspace.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/routes_review.rs`
- Modify: `src/server/routes_runs.rs`

**Step 1: Write failing route tests**

Add focused tests that prove:

- workspace-token issue and revoke still reject idempotency keys before mutation;
- a helper refuses to persist `SecretBearing` responses and returns a redacted non-replayable error;
- workspace create replays as `SecretFree`;
- VCS commit/revert replay bodies classify as `Partial` when commit message or recovery/audit partials are redacted;
- review mutations classify as `Partial` when text fields are nulled;
- FS and run-create responses classify as `SecretFree` or `Partial` without content leaks;
- quota failures return stable JSON, do not mutate, and append a content-free audit event when the request already has an authenticated actor;
- existing same-key replay and same-key/different-fingerprint conflict tests continue to pass.

Run focused suites:

```bash
cargo test --locked server::idempotency --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked server::routes_fs::tests --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture
cargo test --locked server::routes_review::tests --lib -- --nocapture
cargo test --locked server::routes_runs::tests --lib -- --nocapture
```

Expected RED: route helpers persist unclassified responses and no quota error/audit path exists.

**Step 2: Centralize HTTP helper policy**

In `src/server/idempotency.rs`, add:

- classified completion helpers that call `complete_with_classification`;
- replay rejection/error helpers for non-replayable secret-bearing responses;
- deterministic quota error response helper, for example `{"error":"idempotency quota exceeded","quota":"scope"}` with no raw scope/key/body;
- a small classification builder for `SecretFree`, `Partial`, and `SecretBearing`.

Do not classify at replay time only. Classification must happen before persistence.

**Step 3: Wire route completions**

Update route-specific completion helpers:

- workspace create: `SecretFree`.
- workspace token issue/revoke: continue rejecting idempotency keys; no replay storage.
- FS write/mkdir/delete/copy/move/metadata: `SecretFree` unless response is a post-visible recovery/audit partial, then `Partial`.
- VCS ref create/update: `SecretFree`.
- VCS commit/revert: `Partial` for redacted commit-message/recovery/audit partial bodies, otherwise `SecretFree`.
- Review mutations: `Partial` after body/comment/reason/description sanitization, otherwise `SecretFree`.
- Run create: `SecretFree` for normal run id/path response, `Partial` for audit-failure/partial mutation response.

**Step 4: Audit quota failures**

Use existing audit types if possible. If a new audit action is needed, add the smallest content-free variant and tests. Details should include route family, quota kind, repo/workspace/principal presence, and no raw keys or request bodies.

**Step 5: Verify and commit**

Run the focused route commands above plus:

```bash
cargo fmt --all -- --check
git diff --check
```

Commit:

```bash
git add src/server/idempotency.rs src/server/routes_workspace.rs src/server/routes_fs.rs src/server/routes_vcs.rs src/server/routes_review.rs src/server/routes_runs.rs src/audit.rs
git commit -m "feat: classify idempotency replay responses"
```

## Task 4: Recovery-Safe Sweep And Durable Startup Gates

**Files:**

- Modify: `src/backend/object_cleanup.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/backend/runtime.rs`
- Modify: `tests/server_startup.rs`

**Step 1: Write failing tests**

Add tests for:

- object GC/reachability retains idempotency rows that are pending or whose completed replay body contains live recovery/GC root commit ids;
- sweep deletes expired completed rows only after recovery/root blockers are absent;
- sweep aborts stale pending rows only when policy allows and reports redacted counters;
- durable-cloud startup rejects missing completed retention TTL;
- durable-cloud startup rejects missing pending stale TTL;
- durable-cloud startup rejects missing per-scope quota;
- durable-cloud startup accepts configured retention/quota gates and does not create local `.vfs` files on failures.

Run:

```bash
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
```

Expected RED: no sweep/run integration or startup gate exists.

**Step 2: Add startup config**

Add env vars in `src/backend/runtime.rs`:

- `STRATUM_IDEMPOTENCY_COMPLETED_RETENTION_SECONDS`
- `STRATUM_IDEMPOTENCY_PENDING_STALE_SECONDS`
- `STRATUM_IDEMPOTENCY_MAX_RECORDS_PER_SCOPE`
- optional bounded repo/workspace/principal quota envs if implemented in Task 1/2.

Parse positive bounded integers. Durable-cloud readiness requires completed TTL, pending stale TTL, and at least per-scope quota. Do not print secret-bearing env values; these are numeric and safe to name.

**Step 3: Attach policy to stores**

When durable stores are opened, wrap or configure the idempotency store with the parsed policy. Local server mode can use a conservative default that preserves current behavior unless a policy is explicitly passed by tests.

**Step 4: Recovery/root-safe sweep**

Add a bounded helper that builds `IdempotencySweepRequest` from the existing recovery/object-cleanup root scanner:

- unresolved recovery rows and active cleanup claims are blockers;
- completed replay commit ids referenced by reachable refs/workspaces/recovery are blockers;
- pending repo-scoped rows block deletion unless explicitly stale-aborted by policy;
- summaries stay redacted.

Do not run an unbounded background loop in request handlers. If connected to the existing recovery scheduler, use remaining phase limit and report attempted/completed/backing-off/poisoned/remaining style counters.

**Step 5: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
git diff --check
```

Commit:

```bash
git add src/backend/object_cleanup.rs src/server/mod.rs src/backend/runtime.rs tests/server_startup.rs
git commit -m "feat: gate durable startup on idempotency policy"
```

## Task 5: Documentation And Status

**Files:**

- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`
- Modify: `docs/plans/2026-05-13-idempotency-retention-quota-secret-safe-replay.md`

**Step 1: Update docs**

Document:

- retention TTL env vars and durable-cloud startup gate behavior;
- stale pending takeover/abort semantics;
- quota dimensions actually enforced in this slice;
- replay classification model and route classifications;
- workspace-token issuance remains non-idempotent because raw token replay is not stored;
- recovery/GC-safe sweep behavior and what remains future work.

**Step 2: Verify docs**

Run:

```bash
git diff --check
```

Commit:

```bash
git add docs/http-api-guide.md docs/project-status.md docs/plans/2026-05-13-idempotency-retention-quota-secret-safe-replay.md
git commit -m "docs: record idempotency retention policy"
```

## Task 6: Review, Fixes, And Final Verification

**Step 1: Spec/correctness review**

Dispatch a review agent with the full acceptance criteria:

- Expired completed records are swept only when safe.
- Pending stale records have explicit takeover or abort behavior.
- Quota failures are redacted, deterministic, and audited.
- Records required by unresolved recovery or GC reachability roots are not swept.
- Replay of secret-bearing responses is rejected or safely classified with no raw secret persistence.
- Existing FS/VCS/review/run/workspace idempotency behavior remains compatible.
- Broad durable startup fails closed unless retention/quota configuration is present.

Fix all critical/important findings locally or send scoped fixes to workers.

**Step 2: Code-quality/security review**

Dispatch a second review agent focused on:

- race/concurrency behavior in Postgres begin/takeover/sweep;
- `xmin` reservation-token fragility after migrations/backfills;
- raw key/token/body leakage in errors, debug, audit, docs, and tests;
- quota count atomicity;
- local-store migration and corruption handling;
- route compatibility and authorization-before-replay behavior.

Fix all critical/important findings locally or send scoped fixes to workers.

**Step 3: Required verification**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked idempotency --lib -- --nocapture
cargo test --locked server::idempotency --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked server::routes_fs::tests --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_idempotency --lib -- --nocapture
cargo test --locked server::routes_review::tests --lib -- --nocapture
cargo test --locked server::routes_runs::tests --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

If a focused test path does not exist, replace it with the nearest route module filter and record the substitution in `docs/project-status.md`.

**Step 4: Final integration**

After all verification passes:

```bash
git status --short --branch
git push origin v2/foundation
```

Then merge to main only after rechecking the main worktree status and preserving unrelated untracked files:

```bash
cd /Users/rajattiwari/virtualfilesystem/lattice
git status --short --branch
git pull --ff-only origin main
git merge --no-ff v2/foundation
cargo fmt --all -- --check
git diff --check
cargo test --locked --lib --tests
git push origin main
```

## Implementation Status: 2026-05-14

Implemented on `v2/foundation` through the following slice commits:

- `370d577` - `docs: plan idempotency retention policy`
- `456a15d` - `feat: add idempotency retention core`
- `d75583c` - `feat: add postgres idempotency retention policy`
- `17cc028` - `feat: classify idempotency replay responses`
- `2b147e4` - `feat: gate durable startup on idempotency policy`

Current behavior:

- Policy-aware begin/complete/sweep support retention TTLs, stale same-fingerprint pending takeover, stale different-fingerprint abort, and scope/repo/workspace/principal quota identity where route context supplies it.
- Replay storage is classified as `SecretFree`, `Partial`, or `SecretBearing`; `SecretBearing` completion is rejected before persistence.
- Workspace-token issuance and revoke stay non-idempotent because this slice does not add KMS/encrypted raw-token replay.
- Quota failures return deterministic redacted `429` JSON and emit metadata-only audit events when an audit store is available.
- Recovery/GC-safe sweep construction retains or blocks records needed by unresolved recovery, active cleanup claims, reachable refs/workspaces/reviews, and live commit roots; hidden off-page blockers and count errors fail closed.
- The bounded sweep helper is not scheduled automatically yet. Store-level sweeps and `sweep_idempotency_retention_for_repo` are foundations for a later operator/scheduler slice.

Review fixes already folded into the implementation:

- Completed sweeps now account for saturated pending scans and hidden pending rows instead of assuming the visible bounded page is complete.
- Terminal recovery rows and completed cleanup history no longer retain completed idempotency rows forever.
- The pending blocker check moved into the store sweep transaction/lock.
- `PolicyIdempotencyStore::begin_with_policy` now delegates with the configured durable policy.
- Quota 429 responses are audited with `IdempotencyQuotaExceeded`, and audit failure is surfaced only through `audit_recorded: false`.
- Hidden unresolved/active recovery and cleanup counts block sweeping even when rows are outside the bounded visible page.
- Full-page count errors fail closed.
- Review idempotency sanitization now redacts change-request titles and dismissal reasons before partial replay storage.

Focused verification already run during implementation:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked idempotency --lib -- --nocapture
cargo test --locked server::idempotency --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked server::routes_fs::tests --lib -- --nocapture
cargo test --locked server::routes_vcs::tests --lib -- --nocapture
cargo test --locked server::routes_review::tests --lib -- --nocapture
cargo test --locked server::routes_runs::tests --lib -- --nocapture
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked server::tests --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
cargo check --locked --features postgres
```
