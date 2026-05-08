# Pre-Visibility Recovery Run Control Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add bounded admin-triggered run control that can converge guarded durable commit pre-visibility recovery rows once commit/ref visibility is provable or safely release retries when it is not.

**Architecture:** Extend the pre-visibility ledger with optional route-bound recovery context and a post-CAS-style claim/backoff/poison state machine, then add a small classifier/runner that processes due pre-visibility rows. The runner must preserve commit/ref/idempotency identity: it may enqueue existing post-CAS repair only after it proves or safely applies `main` visibility with trusted route context, and it may abort the original idempotency reservation only when the commit is not visible and the persisted reservation context is available.

**Tech Stack:** Rust, Tokio, async-trait stores, Postgres migrations, existing guarded durable commit route, existing post-CAS repair worker, existing idempotency/audit/workspace stores.

---

### Task 1: Persist Route-Bound Pre-Visibility Context

**Files:**
- Modify: `src/backend/core_transaction.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `migrations/postgres/0006_pre_visibility_recovery_context.sql`
- Modify: `src/backend/postgres_migrations.rs`
- Modify: `tests/postgres/0001_durable_backend_foundation_smoke.sql`
- Modify: `src/server/routes_vcs.rs`

**Step 1: Write failing tests**

Add focused tests proving that:

- a pre-visibility row recorded by guarded `POST /vcs/commit` reports `has_recovery_context: true` in `GET /vcs/recovery`;
- raw commit message, idempotency key, reservation token, and private store error details are not exposed;
- duplicate pending rows can upgrade from no context to context without changing the diagnostic identity.

Run:

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_commit_metadata_recovery_failure_does_not_replay_partial --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_pre_visibility_recovery --lib -- --nocapture
```

Expected: fail because pre-visibility status does not carry context yet.

**Step 2: Implement context persistence**

Extend `DurableCorePreVisibilityRecoveryRecord` and `DurableCorePreVisibilityRecoveryStatus` with optional `DurableCorePostCasRecoveryContext`. Keep `Debug` redacted by relying on the existing post-CAS context redaction.

Add migration `0006_pre_visibility_recovery_run_control.sql`:

- add nullable `context_json JSONB`;
- add `lease_owner`, `lease_token`, `lease_expires_at`, `attempts`, `retry_after`, `last_error`, `poisoned_at`, and `updated_at`;
- expand `state` from `pending/resolved` to `pending/active/backing_off/resolved/poisoned`;
- add constraints equivalent to the post-CAS claim table: pending has no lease/error/terminal fields, active has lease fields, backing-off has redacted error plus retry time, resolved has `resolved_at`, poisoned has redacted error plus `poisoned_at`;
- add a due-work index for pending, expired-active, and due-backing-off rows.

Update the Postgres migration catalog and smoke harness.

Update in-memory and Postgres `record` upserts:

- preserve existing context if already present and valid;
- upgrade a pending row from no context to context;
- keep existing diagnostic matching semantics;
- do not expose context JSON through public status.

**Step 3: Wire guarded route context**

When guarded durable commit records a pre-visibility row, attach a `DurableCorePostCasRecoveryContext` containing:

- optional workspace id;
- expected workspace head from the pre-visibility parent commit id;
- a VCS commit audit event bound to the session and commit id;
- optional idempotency reservation context with `FullCommit` response kind.

The route must continue to return the same redacted recovery-required error.

**Step 4: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked backend::core_transaction::tests::durable_core_pre_visibility_recovery --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable_commit_metadata_recovery_failure_does_not_replay_partial --lib -- --nocapture
git diff --check
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Commit:

```bash
git add src/backend/core_transaction.rs src/backend/postgres.rs src/backend/postgres_migrations.rs src/server/routes_vcs.rs migrations/postgres/0006_pre_visibility_recovery_run_control.sql tests/postgres/0001_durable_backend_foundation_smoke.sql
git commit -m "feat: persist pre-visibility recovery context"
```

### Task 2: Add Bounded Pre-Visibility Recovery Runner

**Files:**
- Modify: `src/backend/core_transaction.rs`
- Modify: `src/backend/mod.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `src/server/routes_vcs.rs`

**Step 1: Write failing tests**

Add tests proving that bounded admin `POST /vcs/recovery/run`:

- resolves a `ref_visibility_cas` row after proving `main` points at the commit, then enqueues the first post-CAS recovery step with context;
- applies a safe ref CAS for a `commit_metadata_insert` row when commit metadata exists and `main` is still at the recorded expected parent/version;
- aborts the original idempotency reservation and resolves the row when the commit is not visible and cannot be safely made visible;
- leaves old no-context rows pending/skipped rather than fabricating audit/idempotency context;
- caps the limit at 100 and preserves post-CAS repair behavior in the same route.

Expected: fail because `POST /vcs/recovery/run` only runs post-CAS repair.

**Step 2: Implement store transitions**

Extend `DurableCorePreVisibilityRecoveryStore` with:

- `claim(request) -> Option<claim>`;
- `complete/resolve(claim, now_millis)`;
- `record_failure(claim, diagnosis, backoff, now_millis)`;
- `poison(claim, diagnosis, now_millis)`;
- `list_repair_candidates(now_millis, limit)`.

In-memory and Postgres implementations must fence by lease owner/token/expiry. Stale owners, stale tokens, expired claims, and terminal rows must not be finalizable. Diagnostics are redacted to a fixed marker.

**Step 3: Implement runner**

Add `DurableCorePreVisibilityRecoveryRun` with a redacted summary.

For each due claimed row:

- read commit metadata and current ref;
- verify commit metadata matches the recorded root, parent, and changed-path count before using it;
- treat `main == commit` or a bounded parent walk from current `main` to commit as proven visibility;
- if visibility is proven and trusted context exists, enqueue the first required post-CAS step with persisted context, then resolve the pre-visibility row;
- if commit metadata exists and current `main` is still the recorded expected parent/version, apply the guarded ref CAS, enqueue post-CAS repair, then resolve;
- if visibility is disproven and trusted idempotency context exists, abort the persisted idempotency reservation, then resolve;
- if context is missing for a row that needs audit/idempotency handoff, poison or back off instead of fabricating context;
- if stores are unavailable, record failure/backoff without resolving.

No latest-wins path coalescing, no background loop, and no broad durable `CoreDb` cutover.

**Step 4: Wire route**

Update `POST /vcs/recovery/run` to run the pre-visibility runner first, then the existing post-CAS repair worker. Return both summaries in the JSON response while preserving existing post-CAS fields.

**Step 5: Verify and commit**

Run focused tests:

```bash
cargo test --locked backend::core_transaction::tests::durable_core_pre_visibility_recovery --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Commit:

```bash
git add src/backend/core_transaction.rs src/backend/mod.rs src/backend/postgres.rs src/server/routes_vcs.rs
git commit -m "feat: run pre-visibility recovery control"
```

### Task 3: Review Fixes, Docs, and Integration

**Files:**
- Modify: `docs/project-status.md`
- Modify as needed after reviews.

**Step 1: Spec and quality review**

Run independent spec/correctness and code-quality/security reviews focused on:

- ref visibility proof and bounded ancestor walk;
- safe CAS application only from recorded parent/version;
- idempotency abort versus committed replay behavior;
- context redaction;
- Postgres concurrency/idempotency.

**Step 2: Fix findings locally**

Do not accept review output blindly. Inspect diffs, fix locally, and rerun focused tests.

**Step 3: Final gates**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
cargo test --locked
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Commit docs/status:

```bash
git add docs/project-status.md docs/plans/2026-05-08-pre-visibility-recovery-run-control.md
git commit -m "docs: record pre-visibility recovery run control"
```

Then push `v2/foundation`, merge to `main`, rerun main gates, run main warm perf, and push `main`.
