# Persisted Guarded Commit Recovery Claims Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Persist guarded durable commit recovery work so post-CAS partials and recovery-required durable commit states are inspectable and safely claimable across process restarts.

**Architecture:** Extend the existing `DurableCorePostCasRecoveryClaimStore` contract from in-memory-only to a durable store with explicit pending, active lease, backing-off, completed, and poisoned states. Wire guarded `POST /vcs/commit` to enqueue redacted recovery work after visible post-CAS partials, expose an admin-only recovery status surface, and keep actual repair execution bounded to claim/finalize controls until a follow-up slice can reconstruct route envelopes without weakening idempotency or duplicating audit effects.

**Tech Stack:** Rust, Tokio, Axum, existing durable commit transaction types, Postgres feature-gated adapter tests, migration smoke SQL, in-memory store contracts, guarded durable commit route tests, and `/usr/bin/time -l` release perf.

---

## CTO Plan And Status Read

The CTO plan requires durable metadata plus object storage to replace the local full-state file while preserving correctness around immutable commits, CAS-visible refs, audit, and rollback. Current `project-status.md` says the guarded durable commit route is live only behind `STRATUM_DURABLE_COMMIT_ROUTE=1`, still sources filesystem state from local `StratumDb`, and lacks persisted recovery claims, a repair worker, and operator status.

`extract pieces.md` points to SMFS for claim/finalize/backoff/poison and worker wakeup shape. The extraction boundary is strict:

- Use SMFS claim/finalize/backoff/poison vocabulary and bounded worker wakeup ideas.
- Do not import SMFS latest-wins filepath queue behavior.
- Do not key recovery by mutable paths; key it by repo/ref/commit/step identity.
- Do not copy SMFS SQLite inode/chunk cache or sidecar poison-file UX.

## Scope

In scope:

- Add a persisted recovery claim table to the Postgres migration catalog.
- Extend the existing recovery claim trait with enqueue/list operations while preserving claim, complete, failure/backoff, and poison fencing.
- Update the in-memory implementation and tests to model `pending` before a worker claims work.
- Implement the Postgres adapter with token/lease fencing, retry/backoff, terminal completion, terminal poison, redacted diagnostics, and bounded owner/lease/backoff validation.
- Add the recovery claim store to `StratumStores`, local-memory stores, and guarded durable server store construction.
- Enqueue post-CAS recovery work from guarded durable commit partial outcomes after ref visibility is confirmed.
- Add an admin-only `GET /vcs/recovery` status endpoint for the guarded durable commit capability, returning redacted rows and queue counts.
- Add focused tests proving partial post-CAS outcomes enqueue recovery work without duplicate audit/workspace/idempotency effects.

Out of scope for this slice:

- No broad `STRATUM_CORE_RUNTIME=durable-cloud` route cutover.
- No durable auth/session path.
- No automatic repair worker that reconstructs a post-CAS envelope after restart.
- No new idempotency API to recreate reservations from durable pending rows.
- No audit replay worker; audit append remains non-idempotent.
- No pre-visibility committed-response replay. Pre-visibility uncertainty can be recorded for operators, but idempotency must not be completed as committed until ref visibility is proven.
- No final-object deletion or distributed lock service.

## Acceptance

- Visible commits with post-CAS partial completion enqueue exactly one durable recovery row keyed by repo, `main`, commit ID, and failed step.
- Repeated enqueue for the same target is idempotent and does not reset terminal complete/poisoned rows.
- A worker claim is fenced by lease token and expiry; stale tokens cannot complete, fail, or poison a retry.
- Failure records backoff with a redacted diagnostic and no raw commit message, path, idempotency token, R2 key, or Postgres detail.
- Operator status returns pending/active/backing_off/completed/poisoned counts and bounded redacted rows.
- Pre-visibility uncertain guarded-commit responses do not complete idempotency with committed replay.
- Performance remains in the current backend band.

## Task 1: Plan Commit

**Files:**

- Create: `docs/plans/2026-05-07-persisted-guarded-commit-recovery-claims.md`

**Steps:**

1. Save this plan.
2. Run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

3. Commit:

```bash
git add docs/plans/2026-05-07-persisted-guarded-commit-recovery-claims.md
git commit -m "docs: plan guarded commit recovery claims"
```

## Task 2: Add RED Contract Tests

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Steps:**

1. Extend `durable_core_commit_post_cas_recovery` tests to cover:
   - enqueued pending work is claimable;
   - duplicate enqueue of the same pending/backing-off/active target is idempotent;
   - completed and poisoned targets are terminal for enqueue and claim;
   - `list_recovery` returns redacted bounded statuses for pending, active, backing-off, complete, and poison.
2. Run:

```bash
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_recovery --lib -- --nocapture
```

Expected before implementation: tests fail because enqueue/list status APIs do not exist.

## Task 3: Extend In-Memory Recovery Contract

**Files:**

- Modify: `src/backend/core_transaction.rs`
- Modify: `src/backend/mod.rs`

**Implementation shape:**

- Add `Pending { attempts: u32 }` to the recovery entry state.
- Add `DurableCorePostCasRecoveryStatus` plus a small status enum.
- Add trait methods:
  - `enqueue(target, now_millis)`;
  - `list(limit)`.
- Add `SharedDurableCorePostCasRecoveryClaimStore` to `StratumStores`.
- Update `StratumStores::local_memory()`.

**Verification:**

```bash
cargo fmt --all -- --check
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_recovery --lib -- --nocapture
```

## Task 4: Add Postgres Schema And Adapter

**Files:**

- Create: `migrations/postgres/0003_guarded_commit_recovery_claims.sql`
- Modify: `src/backend/postgres_migrations.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `tests/postgres/0001_durable_backend_foundation_smoke.sql`

**Schema shape:**

- `guarded_commit_recovery_claims(repo_id, ref_name, commit_id, step, state, lease_owner, lease_token, lease_expires_at, attempts, retry_after, last_error, created_at, updated_at, completed_at, poisoned_at)`.
- Primary key: `(repo_id, ref_name, commit_id, step)`.
- Foreign key: `(repo_id, commit_id)` references `commits(repo_id, id)`.
- State check: `pending`, `active`, `backing_off`, `completed`, `poisoned`.
- Check constraints keep active rows leased, backing-off rows retryable, terminal rows terminal, and diagnostics redacted.

**Adapter shape:**

- `enqueue` inserts pending or no-ops for existing rows.
- `claim` atomically transitions pending/expired-active/elapsed-backoff rows to active with a new UUID token.
- `complete`, `record_failure`, and `poison` require matching token, owner, target, and unexpired lease.
- `list` returns redacted statuses ordered by most actionable rows first.

**Verification:**

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh
```

The live Postgres sections may skip when `STRATUM_POSTGRES_TEST_URL` is unset; do not claim live coverage if they skip.

## Task 5: Wire Guarded Route Enqueue And Status

**Files:**

- Modify: `src/server/core.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/server/routes_vcs.rs`

**Implementation shape:**

- Add recovery store access through `GuardedDurableCommitRoute`.
- On `DurableCorePostCasOutcome::Partial(partial)`, enqueue the failed post-CAS target after visible ref CAS.
- Leave the client response as the existing redacted `202 Accepted`.
- Add `GET /vcs/recovery` behind the same admin gate used by VCS/admin routes.
- If the guarded durable commit capability is absent, return a redacted fail-closed response.

**Verification:**

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
```

## Task 6: Full Gates And Review

Run:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo test --locked --features postgres backend::postgres::tests::postgres_metadata_store_round_trips_backend_contracts --lib -- --nocapture
cargo test --locked --features postgres --test server_startup durable_env -- --nocapture
cargo audit --deny warnings
git diff --check
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Then run spec/code-quality review, fix findings locally, rerun affected gates, commit, push `v2/foundation`, merge to `main`, verify main, and push `main`.
