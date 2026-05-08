# Guarded Pre-Visibility Recovery Ledger Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Persist operator-visible guarded durable commit states where the route cannot prove commit/ref visibility, without converting uncertainty into committed idempotency replay.

**Architecture:** Add a narrow pre-visibility recovery ledger beside the existing post-CAS repair claims. The ledger records redacted diagnostics for deterministic commit attempts whose metadata insert or ref CAS visibility is unconfirmed. It is inspectable through the existing admin recovery status route, but it is not consumed by `POST /vcs/recovery/run` and does not complete workspace, audit, or idempotency side effects.

**Tech Stack:** Rust, Tokio, Axum, existing durable commit transaction types, in-memory store contracts, Postgres migration/adapters behind the `postgres` feature, route tests, and release perf checks.

---

## Required Skills

Implementation and review subagents must use:

- `/Users/rajattiwari/.agents/skills/pragmatic-rust-guidelines/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-best-practices/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-async-patterns/SKILL.md`

Use TDD for behavior changes: write the failing test first, run it, then implement.

## Grounding Read

The CTO plan in the main checkout keeps durable commit/ref/object integrity, Postgres metadata, CAS ref updates, idempotency, and audit as core reliability requirements. Current `docs/project-status.md` says post-CAS gaps can be persisted, inspected, claimed, and repaired, but pre-visibility recovery-required states are not yet persisted as committed replay.

`extract pieces.md` is useful for status vocabulary and operator observability: pending/inflight/done/failed/poisoned shapes, bounded status responses, and recovery dashboards. The same file warns not to copy SMFS latest-wins queues into Stratum. For this slice, recovery records stay keyed by repo/ref/commit/stage identity and preserve immutable commit semantics.

Code inspection shows the gap in `src/server/routes_vcs.rs`: metadata insert recovery failure and ref visibility recovery failure both return the redacted "durable commit visibility recovery is required" response, but no durable row is created. The existing post-CAS table cannot safely represent this because its invariant is "commit is visible; completion side effects failed."

## Scope

In scope:

- Add a `DurableCorePreVisibilityRecoveryStore` contract in the backend layer.
- Add in-memory and Postgres-backed implementations.
- Add a Postgres migration for a separate pre-visibility ledger table without a commit FK, because metadata visibility may be unproven.
- Record guarded durable commit diagnostics for:
  - commit metadata insert recovery uncertainty;
  - ref CAS visibility recovery uncertainty.
- Include only redacted, bounded fields: repo id, ref name, deterministic commit id, root tree id, parent commit id when known, stage, state, object count, changed-path count, presence of idempotency reservation, first/last seen timestamps, and occurrence count.
- Extend `GET /vcs/recovery` with pre-visibility rows and counts while preserving the existing post-CAS response fields.
- Keep idempotency reservations pending/in-progress for unconfirmed visibility, matching the current safety behavior.
- Update project status after implementation and verification.

Out of scope:

- No automatic repair of pre-visibility rows.
- No idempotency completion from pre-visibility rows.
- No audit append, workspace-head update, or committed `200`/partial `202` replay unless ref visibility is already proven and the existing post-CAS path owns it.
- No broad durable filesystem/VCS serving cutover.
- No durable auth/session source cutover.
- No background scheduler or startup drain loop.
- No path/message/author/object-byte/idempotency-key/reservation-token leakage in status or debug output.

## Design Constraints

- Pre-visibility rows are diagnostic until a later slice adds an explicit proof/transition path.
- The post-CAS repair worker must ignore pre-visibility rows.
- The ledger must be idempotent for repeated reports of the same `(repo_id, ref_name, commit_id, stage)` and update `last_seen`/occurrence count rather than creating duplicates.
- Metadata-insert uncertainty may know the deterministic commit id without being able to prove the commit row exists, so the table must not require a commit FK.
- Ref-CAS uncertainty may have already made the commit visible, but the route must not assume that after a failed recovery read.
- Status output must be bounded and redacted. Raw storage errors and secrets stay out of HTTP responses and `Debug`.
- Existing successful guarded durable commit, post-CAS partial, recovery-run, and confirmed no-visibility abort semantics must not regress.

## Acceptance

- A metadata-insert recovery failure persists one pre-visibility row, returns the existing redacted 500, leaves the idempotency key in-progress, does not append audit, and does not enqueue post-CAS repair.
- A ref-CAS recovery failure persists one pre-visibility row, returns the existing redacted 500, leaves idempotency in-progress, and does not enqueue post-CAS repair.
- A confirmed no-visibility ref failure can still abort idempotency and must not create a pre-visibility row.
- `GET /vcs/recovery` remains admin-only, bounded at 100 rows per section, and includes both existing post-CAS fields and new pre-visibility fields.
- `POST /vcs/recovery/run` behavior is unchanged and only processes post-CAS repair rows.
- Postgres migration catalog and adapter tests cover insert/list/count/upsert behavior with redacted diagnostics.
- Release perf stays in the current warm band.

## Test Plan

Run focused tests first:

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_commit_metadata_recovery_failure_does_not_replay_partial --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable_commit_ref_visibility_recovery_failure_records_pre_visibility_status --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_pre_visibility_recovery --lib -- --nocapture
```

Then run Postgres-feature adapter/migration checks:

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
```

Final gates:

```bash
cargo fmt --all -- --check
git diff --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked
cargo test --locked --features postgres backend --lib -- --nocapture
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Do not claim live Postgres coverage when `STRATUM_POSTGRES_TEST_URL` is unset.
