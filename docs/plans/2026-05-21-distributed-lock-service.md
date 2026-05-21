# Distributed Lock Service Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add the narrow distributed-lock foundation needed for multi-node durable-cloud without replacing existing CAS, source-check, lease, fence, or idempotency guarantees.

**Architecture:** Treat Slice 8 as a lock taxonomy plus a Postgres transaction-advisory-lock abstraction for the few Postgres critical sections that already require serialization. Keep runtime VCS, recovery, cleanup, and scheduler correctness rooted in compare-and-swap, source checks, persisted lease tokens, metadata fences, and idempotent completion. Do not add Redis or a broad route/runtime mutex.

**Tech Stack:** Rust 2024, Tokio, `tokio-postgres`, `deadpool-postgres`, existing Postgres metadata adapters, existing durable recovery stores, existing object-cleanup metadata fences, Axum route tests, Bash verification scripts, and existing local/optional live provider gates.

---

## Required Skills And Agent Discipline

Implementation subagents must use:

- `superpowers:test-driven-development`
- `pragmatic-rust-guidelines`
- `rust-best-practices`

Review subagents must compare against this plan, the user acceptance criteria, and the redaction constraints in `docs/http-api-guide.md`.

The main session owns integration, local review, verification, commits, merges, and pushes. Subagents may implement scoped tasks, but the main session must inspect diffs and rerun gates before accepting work.

## Required Context Read

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/docs/plans/2026-05-15-backend-roadmap.md`
- `docs/project-status.md`
- `docs/http-api-guide.md`
- `docs/plans/2026-05-20-operator-destructive-cleanup-controls.md`
- `docs/plans/2026-05-20-durable-cloud-default-gate-flip.md`
- `docs/plans/2026-05-20-pre-cutover-load-and-chaos-suite.md`
- `docs/plans/2026-05-19-recovery-scheduler-productionization.md`
- `docs/plans/2026-05-16-real-postgres-pool-secret-seam-migration-adoption.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`

## Current Baseline

- No reusable distributed lock service exists.
- Runtime safety already relies on durable ref CAS, source-checked ref updates, persisted recovery claim owner/token/expiry leases, metadata deletion fences, idempotent completion, and store-level uniqueness constraints.
- Postgres migration apply/adopt has a schema-scoped advisory lock, but it is a bespoke helper.
- Postgres runtime code also uses bespoke advisory locks for object deletion fence serialization, idempotency quota/sweep serialization, and audit sequence allocation.
- Recovery scheduler intentionally does not use a distributed lock; multiple workers race through persisted recovery claims and complete idempotently.
- Single-node durable-cloud must continue to work without new lock configuration.

## Non-Goals

- Do not add Redis.
- Do not add a public lock service to `StratumStores`.
- Do not add a global durable-cloud route lock, commit lock, recovery scheduler lock, or object cleanup lock.
- Do not weaken ref CAS/source-check conflicts by serializing commits or ref updates.
- Do not enable broad unreachable commit/object deletion.
- Do not change durable-cloud unsupported `501` route behavior.
- Do not require local live Postgres/R2 credentials for provider-free gates.

## Durable Critical Section Taxonomy

| Critical section | Files | Classification | Slice 8 action |
|---|---|---|---|
| Immutable object byte put/get/delete proof | `src/backend/mod.rs`, `src/backend/blob_object.rs`, `src/remote/blob.rs` | no-lock-needed, store uniqueness | No distributed lock. Keep content-addressed identity and conditional final-object writes. |
| Object metadata insert | `src/backend/blob_object.rs`, `src/backend/postgres.rs` | no-lock-needed plus fence-aware | Keep uniqueness and active final-object metadata fence rejection. |
| Final-object metadata fence acquire/validate/delete | `src/backend/blob_object.rs`, `src/backend/postgres.rs`, `migrations/postgres/0010_object_deletion_fences.sql` | lease/fence-token protected, Postgres key serializer lock-required | Route the existing Postgres advisory key lock through the new helper; keep fence tokens as the correctness boundary. |
| Object cleanup claims | `src/backend/object_cleanup.rs`, `src/backend/postgres.rs` | lease/fence-token protected | No broad lock. Existing owner/token/expiry gates complete/fail/release/delete phases. |
| Destructive CAS-lost final-object cleanup | `src/backend/object_cleanup.rs`, `src/server/routes_vcs.rs` | lease/fence-token protected | No broad lock. Keep readiness, hold, repeated reachability, metadata fence, byte proof, and metadata proof. |
| Broad unreachable commit/object GC | `src/backend/object_cleanup.rs`, `src/server/routes_vcs.rs` | no-lock-needed | Remains dry-run/protocol-visible only. |
| Commit metadata insert | `src/backend/mod.rs`, `src/backend/postgres.rs`, `src/backend/core_transaction.rs` | no-lock-needed, idempotent uniqueness | No lock. Duplicate same commit returns existing; conflicting content fails. |
| Ref create/update | `src/backend/mod.rs`, `src/backend/postgres.rs`, `src/server/routes_vcs.rs` | CAS-fenced | No lock. Preserve observable stale-CAS conflicts. |
| Source-checked ref update / review merge | `src/backend/mod.rs`, `src/backend/postgres.rs`, `src/server/routes_review.rs`, `src/server/routes_vcs.rs` | source-checked plus CAS-fenced | No lock. Keep atomic source row check and target CAS in one transaction. |
| Guarded durable commit/revert visibility | `src/backend/core_transaction.rs`, `src/server/routes_vcs.rs` | CAS-fenced with recovery | No lock. Keep pre-visibility and post-CAS recovery ledgers. |
| Durable FS session materialization | `src/backend/durable_mutation.rs`, `src/server/routes_fs.rs` | source-checked | No lock. Creating a durable session ref requires the base ref to still match the observed source target/version and the session ref to be absent. |
| Durable FS session mutation visibility | `src/backend/durable_mutation.rs`, `src/server/routes_fs.rs` | CAS-fenced with recovery | No lock. New session commits become visible only through session-ref CAS; CAS losers create cleanup candidates and recovery rows. |
| Pre-visibility recovery ledger | `src/backend/core_transaction.rs`, `src/backend/postgres.rs` | lease/fence-token protected plus ref CAS/source proof | No lock. Existing owner/token/expiry gates resolve/fail/poison. |
| Post-CAS recovery claims | `src/backend/core_transaction.rs`, `src/backend/postgres.rs` | lease/fence-token protected | No lock. Existing owner/token/expiry gates complete/fail/poison. |
| Durable FS mutation recovery ledger | `src/backend/core_transaction.rs`, `src/backend/postgres.rs` | lease/fence-token protected | No lock. Existing operation/ref/commit identity and lease fencing stay primary. |
| Recovery scheduler tick/drain | `src/server/mod.rs` | no-lock-needed | No distributed scheduler lock. Process-local tick mutex prevents same-process overlap; persisted claims handle multi-node races. |
| Manual `POST /vcs/recovery/run` | `src/server/routes_vcs.rs` | lease/fence-token protected | No route lock. Bounded run uses the same persisted claims. |
| Idempotency begin/complete | `src/idempotency.rs`, `src/backend/postgres.rs` | reservation-token protected | Keep per-record token fencing and row locks. |
| Idempotency quota enforcement | `src/backend/postgres.rs`, `migrations/postgres/0011_idempotency_retention_quota.sql` | lock-required | Replace bespoke global advisory lock with helper. |
| Idempotency retention sweep | `src/idempotency.rs`, `src/backend/postgres.rs` | lock-required plus reservation-token protected | Replace bespoke sweep advisory lock with helper; keep bounded retention and blocker roots. |
| Audit append global sequence allocation | `src/audit.rs`, `src/backend/postgres.rs` | lock-required | Replace bespoke audit sequence advisory lock with helper. Future slice may replace this with a DB sequence. |
| Workspace create/read | `src/workspace/mod.rs`, `src/backend/postgres.rs`, `src/server/routes_workspace.rs` | no-lock-needed, store uniqueness | No lock. UUID identity and table constraints are sufficient. |
| Workspace head update during recovery | `src/workspace/mod.rs`, `src/backend/postgres.rs`, `src/backend/core_transaction.rs` | source-checked | No lock. Keep `head_commit IS NOT DISTINCT FROM expected`. |
| Workspace token issue/revoke/validate | `src/workspace/mod.rs`, `src/backend/postgres.rs`, `src/server/routes_workspace.rs` | row-transaction protected and idempotency protected where routed | No distributed lock. Token rows and route idempotency handle duplicates. |
| Review rule/change/approval/comment mutations | `src/review.rs`, `src/backend/postgres.rs`, `src/server/routes_review.rs` | row-transaction protected/source-checked | No advisory lock. Keep deterministic `FOR UPDATE` order and unique constraints. |
| Review merge target ref update | `src/server/core.rs`, `src/server/routes_review.rs`, `src/backend/postgres.rs` | source-checked plus CAS-fenced | No lock. Merge validates source ref freshness and target ref expectation before advancing target. |
| VCS ref create/update routes | `src/server/core.rs`, `src/server/routes_vcs.rs`, `src/backend/postgres.rs` | CAS-fenced plus route idempotency | No lock. Route idempotency protects replay; durable ref store still enforces `MustNotExist` or expected target/version. |
| VCS read routes/status/diff/log | `src/server/core.rs`, `src/server/routes_vcs.rs` | no-lock-needed | No lock. Durable log, status, and diff paths read committed refs/commits/objects and do not mutate shared state. |
| Local workspace head update from legacy VCS route | `src/server/routes_vcs.rs`, `src/workspace/mod.rs`, `src/backend/postgres.rs` | no-lock-needed outside durable-cloud | No distributed lock. This route path is skipped when guarded durable mutation routing is active; guarded durable workspace head updates stay source-checked. |
| Postgres migration apply/adopt | `src/backend/postgres_migrations.rs` | lock-required | Move schema-scoped advisory try-lock onto the helper and keep fail-closed contention. |
| Durable startup/readiness | `src/backend/runtime.rs`, `src/server/mod.rs`, `tests/server_startup.rs` | no-lock-needed plus migration lock | Do not add startup-wide runtime lock beyond migration apply/adopt. |

## Lock Design Decision

Use Postgres transaction-scoped advisory locks only.

Redis is not justified for this slice because Postgres is already the metadata consistency authority, transaction-scoped advisory locks compose with the existing SQL critical sections, and Redis would add TTL/fencing/split-brain posture without protecting R2/S3 bytes or replacing CAS semantics.

Do not use session-scoped advisory locks with pooled clients. A leaked session lock could survive pool checkout/checkin. Transaction-scoped locks release on commit/rollback and satisfy the current lease model.

## Task 1: Save And Commit This Plan

**Files:**

- Create: `docs/plans/2026-05-21-distributed-lock-service.md`

**Step 1: Verify docs-only diff**

Run:

```bash
git diff -- docs/plans/2026-05-21-distributed-lock-service.md
git diff --check
```

Expected: only this plan doc is changed and whitespace is clean.

**Step 2: Commit**

Run:

```bash
git add docs/plans/2026-05-21-distributed-lock-service.md
git commit -m "docs: plan distributed lock service"
```

## Task 2: Add Critical-Section Taxonomy Tests

**Files:**

- Modify: `src/backend/mod.rs`

**Step 1: Write failing taxonomy tests**

Add a focused test module table under `#[cfg(test)]` with a test named:

- `slice8_durable_critical_section_taxonomy_covers_current_runtime_sections`

The test must list the sections in the taxonomy above and assert:

- every classification enum is used;
- `ref update`, `source checked ref update`, `durable FS session materialization`, `durable FS session mutation visibility`, `review merge target ref update`, `VCS read routes/status/diff/log`, `local workspace head update from legacy VCS route`, `recovery scheduler`, `post-CAS recovery`, `pre-visibility recovery`, `durable FS mutation recovery`, `object cleanup claim`, and `destructive final object cleanup` are not `LockRequired`;
- the only `LockRequired` entries are `postgres migration apply/adopt`, `object deletion fence key serializer`, `idempotency quota`, `idempotency retention sweep`, and `audit global sequence`.

Expected RED:

```bash
cargo test --locked backend::tests::slice8_durable_critical_section_taxonomy_covers_current_runtime_sections --lib -- --exact --nocapture
```

The test should fail before the table exists.

**Step 2: Implement the test-only taxonomy table**

Keep this private to tests; do not add production API just to expose documentation. Use a small enum such as:

```rust
enum CriticalSectionClassification {
    CasFenced,
    SourceChecked,
    LeaseFenceTokenProtected,
    LockRequired,
    NoLockNeeded,
    RowTransactionProtected,
}
```

**Step 3: Verify and commit**

Run:

```bash
cargo test --locked backend::tests::slice8_durable_critical_section_taxonomy_covers_current_runtime_sections --lib -- --exact --nocapture
git diff --check
```

Commit:

```bash
git add src/backend/mod.rs
git commit -m "test: record durable critical section lock taxonomy"
```

## Task 3: Add Postgres Advisory Transaction Lock Helper

**Files:**

- Modify: `src/backend/postgres.rs`

**Step 1: Write failing helper tests**

Under the existing Postgres tests, add optional live/local-Postgres tests that skip when `STRATUM_POSTGRES_TEST_URL` is unset:

- `postgres_advisory_xact_lock_contends_across_independent_handles`
- `postgres_advisory_xact_lock_releases_on_transaction_end`
- `postgres_advisory_xact_lock_distinct_keys_do_not_conflict`
- `postgres_advisory_xact_lock_failure_messages_are_redacted`

The contention test must use two independent clients or independent `PostgresMetadataStore` handles, not one in-memory handle.

Run:

```bash
cargo test --locked --features postgres backend::postgres::tests::postgres_advisory_xact_lock --lib -- --nocapture
```

Expected RED: tests fail because the helper does not exist.

**Step 2: Implement the helper**

Near the existing Postgres advisory helpers, add internal types/functions:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PostgresAdvisoryXactLockKey {
    namespace: i32,
    key: i32,
}

impl PostgresAdvisoryXactLockKey {
    pub(crate) const fn new(namespace: i32, key: i32) -> Self;
    pub(crate) fn from_subject(namespace: i32, subject: &str) -> Self;
}

pub(crate) async fn postgres_try_advisory_xact_lock<C>(
    client: &C,
    key: PostgresAdvisoryXactLockKey,
    context: &'static str,
) -> Result<bool, VfsError>
where
    C: GenericClient + Sync;

pub(crate) async fn postgres_advisory_xact_lock<C>(
    client: &C,
    key: PostgresAdvisoryXactLockKey,
    context: &'static str,
) -> Result<(), VfsError>
where
    C: GenericClient + Sync;
```

Implementation rules:

- Use `pg_try_advisory_xact_lock($1, $2)` for the try helper.
- Use `pg_advisory_xact_lock($1, $2)` only where existing behavior already waits; pooled clients already set `statement_timeout`, so acquisition is bounded by the Postgres operation timeout and maps to the fixed timeout error.
- Derive subject keys with SHA-256 bytes, not `hashtext`, so keys are stable across Postgres versions.
- Do not expose the subject, SQL, namespace, key id, DB URL, schema, or raw backend error in public errors.
- The helper is `pub(crate)`, not public API.

**Step 3: Verify and commit**

Run:

```bash
cargo test --locked --features postgres backend::postgres::tests::postgres_advisory_xact_lock --lib -- --nocapture
git diff --check
```

Commit:

```bash
git add src/backend/postgres.rs
git commit -m "feat: add postgres advisory transaction lock helper"
```

## Task 4: Route Lock-Required Postgres Sections Through The Helper

**Files:**

- Modify: `src/backend/postgres.rs`
- Modify: `src/backend/postgres_migrations.rs`

**Step 1: Write failing regression tests**

Add or strengthen tests proving:

- migration apply/adopt still refuses a held schema lock with a fixed redacted conflict;
- object deletion fence key contention is serialized across independent Postgres handles and releases on transaction end;
- idempotency quota begin/sweep lock contention is bounded/fail-closed and does not leak scope, key hash, reservation token, SQL, DB URL, schema, or lock id;
- audit append global sequence remains unique under concurrent appends from independent Postgres handles.

Use existing optional Postgres test harnesses and skip cleanly when `STRATUM_POSTGRES_TEST_URL` is unset.

Run:

```bash
cargo test --locked --features postgres backend::postgres::tests::run_backend_contracts --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations::tests::held_schema_advisory_lock_refuses_apply --lib -- --exact --nocapture
```

Expected before implementation: at least the new helper-specific assertions fail.

**Step 2: Replace bespoke locks**

Replace existing raw advisory-lock SQL calls with the helper in:

- `lock_object_deletion_fence_key`
- idempotency `begin_with_policy`
- idempotency `sweep_retention`
- audit `append`
- migration `acquire_migration_lock`

Keep semantics unchanged:

- migration uses try-lock and returns `ObjectWriteConflict` if already held;
- object deletion fence, idempotency quota/sweep, and audit sequence use transaction-scoped wait locks bounded by `statement_timeout`;
- all lock errors remain fixed/redacted.

**Step 3: Verify and commit**

Run:

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
git diff --check
```

Commit:

```bash
git add src/backend/postgres.rs src/backend/postgres_migrations.rs
git commit -m "refactor: use postgres advisory lock helper"
```

## Task 5: Preserve Existing CAS/Fencing Behavior With Focused Tests

**Files:**

- Modify tests only as needed in `src/backend/core_transaction.rs`
- Modify tests only as needed in `src/backend/object_cleanup.rs`
- Modify tests only as needed in `src/server/mod.rs`
- Modify tests only as needed in `src/server/routes_vcs.rs`

**Step 1: Add or strengthen tests**

Add tests only where gaps remain after inspection:

- concurrent ref CAS still allows exactly one winner and reports stale conflicts;
- concurrent source-checked ref updates still observe source freshness;
- concurrent scheduler ticks over the same stores still use recovery claims, not a global lock;
- stale recovery claim owner/token/expiry still cannot complete/fail/poison;
- destructive cleanup still requires active cleanup claim, active metadata fence, current deletion readiness, final-byte proof, and final-metadata proof.
- audit recovery append remains protected by recovery claims plus audit presence checks; if tests expose duplicate append after lease expiry, fix the audit/recovery contract rather than adding a broad scheduler lock.

Do not add locks to make these tests pass. Fix only broken local contracts if a test exposes a real bug.

Run:

```bash
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_recovery --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_fs_mutation_recovery --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked backend::object_cleanup --lib -- --nocapture
```

**Step 2: Commit only if tests or fixes changed**

```bash
git add src/backend/core_transaction.rs src/backend/object_cleanup.rs src/server/mod.rs src/server/routes_vcs.rs
git commit -m "test: preserve durable fencing without broad locks"
```

Skip the commit if no files changed.

## Task 6: Document Operator And Multi-Node Lock Posture

**Files:**

- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update HTTP/operator docs**

Document near the backend durability and recovery sections:

- Slice 8 adds a Postgres transaction-advisory-lock helper for narrow Postgres metadata critical sections only.
- No Redis dependency is required.
- Runtime commits/ref updates remain CAS/source-check fenced.
- Recovery workers and scheduler remain multi-node-safe through persisted claim owner/token/expiry leases and idempotent completion.
- Object cleanup remains fenced by cleanup claims and final-object metadata fences; broad unreachable deletion remains dry-run only.
- Single-node durable-cloud config is unchanged.
- Durable-cloud unsupported surfaces remain stable `501`.

**Step 2: Update project status after verification**

Add a completed Slice 8 section only after implementation and reviews complete. Include factual verification results only for commands actually run. If local live provider credentials are absent, state that local provider-backed evidence was not collected and do not claim new protected CI evidence unless it is inspected.

**Step 3: Verify and commit**

Run:

```bash
rg -n "distributed lock|advisory|Redis|multi-node|CAS|fence|501" docs/http-api-guide.md docs/project-status.md
git diff --check
```

Commit:

```bash
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: document distributed lock posture"
```

## Review Plan

After implementation tasks and before final gates:

1. Dispatch a spec/correctness review subagent on the full diff. It must check taxonomy completeness, no Redis, no broad locks around CAS/fenced paths, multi-handle Postgres lock tests, fail-closed behavior, docs accuracy, and all acceptance criteria.
2. Fix valid findings locally or with a scoped fix subagent.
3. Dispatch a code-quality/security review subagent on the full diff. It must check Rust async correctness, transaction lifetime, pooled-client lock safety, redaction, test flakiness, and unnecessary public API exposure.
4. Fix valid findings locally or with a scoped fix subagent.
5. Rerun focused gates after each fix.

## Required Verification

Run before pushing:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::tests::slice8_durable_critical_section_taxonomy_covers_current_runtime_sections --lib -- --exact --nocapture
cargo test --locked --features postgres backend::postgres::tests::postgres_advisory_xact_lock --lib -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

If local Postgres credentials are present, also run the focused helper tests against real Postgres with `STRATUM_POSTGRES_TEST_URL` set. If local credentials are absent, the optional Postgres tests may skip locally; do not claim live/provider-backed evidence unless a local provider run or current protected CI is actually inspected.

## Merge Plan

After local gates pass:

```bash
git push origin v2/foundation
git worktree add /tmp/stratum-main-merge origin/main
cd /tmp/stratum-main-merge
git switch -c main-merge-$(date +%Y%m%d%H%M%S)
git merge --no-ff v2/foundation -m "merge: distributed lock service"
git push origin HEAD:main
cd /Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation
git worktree remove /tmp/stratum-main-merge
```

Do not touch the intentionally dirty local `main` checkout.
