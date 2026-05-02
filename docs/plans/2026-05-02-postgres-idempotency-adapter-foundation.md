# Postgres Idempotency Adapter Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement a feature-gated Postgres-backed `IdempotencyStore` adapter that proves the durable schema can preserve current HTTP retry/replay semantics without wiring `stratum-server` to Postgres.

**Architecture:** Reuse the existing `idempotency_records` table from `migrations/postgres/0001_durable_backend_foundation.sql` and implement `IdempotencyStore` for `PostgresMetadataStore`. Keep the adapter behind the existing `postgres` feature, connect per operation using the existing Postgres helper, store only hashed idempotency keys, and leave server runtime selection unchanged.

**Tech Stack:** Rust 2024, `tokio-postgres`, existing `IdempotencyStore` trait, existing `PostgresMetadataStore`, live Postgres tests gated by `STRATUM_POSTGRES_TEST_URL`.

---

## Scope Boundaries

This slice is intentionally safe for a smaller model to implement because it stays behind `--features postgres` and does not affect server behavior.

In scope:

- Implement `IdempotencyStore` for `PostgresMetadataStore`.
- Add focused live Postgres tests for replay, conflict, pending, abort, completion, and concurrent begin semantics.
- Add crate-visible accessors or constructors in `src/idempotency.rs` only as needed for store implementations.
- Update docs to explain retention and secret posture.

Out of scope:

- No `stratum-server` runtime cutover.
- No connection pooling.
- No migration/schema changes unless a test proves the current schema cannot satisfy the contract.
- No cleanup/retention worker.
- No workspace-token idempotency. `POST /workspaces/{id}/tokens` must keep rejecting `Idempotency-Key`.
- No new HTTP behavior.

Secret and retention posture for this foundation:

- Raw `Idempotency-Key` values must never be stored; only `IdempotencyKey::key_hash()` goes into Postgres.
- `request_fingerprint` is already a hash over normalized request semantics and body shape. Store it as-is.
- `response_body_json` may contain replayable non-secret JSON for currently supported endpoints. Do not use this adapter for token issuance or other secret-bearing responses.
- Records are retained indefinitely in this foundation because the existing migration has no `expires_at`. Document this as a future runtime-cutover blocker, not something to paper over in code.

## Task 1: Expose Minimal Idempotency Store Internals

**Files:**
- Modify: `src/idempotency.rs`
- Test: `src/idempotency.rs`

**Step 1: Write the failing test**

Add a unit test showing a store implementation can round-trip a reservation without exposing the raw key:

```rust
#[test]
fn reservation_accessors_expose_store_identity_without_raw_key() {
    let key = IdempotencyKey::parse_header_value(&HeaderValue::from_static("raw-retry-key")).unwrap();
    let reservation = IdempotencyReservation::for_store("runs:create", &key, "request-a");

    assert_eq!(reservation.scope(), "runs:create");
    assert_eq!(reservation.key_hash(), key.key_hash());
    assert_eq!(reservation.request_fingerprint(), "request-a");
    assert_ne!(reservation.key_hash(), "raw-retry-key");
}
```

Run:

```bash
cargo test --locked idempotency::tests::reservation_accessors_expose_store_identity_without_raw_key -- --nocapture
```

Expected: FAIL because the constructor/accessors do not exist.

**Step 2: Implement minimal accessors**

Add crate-visible helpers:

```rust
impl IdempotencyReservation {
    pub(crate) fn for_store(
        scope: &str,
        key: &IdempotencyKey,
        request_fingerprint: &str,
    ) -> Self {
        Self {
            key: IdempotencyStoreKey::new(scope, key),
            request_fingerprint: request_fingerprint.to_string(),
        }
    }

    pub(crate) fn scope(&self) -> &str {
        &self.key.scope
    }

    pub(crate) fn key_hash(&self) -> &str {
        &self.key.key_hash
    }

    pub(crate) fn request_fingerprint(&self) -> &str {
        &self.request_fingerprint
    }
}
```

Then update `begin_locked` to call `IdempotencyReservation::for_store(...)` instead of constructing fields directly.

Run:

```bash
cargo test --locked idempotency::tests::reservation_accessors_expose_store_identity_without_raw_key -- --nocapture
```

Expected: PASS.

**Step 3: Commit**

```bash
git add src/idempotency.rs
git commit -m "refactor: expose idempotency reservation store accessors"
```

## Task 2: Implement Postgres Idempotency Begin/Complete/Abort

**Files:**
- Modify: `src/backend/postgres.rs`
- Test: `src/backend/postgres.rs`

**Step 1: Write failing live Postgres tests**

Inside `src/backend/postgres.rs` test module, add `run_idempotency_contracts(&PostgresMetadataStore)` and call it from `run_backend_contracts`.

Cover these behaviors:

1. First `begin("runs:create", key, "request-a")` returns `IdempotencyBegin::Execute`.
2. `complete(..., 201, json!({"run_id":"run_123"}))` publishes the replay.
3. Same key plus same fingerprint returns `Replay` with status/body.
4. Same key plus different fingerprint returns `Conflict`.
5. A pending same-fingerprint second begin returns `InProgress`.
6. A pending different-fingerprint begin returns `Conflict`.
7. `abort()` removes only the matching pending reservation, allowing a new `Execute`.
8. Completing a stale or aborted reservation returns `VfsError::InvalidArgs`.
9. Concurrent same-key same-fingerprint begins produce one `Execute` and one `InProgress`, not two executions.
10. The `idempotency_records.key_hash` column contains the SHA-256 hash, not the raw retry key.

Use this command:

```bash
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres cargo test --locked --features postgres backend::postgres -- --nocapture
```

Expected: FAIL because `PostgresMetadataStore` does not implement `IdempotencyStore`.

**Step 2: Implement `IdempotencyStore for PostgresMetadataStore`**

Add imports:

```rust
use crate::idempotency::{
    IdempotencyBegin, IdempotencyKey, IdempotencyRecord, IdempotencyReservation, IdempotencyStore,
};
```

Implement `begin` using the current schema:

- Try to insert a pending row:

```sql
INSERT INTO idempotency_records (
    scope,
    key_hash,
    request_fingerprint,
    state,
    reserved_at,
    created_at
)
VALUES ($1, $2, $3, 'pending', clock_timestamp(), clock_timestamp())
ON CONFLICT (scope, key_hash) DO NOTHING
RETURNING state, request_fingerprint, status_code, response_body_json
```

- If the insert returns a row, return `IdempotencyBegin::Execute(IdempotencyReservation::for_store(...))`.
- If it did not insert, load the existing row:

```sql
SELECT state, request_fingerprint, status_code, response_body_json
FROM idempotency_records
WHERE scope = $1 AND key_hash = $2
```

- Map existing rows:
  - `completed` plus matching fingerprint -> `Replay(IdempotencyRecord { ... })`
  - `completed` plus different fingerprint -> `Conflict`
  - `pending` plus matching fingerprint -> `InProgress`
  - `pending` plus different fingerprint -> `Conflict`
  - unknown state or malformed completed row -> `VfsError::CorruptStore`
- If an insert conflicted but the row is gone by the time it is loaded, retry once. If it is still missing, return `VfsError::ObjectWriteConflict` with a non-secret message.

Implement `complete`:

```sql
UPDATE idempotency_records
SET state = 'completed',
    status_code = $4,
    response_body_json = $5,
    completed_at = clock_timestamp()
WHERE scope = $1
  AND key_hash = $2
  AND request_fingerprint = $3
  AND state = 'pending'
```

- Use `tokio_postgres::types::Json(&response_body)` for JSONB.
- If one row updates, return `Ok(())`.
- If no row updates, return `VfsError::InvalidArgs { message: "idempotency reservation is not pending".to_string() }`.
- Let the schema reject status codes outside `100..=599`; map through existing `postgres_error`.

Implement `abort`:

```sql
DELETE FROM idempotency_records
WHERE scope = $1
  AND key_hash = $2
  AND request_fingerprint = $3
  AND state = 'pending'
```

`abort` returns `()` and should swallow database errors after logging only a redacted debug message, matching the trait's best-effort shape.

**Step 3: Run focused tests**

```bash
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres cargo test --locked --features postgres backend::postgres -- --nocapture
```

Expected: PASS.

**Step 4: Commit**

```bash
git add src/backend/postgres.rs
git commit -m "feat: add postgres idempotency adapter"
```

## Task 3: Document Runtime Boundary, Retention, And Secret Posture

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update `docs/http-api-guide.md`**

In "Backend Durability Status", add:

- Optional `postgres` feature now includes a Postgres idempotency adapter over `idempotency_records`.
- The adapter is not wired into `stratum-server`.
- Workspace-token issuance still rejects idempotency keys because replay storage for secret-bearing responses remains out of scope.
- There is no retention/expiration worker yet; runtime cutover must define retention before hosted use.

**Step 2: Update `docs/project-status.md`**

Add a section titled `## Postgres Idempotency Adapter Foundation` near the Postgres backend sections.

Include:

- What is built: adapter, tests, current semantics.
- What is not built: server cutover, retention cleanup, secret-bearing replay, audit adapter, connection pool.
- Verification commands and observed results.
- Residual risk update: durable idempotency adapter exists but hosted runtime still lacks retention/cutover.

**Step 3: Commit**

```bash
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: document postgres idempotency adapter"
```

## Task 4: Final Verification

Run:

```bash
cargo fmt --all -- --check
STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres cargo test --locked --features postgres backend::postgres -- --nocapture
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Expected: all pass.

## Manager To Implementer Handoff

You are implementing a narrow durable backend foundation slice. Keep the scope small.

Work from branch `v2/foundation` after pulling latest. Do not touch server runtime selection, HTTP routes, SDKs, or migrations unless a test proves the current schema cannot satisfy the `IdempotencyStore` contract. The goal is only to prove the current Postgres schema can back the existing idempotency trait.

Important constraints:

- Use TDD. Add the failing test first, run it, then implement the minimum code.
- Keep raw idempotency keys out of Postgres and logs. The only key value stored must be `IdempotencyKey::key_hash()`.
- Do not make workspace-token issuance idempotent. Secret-bearing replay remains out of scope.
- Do not add retention cleanup. Document that indefinite retention is a blocker for runtime cutover.
- Do not wire this into `stratum-server`; no behavior changes for default builds.
- Keep commits small: one refactor commit, one adapter commit, one docs commit.
- Return the exact tests you ran and any skipped tests. Do not claim success without command output.

If you get stuck on Postgres concurrency, stop and report the failing test and SQL shape rather than broadening scope.
