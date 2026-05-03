# Postgres Audit Adapter Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement a feature-gated Postgres-backed `AuditStore` adapter that proves the durable `audit_events` schema can preserve current append/list semantics without wiring `stratum-server` to Postgres.

**Architecture:** Reuse the existing `audit_events` table from `migrations/postgres/0001_durable_backend_foundation.sql` and implement `AuditStore` for `PostgresMetadataStore`. This foundation stores all events as global audit events (`repo_id IS NULL`) because the current `AuditEvent` domain type has no repo identity field. Sequence allocation must be database-owned and serialized with a Postgres transaction-scoped advisory lock so concurrent appends cannot duplicate or skip global sequence numbers.

**Tech Stack:** Rust 2024, `async-trait`, `chrono`, `serde_json`, `tokio-postgres`, existing `AuditStore` trait, existing `PostgresMetadataStore`, live Postgres tests gated by `STRATUM_POSTGRES_TEST_URL`.

---

## Scope Boundaries

This slice is intentionally narrow. It is suitable for a smaller model only because it stays behind the existing `postgres` feature and does not change runtime server behavior.

In scope:

- Implement `AuditStore` for `PostgresMetadataStore`.
- Add focused live Postgres tests for append, sequence allocation, recent listing, JSON round trips, partial outcomes, and concurrent append behavior.
- Add helper functions in `src/backend/postgres.rs` for audit enum string mapping and row decoding.
- Add the minimal `tokio-postgres` feature needed to read/write `chrono::DateTime<Utc>` from `TIMESTAMPTZ`.
- Update docs to explain what the adapter proves and what remains unwired.

Out of scope:

- No `stratum-server` runtime cutover.
- No event bus, streaming sink, partitioning, retention, or audit export.
- No read/auth/policy-decision audit coverage expansion.
- No repo-scoped audit sequence model; use `repo_id = NULL` only.
- No audit schema migration unless a failing test proves the current schema cannot satisfy the existing `AuditStore` contract.
- No changes to HTTP audit response shape.
- No changes to local/in-memory audit stores beyond tests if absolutely needed.

Security and compliance posture for this foundation:

- Do not log full audit JSON, actor JSON, workspace JSON, resource JSON, or details JSON from the adapter.
- Do not store request bodies, file contents, prompts, commands, raw workspace tokens, raw agent tokens, idempotency keys, or connection strings in tests or adapter diagnostics.
- The adapter persists whatever sanitized `NewAuditEvent` callers provide today. It does not make audit coverage complete for reads/auth/policy decisions.
- `details_json` is a JSONB map of already-redacted string values. Do not widen it to arbitrary raw JSON in this slice.

## Task 1: Add Postgres Timestamp Support

**Files:**

- Modify: `Cargo.toml`

**Step 1: Update `tokio-postgres` features**

Add the chrono integration feature to the existing optional dependency:

```toml
tokio-postgres = { version = "0.7", optional = true, default-features = false, features = ["runtime", "with-serde_json-1", "with-chrono-0_4"] }
```

Do not add a new crate.

**Step 2: Verify the feature compiles**

Run:

```bash
cargo check --locked --features postgres
```

Expected: PASS. If `Cargo.lock` changes unexpectedly, stop and report it; this should only enable an existing transitive feature.

**Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: enable postgres chrono timestamps"
```

If `Cargo.lock` is unchanged, omit it from `git add`.

## Task 2: Add Failing Postgres Audit Contract Tests

**Files:**

- Modify: `src/backend/postgres.rs`

**Step 1: Add imports in the test module**

Inside `#[cfg(test)] mod tests`, extend imports with:

```rust
use crate::audit::{
    AuditAction, AuditActor, AuditOutcome, AuditResource, AuditResourceKind, AuditStore,
    AuditWorkspaceContext, NewAuditEvent,
};
use crate::auth::ROOT_UID;
```

Keep imports sorted by local style after `cargo fmt`.

**Step 2: Add helper constructors**

Add test helpers near the other Postgres contract helpers:

```rust
fn audit_event(label: &str) -> NewAuditEvent {
    NewAuditEvent::new(
        AuditActor::new(ROOT_UID, "root"),
        AuditAction::FsWriteFile,
        AuditResource::path(AuditResourceKind::File, format!("/docs/{label}.md")),
    )
    .with_detail("content_hash", format!("{label}-hash"))
}

fn workspace_audit_event(label: &str) -> NewAuditEvent {
    audit_event(label)
        .with_workspace(AuditWorkspaceContext {
            id: Uuid::new_v4(),
            root_path: "/workspaces/demo".to_string(),
            base_ref: "main".to_string(),
            session_ref: Some("agents/demo/session".to_string()),
        })
        .with_outcome(AuditOutcome::Partial)
        .with_detail("workspace_id", label)
}
```

Do not put secrets in labels/details.

**Step 3: Add `run_audit_contracts`**

Add this function and call it from `run_backend_contracts(store).await?` after `run_idempotency_contracts(store).await?`:

```rust
async fn run_audit_contracts(store: &PostgresMetadataStore) -> Result<(), VfsError> {
    let first = AuditStore::append(store, audit_event("first")).await?;
    assert_eq!(first.sequence, 1);
    assert_eq!(first.actor.username, "root");
    assert_eq!(first.action, AuditAction::FsWriteFile);
    assert_eq!(first.resource.path.as_deref(), Some("/docs/first.md"));
    assert_eq!(first.details.get("content_hash").map(String::as_str), Some("first-hash"));

    let second = AuditStore::append(store, workspace_audit_event("second")).await?;
    assert_eq!(second.sequence, 2);
    assert_eq!(second.outcome, AuditOutcome::Partial);
    assert!(second.workspace.is_some());
    assert_eq!(
        second.details.get("workspace_id").map(String::as_str),
        Some("second")
    );

    let recent_one = AuditStore::list_recent(store, 1).await?;
    assert_eq!(recent_one.len(), 1);
    assert_eq!(recent_one[0].sequence, second.sequence);

    let recent_all = AuditStore::list_recent(store, 10).await?;
    assert_eq!(
        recent_all.iter().map(|event| event.sequence).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(recent_all[1], second);

    assert!(AuditStore::list_recent(store, 0).await?.is_empty());

    let store_arc = Arc::new(store.clone());
    let barrier = Arc::new(Barrier::new(2));
    let first_store = store_arc.clone();
    let first_barrier = barrier.clone();
    let concurrent_first = tokio::spawn(async move {
        first_barrier.wait().await;
        AuditStore::append(&*first_store, audit_event("concurrent-a")).await
    });
    let second_store = store_arc.clone();
    let second_barrier = barrier.clone();
    let concurrent_second = tokio::spawn(async move {
        second_barrier.wait().await;
        AuditStore::append(&*second_store, audit_event("concurrent-b")).await
    });

    let first_out = concurrent_first.await.expect("audit append task a")?;
    let second_out = concurrent_second.await.expect("audit append task b")?;
    let mut sequences = vec![first_out.sequence, second_out.sequence];
    sequences.sort_unstable();
    assert_eq!(sequences, vec![3, 4]);

    let final_recent = AuditStore::list_recent(store, 10).await?;
    assert_eq!(
        final_recent
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );

    Ok(())
}
```

**Step 4: Run the failing test**

Run:

```bash
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected: FAIL to compile because `PostgresMetadataStore` does not implement `AuditStore`.

If Postgres is not available locally, use the project-standard `STRATUM_POSTGRES_TEST_URL` for your machine. Do not mark this task complete with skipped Postgres tests.

## Task 3: Implement `AuditStore for PostgresMetadataStore`

**Files:**

- Modify: `src/backend/postgres.rs`

**Step 1: Add imports**

At the top of `src/backend/postgres.rs`, add:

```rust
use chrono::{DateTime, Utc};

use crate::audit::{
    AuditAction, AuditActor, AuditEvent, AuditOutcome, AuditResource, AuditStore,
    AuditWorkspaceContext, NewAuditEvent,
};
```

If any imported type is only needed in tests, keep it in the test module instead.

**Step 2: Add audit advisory lock constants**

Near the other module-level helper constants/functions, add:

```rust
const AUDIT_LOCK_NAMESPACE: i32 = 0x5354_524d; // "STRM"
const AUDIT_GLOBAL_SEQUENCE_LOCK: i32 = 0x4155_4454; // "AUDT"
```

Use the two-`i32` transaction-scoped advisory lock form to avoid colliding with the migration runner lock namespace.

**Step 3: Add enum mapping helpers**

Use serde's existing snake_case enum representation instead of manually spelling every action/outcome twice:

```rust
fn audit_enum_to_db<T>(value: T, label: &str) -> Result<String, VfsError>
where
    T: serde::Serialize,
{
    match serde_json::to_value(value).map_err(|error| VfsError::CorruptStore {
        message: format!("audit {label} encode failed: {error}"),
    })? {
        serde_json::Value::String(value) => Ok(value),
        _ => Err(VfsError::CorruptStore {
            message: format!("audit {label} did not serialize as a string"),
        }),
    }
}

fn audit_action_from_db(value: &str) -> Result<AuditAction, VfsError> {
    serde_json::from_value(serde_json::Value::String(value.to_string())).map_err(|error| {
        VfsError::CorruptStore {
            message: format!("unknown audit action in Postgres metadata: {value}: {error}"),
        }
    })
}

fn audit_outcome_from_db(value: &str) -> Result<AuditOutcome, VfsError> {
    serde_json::from_value(serde_json::Value::String(value.to_string())).map_err(|error| {
        VfsError::CorruptStore {
            message: format!("unknown audit outcome in Postgres metadata: {value}: {error}"),
        }
    })
}
```

Do not use `Debug` formatting for persisted action/outcome values.

**Step 4: Add JSON encoding helper**

Add a helper so serde failures map consistently:

```rust
fn audit_json<T>(value: &T, label: &str) -> Result<serde_json::Value, VfsError>
where
    T: serde::Serialize,
{
    serde_json::to_value(value).map_err(|error| VfsError::CorruptStore {
        message: format!("audit {label} JSON encode failed: {error}"),
    })
}
```

**Step 5: Add row decoder**

Add:

```rust
fn row_to_audit_event(row: Row) -> Result<AuditEvent, VfsError> {
    let id: Uuid = row.get("id");
    let sequence: i64 = row.get("sequence");
    if sequence <= 0 {
        return Err(VfsError::CorruptStore {
            message: format!("audit event has invalid sequence {sequence}"),
        });
    }
    let timestamp: DateTime<Utc> = row.get("created_at");
    let Json(actor): Json<AuditActor> = row
        .try_get("actor_json")
        .map_err(|_| VfsError::CorruptStore {
            message: "audit event actor JSON corrupt".to_string(),
        })?;
    let workspace: Option<Json<AuditWorkspaceContext>> = row
        .try_get("workspace_json")
        .map_err(|_| VfsError::CorruptStore {
            message: "audit event workspace JSON corrupt".to_string(),
        })?;
    let action_text: String = row.get("action");
    let Json(resource): Json<AuditResource> = row
        .try_get("resource_json")
        .map_err(|_| VfsError::CorruptStore {
            message: "audit event resource JSON corrupt".to_string(),
        })?;
    let outcome_text: String = row.get("outcome");
    let Json(details): Json<BTreeMap<String, String>> = row
        .try_get("details_json")
        .map_err(|_| VfsError::CorruptStore {
            message: "audit event details JSON corrupt".to_string(),
        })?;

    Ok(AuditEvent {
        id,
        sequence: sequence as u64,
        timestamp,
        actor,
        workspace: workspace.map(|Json(workspace)| workspace),
        action: audit_action_from_db(&action_text)?,
        resource,
        outcome: audit_outcome_from_db(&outcome_text)?,
        details,
    })
}
```

If the compiler rejects direct `DateTime<Utc>` conversion, confirm Task 1's `with-chrono-0_4` feature is present before adding workarounds.

**Step 6: Implement `append`**

Add:

```rust
#[async_trait]
impl AuditStore for PostgresMetadataStore {
    async fn append(&self, event: NewAuditEvent) -> Result<AuditEvent, VfsError> {
        let mut client = self.connect_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("audit append transaction", error))?;

        tx.execute(
            "SELECT pg_advisory_xact_lock($1, $2)",
            &[&AUDIT_LOCK_NAMESPACE, &AUDIT_GLOBAL_SEQUENCE_LOCK],
        )
        .await
        .map_err(|error| postgres_error("audit sequence lock", error))?;

        let sequence_row = tx
            .query_one(
                "SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence
                 FROM audit_events
                 WHERE repo_id IS NULL",
                &[],
            )
            .await
            .map_err(|error| postgres_error("audit next sequence", error))?;
        let sequence: i64 = sequence_row.get("next_sequence");

        let id = Uuid::new_v4();
        let actor_json = audit_json(&event.actor, "actor")?;
        let workspace_json = event
            .workspace
            .as_ref()
            .map(|workspace| audit_json(workspace, "workspace"))
            .transpose()?;
        let action = audit_enum_to_db(event.action, "action")?;
        let resource_json = audit_json(&event.resource, "resource")?;
        let outcome = audit_enum_to_db(event.outcome, "outcome")?;
        let details_json = audit_json(&event.details, "details")?;

        let row = tx
            .query_one(
                r#"INSERT INTO audit_events (
                       id,
                       repo_id,
                       sequence,
                       actor_json,
                       workspace_json,
                       action,
                       resource_json,
                       outcome,
                       details_json
                   )
                   VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8)
                   RETURNING id, sequence, created_at, actor_json, workspace_json,
                             action, resource_json, outcome, details_json"#,
                &[
                    &id,
                    &sequence,
                    &Json(&actor_json),
                    &workspace_json.as_ref().map(Json),
                    &action,
                    &Json(&resource_json),
                    &outcome,
                    &Json(&details_json),
                ],
            )
            .await
            .map_err(|error| postgres_error("audit insert event", error))?;

        let event = row_to_audit_event(row)?;
        tx.commit()
            .await
            .map_err(|error| postgres_error("audit append commit", error))?;
        Ok(event)
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<AuditEvent>, VfsError> {
        // implemented in Step 7
        todo!()
    }
}
```

If the `workspace_json.as_ref().map(Json)` parameter shape does not compile, create a local `Option<Json<&serde_json::Value>>` variable before the query and pass a reference to it. Do not stringify JSON.

**Step 7: Implement `list_recent`**

Replace the `todo!()` with:

```rust
    async fn list_recent(&self, limit: usize) -> Result<Vec<AuditEvent>, VfsError> {
        let client = self.connect_client().await?;
        let limit = i64::try_from(limit).map_err(|_| VfsError::InvalidArgs {
            message: "audit list limit exceeds Postgres BIGINT range".to_string(),
        })?;
        if limit == 0 {
            return Ok(Vec::new());
        }

        let rows = client
            .query(
                r#"SELECT id, sequence, created_at, actor_json, workspace_json,
                          action, resource_json, outcome, details_json
                   FROM audit_events
                   WHERE repo_id IS NULL
                   ORDER BY sequence DESC
                   LIMIT $1"#,
                &[&limit],
            )
            .await
            .map_err(|error| postgres_error("audit list recent", error))?;

        let mut events = rows
            .into_iter()
            .map(row_to_audit_event)
            .collect::<Result<Vec<_>, VfsError>>()?;
        events.reverse();
        Ok(events)
    }
```

**Step 8: Run focused tests**

Run:

```bash
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected: PASS.

**Step 9: Commit**

```bash
git add Cargo.toml Cargo.lock src/backend/postgres.rs
git commit -m "feat: add postgres audit adapter"
```

## Task 4: Document Runtime Boundary And Residual Risks

**Files:**

- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update `docs/http-api-guide.md`**

In "Backend Durability Status", add a short paragraph near the Postgres adapter paragraphs:

```markdown
The optional `postgres` feature also includes a Postgres-backed `AuditStore` over `audit_events`, currently exercised only by live adapter tests. It stores sanitized audit event actor/workspace/resource/details JSON and allocates global sequences with a database transaction lock, but it is not wired into `stratum-server` and does not expand read/auth/policy-decision audit coverage yet.
```

**Step 2: Update `docs/project-status.md`**

Update the top "Latest completed backend slice" only when implementation is complete:

```markdown
- Latest completed backend slice: Postgres audit adapter foundation (crate-only; `postgres` feature)
```

Add a section after "Postgres Idempotency Adapter Foundation":

```markdown
## Postgres Audit Adapter Foundation

The Postgres audit adapter foundation proves the durable `audit_events` table can satisfy the existing Rust `AuditStore` append/list contract without changing server runtime behavior.

What is built:

- Feature-gated `impl AuditStore for PostgresMetadataStore`, storing sanitized actor/workspace/resource/details JSONB and action/outcome text using the existing serde snake_case enum shape.
- Global audit events use `repo_id IS NULL` and a transaction-scoped Postgres advisory lock for sequence allocation.
- Live adapter tests cover append, partial outcome and workspace JSON round trip, `list_recent` ordering/limit behavior, and concurrent append sequence uniqueness.

What is not built:

- No `stratum-server` Postgres audit runtime cutover.
- No event bus, streaming sink, partitioning, export, retention, or hosted audit operations.
- No read/auth/policy-decision audit expansion.
- No repo-scoped audit sequence model.

Residual risk:

- Production audit remains local/file-backed until runtime wiring, retention/export, policy-decision coverage, and hosted operations are designed.
```

Keep the status doc's SDK/DX sections intact; another lane edits this file often.

**Step 3: Run docs diff check**

Run:

```bash
git diff --check docs/http-api-guide.md docs/project-status.md
```

Expected: PASS.

**Step 4: Commit**

```bash
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: document postgres audit adapter"
```

## Task 5: Final Verification

Run all of these from `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation`:

```bash
cargo fmt --all -- --check
STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Expected: all pass.

If any command fails, stop and report:

- exact command,
- exit code,
- relevant error lines,
- whether code was left modified.

Do not broaden scope to runtime wiring or schema redesign to make a test pass.

## Manager To Implementer Handoff

You are implementing a narrow durable backend foundation slice for Stratum.

Work from branch `v2/foundation` after pulling latest. Follow this plan exactly:

`docs/plans/2026-05-03-postgres-audit-adapter-foundation.md`

The goal is only to prove the current Postgres `audit_events` table can back the existing `AuditStore` trait. Do not wire it into `stratum-server`; do not change HTTP behavior; do not add audit coverage for reads/auth/policy decisions; do not add an event bus or retention worker.

Hard constraints:

- Use TDD. Add the failing Postgres adapter test first and run it before implementing.
- Store structured JSONB using serde/`Json`, not stringified JSON.
- Persist action/outcome using the existing serde snake_case enum representation, not `Debug` output.
- Use `repo_id IS NULL` only. Do not invent repo identity plumbing in this slice.
- Allocate global audit sequence numbers inside a transaction with a Postgres advisory transaction lock.
- Do not log actor/workspace/resource/details JSON or connection strings.
- Keep commits small: one dependency-feature commit if needed, one adapter commit, one docs commit.
- Return exact verification commands and observed results.

If you get stuck on Postgres JSON typing, timestamp conversion, or sequence concurrency, stop and report the failing test and compiler/database error instead of widening the implementation.
