# Postgres Workspace Metadata Adapter Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement a feature-gated Postgres-backed `WorkspaceMetadataStore` adapter that proves the durable `workspaces` and `workspace_tokens` schema can preserve current workspace/session/token semantics without wiring `stratum-server` to Postgres.

**Architecture:** Reuse the existing `workspaces` and `workspace_tokens` tables from `migrations/postgres/0001_durable_backend_foundation.sql` and implement `WorkspaceMetadataStore` for `PostgresMetadataStore`. This foundation stores workspace rows as global workspace metadata (`repo_id IS NULL`) because the current `WorkspaceRecord` domain type has no repo identity field. Workspace-token issuance must generate raw secrets in process, persist only SHA-256 secret hashes, preserve scoped-prefix normalization, and validate tokens without logging or returning raw secrets except at issuance time.

**Tech Stack:** Rust 2024, `async-trait`, `serde_json`, `tokio-postgres`, existing `WorkspaceMetadataStore` trait, existing `PostgresMetadataStore`, live Postgres tests gated by `STRATUM_POSTGRES_TEST_URL`.

---

## Scope Boundaries

This slice is intentionally narrow. It is suitable for a smaller model because it stays behind the existing `postgres` feature and does not change runtime server behavior.

In scope:

- Expose crate-private workspace helper functions needed by durable adapters without changing public API.
- Implement `WorkspaceMetadataStore` for `PostgresMetadataStore`.
- Add focused live Postgres tests for create/list/get, default refs, session refs, head/version updates, scoped token issuance, prefix normalization, secret hashing, validation success/failure, and direct SQL storage shape.
- Update docs to explain what the adapter proves and what remains unwired.

Out of scope:

- No `stratum-server` runtime cutover.
- No server startup migration execution change.
- No connection pool.
- No repo-scoped workspace model; use `repo_id IS NULL` only.
- No idempotent workspace-token issuance; it remains explicitly rejected by HTTP until secret-aware replay storage is designed.
- No KMS, secret manager, token rotation, revocation, expiry, or hosted operations.
- No changes to HTTP response shape or route behavior.
- No schema migration unless a failing test proves the current schema cannot satisfy the existing `WorkspaceMetadataStore` contract.

Security and compliance posture for this foundation:

- Do not persist raw workspace-token secrets.
- Do not log raw workspace-token secrets, raw agent tokens, workspace-token hashes, connection strings, or workspace token rows.
- Tests may hold an issued raw secret in memory to validate the contract, but must never write that raw secret into Postgres or docs.
- Query/error contexts must remain generic and must not include token names, raw secrets, hashes, read/write prefixes, or connection strings.
- Prefixes are authorization boundaries. Preserve current normalization, sorting, deduplication, and out-of-root rejection behavior.

## Task 1: Expose Workspace Metadata Helper Functions

**Files:**

- Modify: `src/workspace/mod.rs`

**Step 1: Add crate-private module-level helpers**

Near the existing `impl InMemoryWorkspaceMetadataStore`, add module-level helper functions and keep the existing associated functions as private wrappers so current tests and local stores keep their shape:

```rust
pub(crate) fn hash_workspace_token_secret(raw_secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_secret.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub(crate) fn generate_workspace_token_secret() -> String {
    let mut bytes = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn workspace_token_hash_eq(left: &str, right: &str) -> bool {
    constant_time_eq(left.as_bytes(), right.as_bytes())
}
```

Then change the existing private associated helpers to delegate:

```rust
fn hash_secret(raw_secret: &str) -> String {
    hash_workspace_token_secret(raw_secret)
}

fn generate_secret() -> String {
    generate_workspace_token_secret()
}
```

Do not remove `InMemoryWorkspaceMetadataStore::hash_secret` or `generate_secret`; the module's existing tests use them.

**Step 2: Make existing validation helpers crate-private**

Change these existing helper signatures only:

```rust
pub(crate) fn workspace_record(
    name: &str,
    root_path: &str,
    base_ref: &str,
    session_ref: Option<&str>,
) -> Result<WorkspaceRecord, VfsError> {
    // existing body unchanged
}

pub(crate) fn normalize_workspace_token_prefixes(
    workspace_root: &str,
    prefixes: Vec<String>,
) -> Result<Vec<String>, VfsError> {
    // existing body unchanged
}
```

Keep `normalize_workspace_ref`, `normalize_workspace_session_ref`, `normalize_workspace_token_prefix`, `path_matches_prefix`, and `constant_time_eq` private unless the compiler proves they are needed outside this module.

**Step 3: Add a focused helper test**

Inside `#[cfg(test)] mod tests` in `src/workspace/mod.rs`, add:

```rust
#[test]
fn workspace_token_secret_helpers_preserve_existing_hash_shape() {
    let raw_secret = "workspace-helper-secret";
    let expected = InMemoryWorkspaceMetadataStore::hash_secret(raw_secret);
    let actual = hash_workspace_token_secret(raw_secret);

    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 64);
    assert!(actual.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')));
    assert!(workspace_token_hash_eq(&actual, &expected));
    assert!(!workspace_token_hash_eq(&actual, "not-the-same-hash"));
}
```

**Step 4: Run the focused test**

Run:

```bash
cargo test --locked workspace::tests::workspace_token_secret_helpers_preserve_existing_hash_shape -- --nocapture
```

Expected: PASS.

**Step 5: Commit**

```bash
git add src/workspace/mod.rs
git commit -m "refactor: expose workspace metadata helpers"
```

## Task 2: Add Failing Postgres Workspace Contract Tests

**Files:**

- Modify: `src/backend/postgres.rs`

**Step 1: Add imports in the test module**

Inside `#[cfg(test)] mod tests`, extend imports with:

```rust
use crate::workspace::WorkspaceMetadataStore;
```

Keep imports sorted by local style after `cargo fmt`.

**Step 2: Add test helpers near the other Postgres contract helpers**

Add:

```rust
fn workspace_head(label: &str) -> String {
    object_id(label.as_bytes()).to_hex()
}

fn is_lower_hex_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}
```

**Step 3: Add direct SQL assertions for storage shape**

Add these helpers near the other contract helpers:

```rust
async fn assert_workspace_storage_shape(
    store: &PostgresMetadataStore,
    workspace_id: Uuid,
) -> Result<(), VfsError> {
    let client = store.connect_client().await?;
    let row = client
        .query_one(
            r#"SELECT repo_id, name, root_path, version, base_ref, session_ref
               FROM workspaces
               WHERE id = $1"#,
            &[&workspace_id],
        )
        .await
        .map_err(|error| postgres_error("load workspace storage shape", error))?;

    assert!(row.get::<_, Option<String>>("repo_id").is_none());
    assert_eq!(row.get::<_, String>("name"), "alpha");
    assert_eq!(row.get::<_, String>("root_path"), "/alpha");
    assert_eq!(row.get::<_, i64>("version"), 2);
    assert_eq!(row.get::<_, String>("base_ref"), "main");
    assert_eq!(
        row.get::<_, Option<String>>("session_ref").as_deref(),
        Some("agent/demo/session")
    );
    Ok(())
}

async fn assert_workspace_token_storage_shape(
    store: &PostgresMetadataStore,
    token_id: Uuid,
    raw_secret: &str,
) -> Result<(), VfsError> {
    let client = store.connect_client().await?;
    let row = client
        .query_one(
            r#"SELECT secret_hash, read_prefixes_json, write_prefixes_json
               FROM workspace_tokens
               WHERE id = $1"#,
            &[&token_id],
        )
        .await
        .map_err(|error| postgres_error("load workspace token storage shape", error))?;

    let secret_hash: String = row.get("secret_hash");
    assert_ne!(secret_hash, raw_secret);
    assert!(is_lower_hex_sha256(&secret_hash));

    let Json(read_prefixes): Json<Vec<String>> = row.get("read_prefixes_json");
    let Json(write_prefixes): Json<Vec<String>> = row.get("write_prefixes_json");
    assert_eq!(read_prefixes, vec!["/alpha", "/alpha/docs"]);
    assert_eq!(write_prefixes, vec!["/alpha/docs"]);
    Ok(())
}
```

Do not print the raw secret or secret hash in assertion messages.

**Step 4: Add `run_workspace_contracts`**

Add this function and call it from `run_backend_contracts(store).await?` after `run_audit_contracts(store).await?`:

```rust
async fn run_workspace_contracts(store: &PostgresMetadataStore) -> Result<(), VfsError> {
    assert!(WorkspaceMetadataStore::list_workspaces(store).await?.is_empty());

    let beta = WorkspaceMetadataStore::create_workspace(store, "beta", "/beta").await?;
    assert_eq!(beta.name, "beta");
    assert_eq!(beta.root_path, "/beta");
    assert_eq!(beta.version, 0);
    assert_eq!(beta.base_ref, "main");
    assert!(beta.session_ref.is_none());

    let alpha = WorkspaceMetadataStore::create_workspace_with_refs(
        store,
        "alpha",
        "/alpha",
        "main",
        Some("agent/demo/session"),
    )
    .await?;
    assert_eq!(alpha.name, "alpha");
    assert_eq!(alpha.base_ref, "main");
    assert_eq!(alpha.session_ref.as_deref(), Some("agent/demo/session"));

    let listed = WorkspaceMetadataStore::list_workspaces(store).await?;
    assert_eq!(
        listed.iter().map(|workspace| workspace.name.as_str()).collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );

    let loaded = WorkspaceMetadataStore::get_workspace(store, alpha.id)
        .await?
        .expect("workspace should load");
    assert_eq!(loaded.id, alpha.id);
    assert_eq!(loaded.version, 0);

    let head = workspace_head("workspace alpha head");
    let updated = WorkspaceMetadataStore::update_head_commit(store, alpha.id, Some(head.clone()))
        .await?
        .expect("workspace update should return row");
    assert_eq!(updated.head_commit.as_deref(), Some(head.as_str()));
    assert_eq!(updated.version, 1);

    let cleared = WorkspaceMetadataStore::update_head_commit(store, alpha.id, None)
        .await?
        .expect("workspace clear should return row");
    assert!(cleared.head_commit.is_none());
    assert_eq!(cleared.version, 2);
    assert!(WorkspaceMetadataStore::update_head_commit(store, Uuid::new_v4(), None)
        .await?
        .is_none());

    assert_workspace_storage_shape(store, alpha.id).await?;

    assert!(matches!(
        WorkspaceMetadataStore::issue_scoped_workspace_token(
            store,
            alpha.id,
            "bad-scope",
            42,
            vec!["/outside".to_string()],
            vec!["/alpha/docs".to_string()],
        )
        .await,
        Err(VfsError::PermissionDenied { .. })
    ));

    let issued = WorkspaceMetadataStore::issue_scoped_workspace_token(
        store,
        alpha.id,
        "alpha-token",
        42,
        vec![
            "/alpha/docs".to_string(),
            "/alpha/docs/../docs".to_string(),
            "/alpha".to_string(),
        ],
        vec!["/alpha/docs".to_string()],
    )
    .await?;
    assert_eq!(issued.token.workspace_id, alpha.id);
    assert_eq!(issued.token.agent_uid, 42);
    assert_eq!(issued.token.read_prefixes, vec!["/alpha", "/alpha/docs"]);
    assert_eq!(issued.token.write_prefixes, vec!["/alpha/docs"]);
    assert_ne!(issued.token.secret_hash, issued.raw_secret);
    assert!(is_lower_hex_sha256(&issued.token.secret_hash));
    assert_workspace_token_storage_shape(store, issued.token.id, &issued.raw_secret).await?;

    let valid = WorkspaceMetadataStore::validate_workspace_token(store, alpha.id, &issued.raw_secret)
        .await?
        .expect("issued token should validate");
    assert_eq!(valid.workspace.id, alpha.id);
    assert_eq!(valid.token.id, issued.token.id);
    assert_eq!(valid.token.read_prefixes, vec!["/alpha", "/alpha/docs"]);

    assert!(WorkspaceMetadataStore::validate_workspace_token(store, alpha.id, "wrong-secret")
        .await?
        .is_none());
    assert!(WorkspaceMetadataStore::validate_workspace_token(store, beta.id, &issued.raw_secret)
        .await?
        .is_none());

    let default_issued =
        WorkspaceMetadataStore::issue_workspace_token(store, beta.id, "beta-token", 43).await?;
    assert_eq!(default_issued.token.read_prefixes, vec!["/beta"]);
    assert_eq!(default_issued.token.write_prefixes, vec!["/beta"]);
    assert!(
        WorkspaceMetadataStore::validate_workspace_token(store, beta.id, &default_issued.raw_secret)
            .await?
            .is_some()
    );

    Ok(())
}
```

**Step 5: Run the failing test**

Run:

```bash
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected: FAIL to compile because `PostgresMetadataStore` does not implement `WorkspaceMetadataStore`.

If Postgres is not available locally, use the project-standard `STRATUM_POSTGRES_TEST_URL` for your machine. Do not mark this task complete with skipped Postgres tests.

## Task 3: Implement `WorkspaceMetadataStore for PostgresMetadataStore`

**Files:**

- Modify: `src/backend/postgres.rs`

**Step 1: Add imports**

At the top of `src/backend/postgres.rs`, add:

```rust
use crate::workspace::{
    IssuedWorkspaceToken, ValidWorkspaceToken, WorkspaceMetadataStore, WorkspaceRecord,
    WorkspaceTokenRecord, generate_workspace_token_secret, hash_workspace_token_secret,
    normalize_workspace_token_prefixes, workspace_record, workspace_token_hash_eq,
};
```

Keep imports sorted after `cargo fmt`.

**Step 2: Add workspace row helpers**

Near the other row-decoding helpers, add:

```rust
fn uid_to_i32(uid: crate::auth::Uid) -> Result<i32, VfsError> {
    i32::try_from(uid).map_err(|_| VfsError::InvalidArgs {
        message: "workspace token agent uid exceeds Postgres INTEGER range".to_string(),
    })
}

fn i32_to_uid(uid: i32) -> Result<crate::auth::Uid, VfsError> {
    crate::auth::Uid::try_from(uid).map_err(|_| VfsError::CorruptStore {
        message: format!("workspace token has invalid agent uid {uid}"),
    })
}

fn row_to_workspace_record(row: Row) -> Result<WorkspaceRecord, VfsError> {
    let version: i64 = row.get("version");
    if version < 0 {
        return Err(VfsError::CorruptStore {
            message: format!("workspace has invalid negative version {version}"),
        });
    }

    Ok(WorkspaceRecord {
        id: row.get("id"),
        name: row.get("name"),
        root_path: row.get("root_path"),
        head_commit: row.get("head_commit"),
        version: version as u64,
        base_ref: row.get("base_ref"),
        session_ref: row.get("session_ref"),
    })
}

fn row_to_workspace_token_record(row: Row) -> Result<WorkspaceTokenRecord, VfsError> {
    let Json(read_prefixes): Json<Vec<String>> = row
        .try_get("read_prefixes_json")
        .map_err(|_| VfsError::CorruptStore {
            message: "workspace token read prefixes JSON corrupt".to_string(),
        })?;
    let Json(write_prefixes): Json<Vec<String>> = row
        .try_get("write_prefixes_json")
        .map_err(|_| VfsError::CorruptStore {
            message: "workspace token write prefixes JSON corrupt".to_string(),
        })?;
    let agent_uid: i32 = row.get("agent_uid");

    Ok(WorkspaceTokenRecord {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        name: row.get("name"),
        agent_uid: i32_to_uid(agent_uid)?,
        secret_hash: row.get("secret_hash"),
        read_prefixes,
        write_prefixes,
    })
}
```

Do not include `secret_hash` values in error messages.

**Step 3: Add query constants only if they reduce duplication**

If helpful, add a local `SELECT` projection string. Do not create a new abstraction layer just for one adapter.

**Step 4: Implement list/create/get/update**

Add:

```rust
#[async_trait]
impl WorkspaceMetadataStore for PostgresMetadataStore {
    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, VfsError> {
        let client = self.connect_client().await?;
        let rows = client
            .query(
                r#"SELECT id, name, root_path, head_commit, version, base_ref, session_ref
                   FROM workspaces
                   WHERE repo_id IS NULL
                   ORDER BY name ASC, id ASC"#,
                &[],
            )
            .await
            .map_err(|error| postgres_error("workspace list", error))?;
        rows.into_iter().map(row_to_workspace_record).collect()
    }

    async fn create_workspace(
        &self,
        name: &str,
        root_path: &str,
    ) -> Result<WorkspaceRecord, VfsError> {
        self.create_workspace_with_refs(name, root_path, crate::vcs::MAIN_REF, None)
            .await
    }

    async fn create_workspace_with_refs(
        &self,
        name: &str,
        root_path: &str,
        base_ref: &str,
        session_ref: Option<&str>,
    ) -> Result<WorkspaceRecord, VfsError> {
        let record = workspace_record(name, root_path, base_ref, session_ref)?;
        let client = self.connect_client().await?;
        let version = u64_to_i64(record.version, "workspace version")?;
        let row = client
            .query_one(
                r#"INSERT INTO workspaces (
                       id, repo_id, name, root_path, head_commit, version, base_ref, session_ref
                   )
                   VALUES ($1, NULL, $2, $3, $4, $5, $6, $7)
                   RETURNING id, name, root_path, head_commit, version, base_ref, session_ref"#,
                &[
                    &record.id,
                    &record.name,
                    &record.root_path,
                    &record.head_commit,
                    &version,
                    &record.base_ref,
                    &record.session_ref,
                ],
            )
            .await
            .map_err(|error| postgres_error("workspace create", error))?;
        row_to_workspace_record(row)
    }

    async fn get_workspace(&self, id: Uuid) -> Result<Option<WorkspaceRecord>, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_opt(
                r#"SELECT id, name, root_path, head_commit, version, base_ref, session_ref
                   FROM workspaces
                   WHERE repo_id IS NULL AND id = $1"#,
                &[&id],
            )
            .await
            .map_err(|error| postgres_error("workspace get", error))?;
        row.map(row_to_workspace_record).transpose()
    }

    async fn update_head_commit(
        &self,
        id: Uuid,
        head_commit: Option<String>,
    ) -> Result<Option<WorkspaceRecord>, VfsError> {
        let client = self.connect_client().await?;
        let row = client
            .query_opt(
                r#"UPDATE workspaces
                   SET head_commit = $2,
                       version = version + 1
                   WHERE repo_id IS NULL AND id = $1
                   RETURNING id, name, root_path, head_commit, version, base_ref, session_ref"#,
                &[&id, &head_commit],
            )
            .await
            .map_err(|error| postgres_error("workspace update head", error))?;
        row.map(row_to_workspace_record).transpose()
    }

    // issue_scoped_workspace_token and validate_workspace_token implemented below.
}
```

If this conflicts with another trait impl location, place it near the other store trait impls in `src/backend/postgres.rs`.

**Step 5: Implement scoped token issuance inside the same trait impl**

Replace the placeholder comment with:

```rust
    async fn issue_scoped_workspace_token(
        &self,
        workspace_id: Uuid,
        name: &str,
        agent_uid: crate::auth::Uid,
        read_prefixes: Vec<String>,
        write_prefixes: Vec<String>,
    ) -> Result<IssuedWorkspaceToken, VfsError> {
        let mut client = self.connect_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(|error| postgres_error("workspace token transaction", error))?;

        let workspace_row = tx
            .query_opt(
                r#"SELECT id, name, root_path, head_commit, version, base_ref, session_ref
                   FROM workspaces
                   WHERE repo_id IS NULL AND id = $1
                   FOR UPDATE"#,
                &[&workspace_id],
            )
            .await
            .map_err(|error| postgres_error("workspace token load workspace", error))?;
        let Some(workspace_row) = workspace_row else {
            return Err(VfsError::NotFound {
                path: format!("workspace:{workspace_id}"),
            });
        };
        let workspace = row_to_workspace_record(workspace_row)?;
        let read_prefixes = normalize_workspace_token_prefixes(&workspace.root_path, read_prefixes)?;
        let write_prefixes =
            normalize_workspace_token_prefixes(&workspace.root_path, write_prefixes)?;
        let read_json = Json(&read_prefixes);
        let write_json = Json(&write_prefixes);
        let agent_uid = uid_to_i32(agent_uid)?;

        for _ in 0..3 {
            let raw_secret = generate_workspace_token_secret();
            let secret_hash = hash_workspace_token_secret(&raw_secret);
            let token_id = Uuid::new_v4();
            let row = tx
                .query_opt(
                    r#"INSERT INTO workspace_tokens (
                           id, workspace_id, name, agent_uid, secret_hash,
                           read_prefixes_json, write_prefixes_json
                       )
                       VALUES ($1, $2, $3, $4, $5, $6, $7)
                       ON CONFLICT (workspace_id, secret_hash) DO NOTHING
                       RETURNING id, workspace_id, name, agent_uid, secret_hash,
                                 read_prefixes_json, write_prefixes_json"#,
                    &[
                        &token_id,
                        &workspace_id,
                        &name,
                        &agent_uid,
                        &secret_hash,
                        &read_json,
                        &write_json,
                    ],
                )
                .await
                .map_err(|error| postgres_error("workspace token insert", error))?;

            if let Some(row) = row {
                let token = row_to_workspace_token_record(row)?;
                tx.commit()
                    .await
                    .map_err(|error| postgres_error("workspace token commit", error))?;
                return Ok(IssuedWorkspaceToken { token, raw_secret });
            }
        }

        Err(VfsError::ObjectWriteConflict {
            message: "workspace token secret collision after retries".to_string(),
        })
    }
```

Do not log or format `raw_secret` or `secret_hash`.

**Step 6: Implement token validation**

Still inside the trait impl, add:

```rust
    async fn validate_workspace_token(
        &self,
        workspace_id: Uuid,
        raw_secret: &str,
    ) -> Result<Option<ValidWorkspaceToken>, VfsError> {
        let client = self.connect_client().await?;
        let workspace_row = client
            .query_opt(
                r#"SELECT id, name, root_path, head_commit, version, base_ref, session_ref
                   FROM workspaces
                   WHERE repo_id IS NULL AND id = $1"#,
                &[&workspace_id],
            )
            .await
            .map_err(|error| postgres_error("workspace token validate workspace", error))?;
        let Some(workspace_row) = workspace_row else {
            return Ok(None);
        };
        let workspace = row_to_workspace_record(workspace_row)?;
        let expected_hash = hash_workspace_token_secret(raw_secret);

        let rows = client
            .query(
                r#"SELECT id, workspace_id, name, agent_uid, secret_hash,
                          read_prefixes_json, write_prefixes_json
                   FROM workspace_tokens
                   WHERE workspace_id = $1
                   ORDER BY created_at ASC, id ASC"#,
                &[&workspace_id],
            )
            .await
            .map_err(|error| postgres_error("workspace token validate token", error))?;

        for row in rows {
            let token = row_to_workspace_token_record(row)?;
            let normalized_read =
                normalize_workspace_token_prefixes(&workspace.root_path, token.read_prefixes.clone())
                    .map_err(|_| VfsError::CorruptStore {
                        message: "workspace token read prefixes are outside workspace root"
                            .to_string(),
                    })?;
            if normalized_read != token.read_prefixes {
                return Err(VfsError::CorruptStore {
                    message: "workspace token read prefixes are outside workspace root".to_string(),
                });
            }
            let normalized_write = normalize_workspace_token_prefixes(
                &workspace.root_path,
                token.write_prefixes.clone(),
            )
            .map_err(|_| VfsError::CorruptStore {
                message: "workspace token write prefixes are outside workspace root".to_string(),
            })?;
            if normalized_write != token.write_prefixes {
                return Err(VfsError::CorruptStore {
                    message: "workspace token write prefixes are outside workspace root".to_string(),
                });
            }
            if workspace_token_hash_eq(&token.secret_hash, &expected_hash) {
                return Ok(Some(ValidWorkspaceToken { workspace, token }));
            }
        }

        Ok(None)
    }
```

This intentionally compares hashes in Rust with the same constant-time helper used by the local store instead of putting the raw secret or expected hash into logs or errors.

**Step 7: Run focused tests**

Run:

```bash
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected: PASS.

If the compiler asks for explicit `Json` type annotations, introduce locals like `let read_json = Json(&read_prefixes);` and pass references to those locals. Do not stringify JSON.

**Step 8: Commit**

```bash
git add src/workspace/mod.rs src/backend/postgres.rs
git commit -m "feat: add postgres workspace metadata adapter"
```

## Task 4: Document Runtime Boundary And Residual Risks

**Files:**

- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update `docs/http-api-guide.md`**

In "Backend Durability Status", near the Postgres adapter paragraphs, add:

```markdown
The optional `postgres` feature also includes a Postgres-backed `WorkspaceMetadataStore` over `workspaces` and `workspace_tokens`, currently exercised only by live adapter tests. It stores global workspace rows with `repo_id IS NULL`, preserves base/session refs and head-version updates, and persists only workspace-token secret hashes with normalized read/write prefixes. It is not wired into `stratum-server`, does not make workspace-token issuance idempotent, and does not add token rotation, expiry, revocation, or hosted secret-management behavior.
```

**Step 2: Update `docs/project-status.md`**

Update the top "Latest completed backend slice" only when implementation is complete:

```markdown
- Latest completed backend slice: Postgres workspace metadata adapter foundation (crate-only; `postgres` feature)
```

In the "Postgres Metadata Adapter" section, add:

```markdown
- `PostgresMetadataStore` implements `WorkspaceMetadataStore` over global `workspaces` rows (`repo_id IS NULL`) and `workspace_tokens`, preserving base/session refs, head-version updates, scoped-prefix normalization, and hash-only workspace-token validation.
```

Adjust "What is not built" in that section so it no longer says the workspace adapter is missing, but still says protected-change/review adapters are not built.

Add a new section after "Postgres Audit Adapter Foundation":

```markdown
## Postgres Workspace Metadata Adapter Foundation

The Postgres workspace metadata adapter foundation proves the durable `workspaces` and `workspace_tokens` tables can satisfy the existing Rust `WorkspaceMetadataStore` contract without changing server runtime behavior.

What is built:

- Feature-gated `impl WorkspaceMetadataStore for PostgresMetadataStore`, storing global workspaces with `repo_id IS NULL`.
- Workspace create/list/get, base/session ref ownership, head commit updates, and monotonic version increments over the durable `workspaces` table.
- Workspace-token issuance stores only SHA-256 secret hashes and normalized read/write prefix arrays in JSONB; validation returns the existing workspace/token shape without exposing raw secrets.
- Live adapter tests cover workspace ordering, ref fields, head update versioning, scoped token normalization, wrong-secret rejection, wrong-workspace rejection, and raw SQL assertions that token secrets are not stored.

What is not built:

- No `stratum-server` Postgres workspace runtime cutover.
- No idempotent workspace-token issuance or secret-bearing replay persistence.
- No workspace-token expiry, revocation, rotation, KMS/secret-manager integration, or hosted operations.
- No repo-scoped workspace domain model.

Residual risk:

- Production workspace metadata remains local/file-backed until runtime wiring, secret posture, token lifecycle operations, and hosted deployment behavior are designed.

Grounding: `src/workspace/mod.rs`, `src/backend/postgres.rs`, `migrations/postgres/0001_durable_backend_foundation.sql`, `docs/plans/2026-05-03-postgres-workspace-metadata-adapter-foundation.md`.
```

Keep the SDK/DX sections intact; another lane edits this file often.

**Step 3: Run docs diff check**

Run:

```bash
git diff --check docs/http-api-guide.md docs/project-status.md
```

Expected: PASS.

**Step 4: Commit**

```bash
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: document postgres workspace metadata adapter"
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

If local disk pressure appears, use the project-standard workaround:

```bash
export CARGO_TARGET_DIR="$(pwd)/target"
```

If any command fails, stop and report:

- exact command,
- exit code,
- relevant error lines,
- whether code was left modified.

Do not broaden scope to runtime wiring, schema redesign, token lifecycle management, or secret replay to make a test pass.

## Manager To Implementer Handoff

You are implementing a narrow durable backend foundation slice for Stratum.

Work from branch `v2/foundation` after pulling latest. Follow this plan exactly:

`docs/plans/2026-05-03-postgres-workspace-metadata-adapter-foundation.md`

The goal is only to prove the current Postgres `workspaces` and `workspace_tokens` tables can back the existing `WorkspaceMetadataStore` trait. Do not wire it into `stratum-server`; do not change HTTP behavior; do not add connection pooling; do not make workspace-token issuance idempotent; do not add token expiry/revocation/rotation/KMS; do not invent repo identity plumbing.

Hard constraints:

- Use TDD. Add the failing Postgres adapter test before implementing the adapter.
- Use `repo_id IS NULL` for workspace rows because `WorkspaceRecord` currently has no repo ID.
- Preserve local store semantics for workspace ordering, default `main` base ref, `agent/{name}/{session}` session refs, head-version increments, scoped-prefix normalization, and token validation.
- Store structured prefix arrays as JSONB using `Json`, not stringified JSON.
- Persist only SHA-256 workspace-token secret hashes. Never persist or log raw workspace-token secrets.
- Do not include token names, raw secrets, secret hashes, read/write prefixes, or connection strings in adapter diagnostics.
- Keep commits small: helper refactor, adapter implementation, docs.
- Return exact verification commands and observed results.

If you get stuck on helper visibility, JSONB typing, UUID/UID conversion, or workspace-token validation semantics, stop and report the failing test and compiler/database error instead of widening the implementation.
