# Postgres FTS Search MVP Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the currently unavailable semantic search surface with a derived, ACL-aware Postgres full-text search MVP for durable committed state while preserving fail-closed behavior when the index is missing, stale, failed, unavailable, or scoped to the wrong repo/head.

**Architecture:** Add an additive Postgres migration for FTS index state and searchable file rows keyed by repo, commit, and root tree. Add a provider-free indexer that walks durable committed tree/blob objects and writes bounded text rows idempotently, then expose `GET /search/semantic` through a store trait that fails closed unless the indexed head exactly matches the requested durable ref. Search results are metadata-safe and are rechecked through the existing committed reader/session permission logic before rendering.

**Tech Stack:** Rust 2024, Axum, Tokio, Postgres `tsvector`/`websearch_to_tsquery`, existing durable object/commit/ref store traits, TypeScript SDK, Python SDK, Bash SDK, provider-free tests, and optional live Postgres tests behind `STRATUM_POSTGRES_TEST_URL`.

---

## Current Inventory

- `src/server/routes_capabilities.rs` advertises `routes.search.semantic.available = false` with reason `not implemented`; `/search/semantic` is not mounted.
- `src/server/routes_fs.rs` mounts `/search/grep`, `/search/find`, and `/tree` for both local and durable read routers. These routes resolve workspace-relative paths through `Session`.
- `src/backend/committed_read.rs` already reads durable committed refs and enforces read/execute permissions, mount scope, symlink handling, traversal caps, and redacted durable read errors for `tree`, `find`, `grep`, and `cat_with_stat`.
- `src/server/core.rs` wires durable-cloud reads through `DurableCoreRuntime`, backed by `StratumStores` object/commit/ref stores.
- `src/backend/postgres.rs` implements Postgres-backed object metadata, commit, ref, workspace, idempotency, audit, recovery, and review stores through `PostgresMetadataStore`.
- Postgres migrations currently stop at `0018_scim_provisioning_foundation.sql`; `src/backend/postgres_migrations.rs` verifies known schema shape for apply/adopt.
- TypeScript and Python SDK `semantic()` methods currently throw `UnsupportedFeatureError`; `@stratum/bash` `sgrep` reports unsupported.
- Durable-cloud must not fall back to local `.vfs` state for search.

## API Shape

Route:

```http
GET /search/semantic?query=<text>&path=<optional>&limit=<optional>
```

Success:

```json
{
  "results": [
    {
      "path": "/docs/runbook.md",
      "score": 0.514,
      "snippet": "checkout timeout mitigation ...",
      "commit": "64-char-hex",
      "root_tree": "64-char-hex",
      "match": {
        "rank": 0.514,
        "headline": "checkout timeout mitigation ..."
      }
    }
  ],
  "count": 1,
  "commit": "64-char-hex",
  "root_tree": "64-char-hex",
  "stale": false
}
```

Fail-closed responses:

- `501` when semantic search storage is unavailable for the server/runtime.
- `503` when the index state is missing, stale, failed, or indexed for a different repo/commit/root tree.
- `400` for missing, empty, or overlong query/limit/path inputs.
- `403` when the authenticated session is not allowed to search/read the scoped durable view.

Responses, errors, audit details, docs examples, and SDK errors must not include raw SQL, DB URLs, object keys, backing paths, env vars, provider errors, raw tokens, raw index vectors, or secret-like material.

## Schema

Add `migrations/postgres/0019_postgres_fts_search_mvp.sql`.

Tables:

- `search_index_state`
  - `repo_id TEXT NOT NULL`
  - `commit_id TEXT NOT NULL`
  - `root_tree_id TEXT NOT NULL`
  - `status TEXT NOT NULL CHECK (status IN ('indexing', 'ready', 'failed'))`
  - `indexed_file_count INTEGER NOT NULL DEFAULT 0 CHECK (indexed_file_count >= 0)`
  - `indexed_byte_count BIGINT NOT NULL DEFAULT 0 CHECK (indexed_byte_count >= 0)`
  - `failure_code TEXT`
  - `started_at TIMESTAMPTZ NOT NULL DEFAULT now()`
  - `completed_at TIMESTAMPTZ`
  - `updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`
  - primary key `(repo_id, commit_id, root_tree_id)`
  - foreign key `(repo_id, commit_id)` to `commits(repo_id, id)` with cascade delete

- `search_index_files`
  - `repo_id TEXT NOT NULL`
  - `commit_id TEXT NOT NULL`
  - `root_tree_id TEXT NOT NULL`
  - `path TEXT NOT NULL`
  - `object_id TEXT NOT NULL`
  - `byte_len INTEGER NOT NULL CHECK (byte_len >= 0)`
  - `content_preview TEXT NOT NULL`
  - `search_vector TSVECTOR NOT NULL`
  - `updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`
  - primary key `(repo_id, commit_id, root_tree_id, path)`
  - foreign key `(repo_id, commit_id, root_tree_id)` to `search_index_state`
  - strict path/object/check constraints
  - GIN index on `search_vector`
  - lookup indexes on `(repo_id, commit_id, root_tree_id)` and `(repo_id, commit_id, root_tree_id, path)`

Do not store raw extracted binary material, embeddings, provider responses, tokens, DB URLs, or environment values.

## Task 1: Migration And Adoption Checks

**Files:**
- Add: `migrations/postgres/0019_postgres_fts_search_mvp.sql`
- Modify: `src/backend/postgres_migrations.rs`

**Step 1: Write failing migration tests**

Add tests that version 19 is registered, the catalog length is 19, SQL is additive/non-destructive, required tables/indexes/checks exist, and forbidden raw/provider/vector material strings do not appear.

Run:

```bash
cargo test --locked --features postgres backend::postgres_migrations::tests::postgres_fts_search_mvp_migration_is_registered_and_non_destructive --lib -- --nocapture
```

Expected: FAIL because migration 19 is absent.

**Step 2: Implement the migration and catalog entry**

Add the SQL file, include it, append `PostgresMigration { version: 19, name: "postgres_fts_search_mvp", ... }`, and update the catalog length.

**Step 3: Add schema adoption verification**

Extend `verify_known_schema_catalog` to require both search tables, columns, primary keys, foreign keys, constraints, column shapes/defaults, and index shapes. Add negative tests that weakened status/path/object constraints and weakened GIN/index shapes fail adoption without leaking table/column names.

**Step 4: Verify**

```bash
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
```

Expected: PASS, with live portions skipped unless `STRATUM_POSTGRES_TEST_URL` is set.

## Task 2: Provider-Free Search Domain And Indexer

**Files:**
- Add: `src/backend/search_index.rs`
- Modify: `src/backend/mod.rs`
- Modify: `src/lib.rs`

**Step 1: Write failing domain/indexer tests**

Cover:

- empty/overlong query validation
- result limit bounds
- idempotent duplicate indexing for the same repo/commit/root tree
- tree/blob traversal derives text rows from UTF-8 file blobs only
- binary/invalid UTF-8 files are skipped
- stale/missing/failed state returns a fixed fail-closed error
- candidate results are rechecked through durable read permissions before rendering
- debug/error strings omit file contents and secret-like material

Run:

```bash
cargo test --locked backend::search_index --lib -- --nocapture
```

Expected: FAIL because the module does not exist.

**Step 2: Implement the domain**

Add:

- `SearchIndexStore` trait with `index_commit`, `search`, `health_for_head`, and `available`.
- `InMemorySearchIndexStore` for provider-free route/capability tests.
- `UnavailableSearchIndexStore` default for local/default paths.
- `SearchIndexRequest`, `SearchIndexResult`, `SearchIndexHead`, and redacted error helpers.
- `index_durable_commit` helper that walks durable tree objects using existing object store semantics and writes rows through the trait.

Use commit/root tree identity as the freshness boundary. Do not read local `StratumDb` or `.vfs` state.

**Step 3: Verify**

```bash
cargo test --locked backend::search_index --lib -- --nocapture
```

Expected: PASS.

## Task 3: Postgres FTS Store

**Files:**
- Modify: `src/backend/postgres.rs`
- Modify: `src/server/mod.rs`

**Step 1: Write failing Postgres store tests**

Add optional/live-safe tests for:

- `PostgresMetadataStore` reports unavailable when schema 19 is missing.
- indexing commit A twice is idempotent and does not duplicate rows.
- indexing commit B for the same repo does not make commit A answer as fresh for B.
- ready state returns ranked bounded matches with snippets.
- failed/missing/stale state fails closed with redacted messages.

Run:

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected: FAIL before implementation, or skip live portions when no `STRATUM_POSTGRES_TEST_URL` is set.

**Step 2: Implement `SearchIndexStore` for `PostgresMetadataStore`**

Use parameterized SQL only:

- Upsert `search_index_state` to `indexing`.
- Delete old rows for the same `(repo, commit, root_tree)`.
- Insert file rows with `to_tsvector('simple', content_preview)`.
- Mark state `ready` after all rows are inserted.
- Mark state `failed` with a bounded failure code if indexing fails after state creation.
- Search with `websearch_to_tsquery('simple', $query)`, `ts_rank`, and bounded snippets through `ts_headline`.

Do not log SQL text or raw Postgres errors into public errors.

**Step 3: Wire stores**

Add a `search_index` shared store to `StratumStores`, `ServerStores`, and `ServerState`. In durable Postgres runtime, use `PostgresMetadataStore`; in normal in-memory/local builders use `UnavailableSearchIndexStore` unless a focused test injects `InMemorySearchIndexStore`.

**Step 4: Verify**

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo check --locked --features postgres
```

Expected: PASS, with live portions skipped unless configured.

## Task 4: Semantic Search Route And Capabilities

**Files:**
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_capabilities.rs`
- Modify: `src/server/mod.rs`
- Modify: `sdk/contracts/capabilities.v1.json`
- Modify: `sdk/contracts/capabilities.v1.durable-cloud.json`

**Step 1: Write failing route/capability tests**

Cover:

- local/default manifest keeps semantic search unavailable.
- durable-cloud manifest reports semantic search available only when the search index store is healthy for the current durable head.
- `/search/semantic` is mounted and returns `501` when the store is unavailable.
- missing/stale/failed index returns `503` and does not fall back to grep/find/local state.
- successful query returns projected paths, score, snippet, commit/root tree, and count.
- unreadable/private candidate paths are filtered or denied by existing committed reader permission checks.
- query/path/limit errors are bounded and redacted.

Run:

```bash
cargo test --locked server::routes_fs --lib -- --nocapture
cargo test --locked server::routes_capabilities --lib -- --nocapture
```

Expected: FAIL before route/capability wiring.

**Step 2: Implement route**

Add `search_semantic` beside `search_grep`/`search_find`. Resolve session, durable repo context, current durable ref/commit/root tree, optional path, query, and limit. Call the search store, then recheck each candidate with `state.core.cat_with_stat_as(candidate.path, &session)` before returning it.

Local/default unavailable behavior must be explicit. Durable-cloud must not open or traverse local state.

**Step 3: Implement capability logic**

Semantic search remains unavailable unless:

- server is durable-cloud,
- search index store reports available,
- current durable head has a ready index for the exact repo/commit/root tree.

Update capability fixtures with the existing fixture update command only after tests pass.

**Step 4: Verify**

```bash
cargo test --locked server::routes_fs --lib -- --nocapture
cargo test --locked server::routes_capabilities --lib -- --nocapture
STRATUM_UPDATE_CAPABILITY_FIXTURES=1 cargo test --locked server::routes_capabilities::tests::update_checked_in_sdk_contract_fixture_when_requested --lib -- --nocapture
```

Expected: PASS.

## Task 5: SDK And CLI Surface

**Files:**
- Modify: `sdk/typescript/src/types.ts`
- Modify: `sdk/typescript/src/client.ts`
- Modify: `sdk/typescript/tests/client.test.ts`
- Modify: `sdk/python/src/stratum_sdk/types.py`
- Modify: `sdk/python/src/stratum_sdk/client.py`
- Modify: Python SDK tests if present
- Modify: `sdk/bash/src/commands.ts`
- Modify: Bash SDK tests if present

**Step 1: Write failing SDK tests**

TypeScript and Python should call `GET /search/semantic` and type the response. Bash `sgrep` may keep returning unsupported unless the bash volume has a semantic method; update the unsupported wording to point to server capability rather than package absence.

Run:

```bash
cd sdk && bun run --cwd typescript test:run -- client.test.ts
cd sdk && bun run --cwd bash test:run
```

Expected: FAIL before SDK wrappers are added.

**Step 2: Add SDK types/wrappers**

Add `StratumSemanticSearchOptions`, `StratumSemanticSearchResult`, and match/result item types. Implement `SearchClient.semantic(query, options)` in TypeScript and Python as a normal server call; let server capability/HTTP status drive availability.

**Step 3: Verify**

```bash
cd sdk && bun run typecheck
cd sdk && bun run test:run
```

Expected: PASS.

## Task 6: Docs, Status, Review, And Final Gates

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/semantic-index.md`
- Modify: `docs/project-status.md`

**Step 1: Update docs**

Document route shape, fail-closed freshness model, query limits, capability semantics, SDK behavior, and out-of-scope items. Keep examples metadata-safe.

**Step 2: Run required focused verification**

```bash
git status --short --branch
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::committed_read --lib -- --nocapture
cargo test --locked server::routes_fs --lib -- --nocapture
cargo test --locked server::routes_capabilities --lib -- --nocapture
cargo test --locked server::repo_context --lib -- --nocapture
cargo test --locked server::middleware --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
cargo check --locked
cargo check --locked --features postgres
cargo test --locked --lib --tests
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cd sdk && bun run typecheck
cd sdk && bun run test:run
```

**Step 3: Review**

Perform a code/spec/security review before commit. Check specifically for stale fallback, leaked SQL/provider details, path scope bypass, snippets from unreadable files, raw DB URLs/env/token strings in errors, index freshness confusion, and SDK capability mismatch.

**Step 4: Commit and push**

Use a concise slice commit message after verification passes:

```bash
git add .
git commit -m "feat: add Postgres FTS search MVP"
git push origin v2/foundation
```
