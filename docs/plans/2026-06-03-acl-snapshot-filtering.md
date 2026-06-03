# ACL Snapshot Filtering Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `GET /search/semantic` permission-correct by requiring every indexed file row to carry a verified ACL snapshot and by filtering search hits against the caller's session before any result is rendered.

**Architecture:** Extend the Slice 20 Postgres FTS index with additive ACL snapshot metadata and make ACL readiness part of search index health. Generate snapshots from the same durable tree entry metadata that committed reads enforce today: path scope, root/ancestor execute requirements, and file read requirements based on mode/uid/gid. Pass a normalized caller ACL filter into `SearchIndexStore::search`, push filtering into the Postgres query where practical, mirror the same predicate in the in-memory store, and preserve the final `cat_with_stat_as` candidate recheck as defense in depth.

**Tech Stack:** Rust 2024, Axum, Tokio, Postgres `jsonb` plus `tsvector`, existing `Session`/durable committed-read permission model, TypeScript and Python SDK contract checks, provider-free tests, and optional live Postgres tests behind `STRATUM_POSTGRES_TEST_URL`.

---

## Current Inventory

- `src/backend/search_index.rs` owns `SearchIndexHead`, `SearchIndexRequest`, `SearchIndexResult`, `IndexedFileRow`, `SearchIndexStore`, `InMemorySearchIndexStore`, `UnavailableSearchIndexStore`, and `index_durable_commit`.
- `src/backend/postgres.rs` implements `SearchIndexStore` for `PostgresMetadataStore` with tables from `migrations/postgres/0019_postgres_fts_search_mvp.sql`.
- `search_index_state` is keyed by `(repo_id, commit_id, root_tree_id)` and currently has only index lifecycle counters/status.
- `search_index_files` stores path, object id, byte length, bounded preview, `tsvector`, and timestamps. It does not store ACL metadata.
- `src/server/routes_fs.rs::search_semantic` resolves session, durable repo context, current durable head, query/path/limit, calls `state.search_index.search(...)`, then rechecks every returned candidate with `state.core.cat_with_stat_as(&candidate.path, &session)` and skips stale object-id mismatches.
- `src/backend/committed_read.rs` enforces durable read access through `Session::is_path_allowed`, root execute, ancestor execute, file read, mode/uid/gid bits, mounted path projection, and session-ref root selection.
- `src/auth/session.rs` already exposes mounted workspace identity, token id/version, read/write prefixes, hosted identity, uid/gid/groups, delegate intersection, and `has_permission_bits`.
- `src/server/middleware.rs` maps workspace bearer tokens to mounted sessions with repo/org, base/session ref, token id/version, principal uid, and normalized read/write prefixes.
- `src/server/repo_context.rs` resolves local singleton, workspace mount, hosted session, and admin header repo/tenant context.
- `src/server/routes_capabilities.rs` marks semantic search available only when durable-cloud and the current main head has a ready index.
- `docs/semantic-index.md` says search is derived, fail-closed, tied to exact head identity, and candidate hits are rechecked through committed read permission logic.

## ACL Snapshot Model

Use `posix-tree-v1` as the first snapshot version. It must model exactly the permissions that can be derived from durable tree entries today, without inventing the future policy service.

Per indexed file row, store:

- `acl_snapshot_version`: `posix-tree-v1`.
- `acl_snapshot_hash`: SHA-256/ObjectId-style hex digest over canonical snapshot bytes plus repo, commit, root tree, path, and object id.
- `acl_snapshot`: canonical JSON object with no secret material:

```json
{
  "version": "posix-tree-v1",
  "requirements": [
    {"path": "/", "access": "execute", "mode": 493, "uid": 0, "gid": 0},
    {"path": "/docs", "access": "execute", "mode": 493, "uid": 1000, "gid": 1000},
    {"path": "/docs/runbook.md", "access": "read", "mode": 420, "uid": 1000, "gid": 1000}
  ]
}
```

Rules:

- Root execute must be represented with the same synthetic durable root permissions currently used by `committed_read.rs`.
- Each ancestor directory must contribute an `execute` requirement.
- The indexed blob must contribute a `read` requirement.
- Symlinks are still skipped by the indexer in this slice.
- The snapshot must not include raw file contents, tokens, workspace secrets, object-store keys, backing provider paths, DB URLs, env vars, or raw SQL.
- Snapshot compatibility is strict. Unknown version, missing JSON, missing hash, malformed path, unsupported access, wrong repo/head, or mismatched hash fails closed.

Per query, derive a caller filter from `Session`:

- Principal uid, gid, groups, and optional delegate uid/gid/groups.
- Normalized read prefixes from `SessionScope`; use `["/"]` only when the session has no scope and the route context explicitly allows that identity.
- Mounted workspace metadata when present: workspace id, root path, base ref, session ref, repo id, principal uid, token id, and token version.
- Hosted identity metadata when present: session id, org id, repo id, uid, username.
- Ref identity used for this search: requested base ref or session ref, commit id, and root tree id.

The caller filter is verification metadata, not response data. Do not return it from the API.

## API Shape

Keep the public route and response shape stable:

```http
GET /search/semantic?query=<text>&path=<optional>&limit=<optional>
```

Success response remains:

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

Fail-closed behavior:

- `501`: search store unavailable for this runtime.
- `503`: index missing/stale/failed, index ready but ACL snapshot status missing/failed, file row snapshot absent/malformed, snapshot hash mismatch, unknown snapshot version, or unsupported ACL query scope.
- `403`: authenticated session is valid but not allowed to use the durable repo/search context.
- `400`: invalid query/path/limit.
- `401`: invalid or missing authentication.

Error bodies must remain generic and redacted; do not mention table names, SQLSTATE, raw snapshot JSON, token ids, read prefixes, denied paths outside the mounted projection, or secret-like material.

## Task 1: Migration And Schema Adoption

**Files:**
- Add: `migrations/postgres/0020_acl_snapshot_filtering.sql`
- Modify: `src/backend/postgres_migrations.rs`

**Step 1: Write failing migration registration tests**

Add a focused test that migration 20 is registered after `postgres_fts_search_mvp`, the catalog length increases by one, and the SQL is additive only.

Run:

```bash
cargo test --locked --features postgres backend::postgres_migrations::tests::acl_snapshot_filtering_migration_is_registered_and_non_destructive --lib -- --nocapture
```

Expected: FAIL because migration 20 is absent.

**Step 2: Add the additive migration**

Create migration 20:

```sql
ALTER TABLE search_index_state
  ADD COLUMN IF NOT EXISTS acl_snapshot_version TEXT,
  ADD COLUMN IF NOT EXISTS acl_snapshot_status TEXT NOT NULL DEFAULT 'missing'
    CONSTRAINT search_index_state_acl_snapshot_status_check
    CHECK (acl_snapshot_status IN ('missing', 'ready', 'failed')),
  ADD COLUMN IF NOT EXISTS acl_snapshot_failure_code TEXT
    CONSTRAINT search_index_state_acl_snapshot_failure_code_check
    CHECK (acl_snapshot_failure_code IS NULL OR acl_snapshot_failure_code <> '');

ALTER TABLE search_index_files
  ADD COLUMN IF NOT EXISTS acl_snapshot_version TEXT,
  ADD COLUMN IF NOT EXISTS acl_snapshot_hash TEXT
    CONSTRAINT search_index_files_acl_snapshot_hash_check
    CHECK (acl_snapshot_hash IS NULL OR acl_snapshot_hash ~ '^[0-9a-f]{64}$'),
  ADD COLUMN IF NOT EXISTS acl_snapshot JSONB;
```

Add lifecycle/check constraints so file-row ACL metadata is either fully absent or fully present, and so present snapshots must be JSON objects. Do not make old rows look ACL-ready. Existing Slice 20 ready indexes must become `acl_snapshot_status = 'missing'` and therefore fail closed until reindexed.

Add indexes only if tests show they are useful for the chosen predicate. Minimum expected shape:

- state lookup remains `(repo_id, commit_id, root_tree_id)`.
- file lookup remains `(repo_id, commit_id, root_tree_id, path)`.
- optional GIN on `acl_snapshot` only if the implementation uses JSONB pushdown.

**Step 3: Extend adoption verification**

Update `verify_known_schema_catalog` to require:

- New state columns and status/failure-code constraints.
- New file columns and hash/JSON/full-or-empty constraints.
- Migration ordering after 19.
- Negative adoption tests for missing ACL columns, weakened hash check, weakened status check, and JSON shape mismatch.

All adoption failure messages must remain generic and avoid table/column names in public `VfsError` rendering, following the existing search-index adoption tests.

**Step 4: Verify**

```bash
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
```

Expected: PASS; live Postgres cases skip unless `STRATUM_POSTGRES_TEST_URL` is set.

Commit:

```bash
git add migrations/postgres/0020_acl_snapshot_filtering.sql src/backend/postgres_migrations.rs
git commit -m "migration: add ACL snapshot search schema"
```

## Task 2: Provider-Free ACL Snapshot Domain

**Files:**
- Modify: `src/backend/search_index.rs`

**Step 1: Write failing ACL snapshot tests**

Add provider-free tests covering:

- `SearchAclSnapshot::posix_tree_v1` includes root execute, ancestor execute, and file read requirements.
- Snapshot hash changes when path, object id, mode, uid, gid, commit, or root tree changes.
- Malformed/unknown snapshot versions fail closed.
- Missing snapshot on an indexed row prevents search results.
- A local user with matching uid/group can search an allowed file.
- A local user without read bits gets no result before route rendering.
- Delegate sessions require both principal and delegate to pass.
- Session read prefixes filter `/docs/private.txt` when only `/docs/public` is allowed.
- Prefix matching is segment-safe (`/doc` must not match `/docs/file.txt`).

Run:

```bash
cargo test --locked backend::search_index --lib -- --nocapture
```

Expected: FAIL until ACL types and filtering are implemented.

**Step 2: Add ACL snapshot types**

Add small, serializable domain types:

- `SearchAclSnapshotVersion` with `PosixTreeV1`.
- `SearchAclAccess` with `Read` and `Execute`.
- `SearchAclRequirement { path, access, mode, uid, gid }`.
- `SearchAclSnapshot { version, requirements, hash }`.
- `SearchAclPrincipal { uid, gid, groups }`.
- `SearchAclFilter { principal, delegate, read_prefixes, identity_kind, ref_name, commit_id, root_tree_id }`.

Keep these types free of raw token values. If token identity is needed for compatibility checks, use token id/version metadata only and never render it in errors.

**Step 3: Add permission helpers**

Implement one shared predicate for in-memory search and Postgres row verification:

```text
allows(snapshot, filter, path) =
  path matches one read prefix
  AND every snapshot requirement passes for principal
  AND every snapshot requirement passes for delegate when delegate exists
```

Use the same mode/uid/gid bit semantics as `Session::has_permission_bits`:

- Root uid passes POSIX bits for that principal.
- Owner read/execute bits apply when uid matches.
- Group bits apply when any group matches gid.
- Other bits apply otherwise.
- Delegate intersection is strict.

**Step 4: Thread ACL through indexed rows and requests**

Extend:

- `IndexedFileRow` with `acl_snapshot: Option<SearchAclSnapshot>`.
- `SearchIndexRequest` with `acl_filter: SearchAclFilter`.
- `SearchIndexState` with `acl_snapshot_status` and `acl_snapshot_version`.

For compatibility during implementation, test helpers should deliberately use `None` to prove old rows fail closed.

**Step 5: Update `index_durable_commit` snapshot generation**

Carry ancestor ACL requirements during tree traversal:

- Start with synthetic root execute.
- Push directory execute requirements before descending.
- For each UTF-8 blob, create a file read requirement and attach a `posix-tree-v1` snapshot.
- Continue skipping binary/invalid UTF-8 files and symlinks.
- Keep invalid tree entry validation and UTF-8 truncation behavior unchanged.

**Step 6: Update `InMemorySearchIndexStore`**

The in-memory `search` implementation must:

- Validate query and limit as before.
- Treat missing, malformed, or incompatible snapshots as fail-closed for that row.
- Apply ACL filtering before returning `SearchIndexResult`.
- Truncate after filtering, not before.
- Preserve existing path-prefix behavior and stale/missing/failed index behavior.

**Step 7: Verify**

```bash
cargo test --locked backend::search_index --lib -- --nocapture
```

Expected: PASS.

Commit:

```bash
git add src/backend/search_index.rs
git commit -m "feat: add search ACL snapshot domain"
```

## Task 3: Postgres Store ACL Filtering

**Files:**
- Modify: `src/backend/postgres.rs`

**Step 1: Write failing Postgres tests**

Add optional/live-safe tests for:

- New indexes written by `index_commit` set `acl_snapshot_status = 'ready'`.
- Old-style ready state with `acl_snapshot_status = 'missing'` returns the same generic `503` path as not-ready search.
- Missing file-row snapshot fails closed and does not leak path/content/snapshot details.
- Snapshot hash mismatch fails closed.
- Local user allowed/denied by mode/uid/gid.
- Workspace bearer read prefixes allow `/workspace/docs/public.txt` and deny `/workspace/docs/private.txt`.
- Agent/mounted session ref uses the session head and does not answer from `main`.
- Wrong repo/workspace search returns no rows or a generic fail-closed error.
- Stale snapshot version fails closed.
- Denied high-rank rows do not consume the requested result `limit`.

Run:

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected: FAIL before Postgres implementation, or skip live portions when `STRATUM_POSTGRES_TEST_URL` is unset.

**Step 2: Update write path**

In `PostgresMetadataStore::index_commit`:

- Require every prepared file to have a valid ACL snapshot.
- Insert `acl_snapshot_version`, `acl_snapshot_hash`, and `acl_snapshot` into `search_index_files`.
- Set `search_index_state.acl_snapshot_version = 'posix-tree-v1'`.
- Set `search_index_state.acl_snapshot_status = 'ready'` only after all rows are written.
- On any ACL preparation/write failure, mark state failed with a bounded non-secret failure code.
- Preserve existing stale-ready hardening: failed reindex must hide previous ready rows.

**Step 3: Update search health**

`health_for_head` must return ACL status/version. `search` must require both:

- `SearchIndexStatus::Ready`.
- `AclSnapshotStatus::Ready` with version `posix-tree-v1`.

Any other status returns the same generic not-ready error used for missing/stale/failed search.

**Step 4: Implement ACL predicate**

Prefer SQL pushdown so denied rows do not consume result limits:

- Filter exact repo/commit/root tree before FTS.
- Filter query/path prefix as today.
- Filter caller read prefixes with segment-safe matching.
- Filter every JSONB ACL requirement against principal uid/groups and delegate uid/groups.
- Order by rank/path and apply limit after ACL filtering.

If JSONB predicate complexity becomes too high, use a bounded SQL helper function or typed columns. Do not implement unbounded Rust-side overfetch that can let denied high-rank rows hide allowed results.

All SQL must be parameterized. Do not include raw SQL, table names, snapshots, token ids, paths outside the projected route path, or Postgres errors in public errors.

**Step 5: Verify**

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo check --locked --features postgres
```

Expected: PASS, with live portions skipped unless configured.

Commit:

```bash
git add src/backend/postgres.rs
git commit -m "feat: filter Postgres search by ACL snapshots"
```

## Task 4: Route Integration And Session Identity

**Files:**
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/middleware.rs` only if route tests expose missing session metadata accessors
- Modify: `src/server/repo_context.rs` only if hosted user search cannot resolve repo context through existing helpers

**Step 1: Write failing route tests**

Add route tests for:

- Local/user session allowed result.
- Local/user session denied result.
- Workspace bearer allowed result.
- Workspace bearer denied result through read prefixes.
- Mounted session ref scope uses the mounted `session_ref` head.
- Hosted or mounted request with wrong repo header fails closed.
- Wrong workspace token/repo pairing fails closed.
- Missing ACL snapshot returns `503`.
- Stale ACL snapshot/version returns `503`.
- Index ready but `acl_snapshot_status = missing` returns `503`.
- Candidate recheck still suppresses stale object-id matches.
- Candidate recheck still suppresses stale path/object mismatch even after ACL filter passes.
- Error bodies do not include raw query content beyond normal validation, token ids, read prefixes, ACL JSON, SQL, object keys, env vars, or backing paths.

Run:

```bash
cargo test --locked server::routes_fs --lib -- --nocapture
```

Expected: FAIL before route integration.

**Step 2: Derive `SearchAclFilter` in the route**

In `search_semantic`:

- Resolve session as today.
- Resolve tenant/repo context before search.
- Resolve the current durable head for the correct `main` or mounted `session_ref`.
- Build a `SearchAclFilter` from the authenticated `Session` and resolved head.
- Reject unsupported session shapes with the generic search-index-not-ready response, unless the caller truly lacks repo/search permission, which should remain `403`.
- Pass `acl_filter` into `SearchIndexRequest`.

Do not return ACL metadata in the JSON response.

**Step 3: Preserve final candidate recheck**

Keep the existing post-search loop:

- Segment-safe path prefix check.
- `state.core.cat_with_stat_as(&candidate.path, &session)`.
- Object-id/content-hash equality check.
- Skip `NotFound` and `PermissionDenied`.
- Return generic errors for unexpected failures.

This is defense in depth and must not be removed even after ACL snapshots pass.

**Step 4: Verify**

```bash
cargo test --locked server::routes_fs --lib -- --nocapture
cargo test --locked server::middleware --lib -- --nocapture
cargo test --locked server::repo_context --lib -- --nocapture
```

Expected: PASS.

Commit:

```bash
git add src/server/routes_fs.rs src/server/middleware.rs src/server/repo_context.rs
git commit -m "feat: enforce ACL snapshots in semantic search route"
```

## Task 5: Capabilities, SDK Impact, And Docs

**Files:**
- Modify: `src/server/routes_capabilities.rs`
- Modify: `sdk/contracts/capabilities.v1.json` if fixture output changes
- Modify: `sdk/contracts/capabilities.v1.durable-cloud.json` if fixture output changes
- Modify: `docs/http-api-guide.md`
- Modify: `docs/semantic-index.md`
- Modify: `docs/project-status.md`

**Step 1: Write failing capability tests**

Cover:

- Durable-cloud semantic search remains unavailable when the FTS index is ready but ACL snapshot status is missing.
- Durable-cloud semantic search is available only when both index status and ACL snapshot status are ready for the exact current main head.
- Capability reasons are generic and do not expose schema/table/snapshot details.

Run:

```bash
cargo test --locked server::routes_capabilities --lib -- --nocapture
```

Expected: FAIL until capability health is ACL-aware.

**Step 2: Update capability logic**

`semantic_search_capability` must require:

- durable-cloud runtime,
- search index store availability,
- current main head resolution,
- `SearchIndexStatus::Ready`,
- `AclSnapshotStatus::Ready`,
- compatible ACL snapshot version.

Use a generic reason such as `search index not ACL-ready for current head`.

**Step 3: SDK impact assessment**

No SDK API shape change is expected. TypeScript and Python `search.semantic()` should continue calling the same route and surfacing server status. Do not add snapshot fields to SDK response types.

Still run SDK checks because capability fixtures or generated contracts may change:

```bash
cd sdk && bun run typecheck
cd sdk && bun run test:run
```

Python SDK checks are required only if Python files change:

```bash
cd sdk/python && python -m ruff check src tests
cd sdk/python && python -m mypy src/stratum_sdk
cd sdk/python && python -m pytest
```

**Step 4: Update docs**

Document:

- ACL snapshots are required for semantic search.
- Existing ready Slice 20 indexes without ACL snapshots fail closed until reindexed.
- Filtering happens before rendering and final committed-read recheck remains.
- Missing/stale/failed/unavailable index behavior remains unchanged.
- No SDK response fields expose ACL metadata.
- Automatic/background indexing remains out of scope.
- Rollback is to keep indexed search disabled for unsupported principals or ACL-missing heads.

**Step 5: Verify and update fixtures if needed**

```bash
cargo test --locked server::routes_capabilities --lib -- --nocapture
STRATUM_UPDATE_CAPABILITY_FIXTURES=1 cargo test --locked server::routes_capabilities::tests::update_checked_in_sdk_contract_fixture_when_requested --lib -- --nocapture
cd sdk && bun run typecheck
cd sdk && bun run test:run
```

Expected: PASS.

Commit:

```bash
git add src/server/routes_capabilities.rs sdk/contracts docs/http-api-guide.md docs/semantic-index.md docs/project-status.md
git commit -m "docs: document ACL-aware search filtering"
```

## Task 6: Full Review Gates

**Files:**
- No new files unless previous tasks uncovered narrowly required fixes.

**Step 1: Required verification**

Run:

```bash
git status --short --branch
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::search_index --lib -- --nocapture
cargo test --locked server::routes_fs --lib -- --nocapture
cargo test --locked server::routes_capabilities --lib -- --nocapture
cargo test --locked server::repo_context --lib -- --nocapture
cargo test --locked server::middleware --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo check --locked
cargo check --locked --features postgres
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cd sdk && bun run typecheck
cd sdk && bun run test:run
```

Expected: PASS. Postgres live tests may skip unless `STRATUM_POSTGRES_TEST_URL` is configured.

If Python SDK files changed, also run:

```bash
cd sdk/python && python -m ruff check src tests
cd sdk/python && python -m mypy src/stratum_sdk
cd sdk/python && python -m pytest
```

**Step 2: Security and privacy checks**

Search diffs for accidental leaks:

```bash
rg -n "postgres://|postgresql://|STRATUM_|token|secret|acl_snapshot|read_prefix|object_key|SQLSTATE" src docs sdk migrations
```

Expected: Only intentional code/docs references appear. Public error strings and fixtures must not include raw tokens, read prefixes, object keys, DB URLs, env vars, raw SQL, or ACL JSON.

**Step 3: Final status**

Run:

```bash
git status --short --branch
```

Expected: clean except intentional commits.

Commit any narrow verification/docs cleanup:

```bash
git add <files>
git commit -m "fix: harden ACL snapshot search review findings"
```

## Rollback Plan

- Leave migration 20 in place; it is additive.
- Set capability to unavailable when `acl_snapshot_status != ready`.
- Route should return the existing generic `503` for ACL-missing heads.
- Reindexing can repair ACL-missing rows without API changes.
- If a bug is found in snapshot filtering, disable semantic search for principals or heads without verified `posix-tree-v1` snapshots rather than falling back to route-only filtering.
- Never fall back to local `.vfs` state, grep/find traversal, stale index rows, or result rendering before ACL verification.

## Out Of Scope

- pgvector or embeddings.
- File extractors for docx/pdf/binary formats.
- Hosted search UI.
- Broad policy-service rewrite.
- Policy rules beyond durable tree POSIX mode/uid/gid plus session path scope.
- Replacing final committed-read candidate recheck.
- Publishing SDK packages.
- Live provider tests.
- Search over stale local state.
- Storing or returning raw ACL/token material.

## Final Review Prompts

Use these prompts after implementation, before merge.

### Gemini Implementation Review

Review Slice 21 ACL Snapshot Filtering for Stratum. Focus on correctness and security, not style. Verify that search results are filtered by ACL snapshots before route rendering for local/user sessions, workspace bearer tokens, and mounted agent/session refs. Confirm missing/stale/malformed snapshots, wrong repo/workspace, wrong head, unsupported session shapes, and failed ACL states all fail closed. Check that the final `cat_with_stat_as` candidate recheck still runs and still suppresses stale object/path matches. Look for leaks of raw tokens, read prefixes, ACL JSON, SQL, DB URLs, object keys, backing paths, env vars, or denied paths in responses, logs, docs, and tests. Challenge any duplicated permission logic against `committed_read.rs` and `Session::has_permission_bits`.

### Rust/Security Subagent Review

Audit the Rust implementation for permission predicate drift, path-prefix segment overmatch, delegate intersection mistakes, root handling mistakes, stale ready rows after failed reindex, malformed JSON/hash handling, and Postgres SQL predicate bugs. Confirm all SQL is parameterized and all public errors are redacted.

### Database Review

Review migration 20 for additive safety, adoption verification completeness, rollback behavior, and old Slice 20 index handling. Confirm old ready indexes become ACL-missing and cannot serve results until reindexed with snapshots.
