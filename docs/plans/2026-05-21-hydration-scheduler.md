# Hydration Scheduler Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a bounded provider-free hydration scheduler foundation that can materialize durable commit/root-tree/ref-version identities into the local sparse cache without enabling sparse FUSE or durable mount behavior.

**Architecture:** Keep the foundation inside `src/sparse_cache` and build on the Slice 10 SQLite cache rather than routing production reads through it. Add durable-identity hydration jobs, bounded claim/progress/error APIs, and an async hydrator that reads tree/blob objects through existing `ObjectStore`/`CommitStore`/`RefStore` traits and writes cache rows in redacted SQLite operations. The scheduler is an inert library surface for tests and future sparse mount integration; HTTP routes, durable-cloud startup, local `.vfs/state.bin`, MCP, REPL, and `stratum-mount` are untouched.

**Tech Stack:** Rust 2024, Cargo workspace, `rusqlite` bundled SQLite, existing `stratum` backend traits, `TreeObject` decoding, `ObjectId`/`CommitId`/`RepoId`/`RefName` durable identities, `tokio` async tests, and provider-free in-memory durable stores.

---

## Required Skills And Agent Discipline

Implementation subagents must use:

- `superpowers:test-driven-development`
- `pragmatic-rust-guidelines`
- `rust-best-practices`

Review subagents must compare code against this plan, the Slice 11 acceptance criteria, `docs/project-status.md`, `docs/http-api-guide.md`, and the Slice 10 sparse-cache plan.

The main session owns integration, local review, final verification, commits, merge to `main`, and pushes. Subagents may implement or review scoped work, but the main session must inspect diffs and rerun gates before accepting work.

## Required Context Read

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/docs/plans/2026-05-15-backend-roadmap.md`
- `docs/project-status.md`
- `docs/http-api-guide.md`
- `docs/plans/2026-05-21-sparse-vfs-cache-schema.md`
- `src/sparse_cache/mod.rs`
- `src/sparse_cache/schema.sql`
- `src/backend/committed_read.rs`
- `src/backend/mod.rs`
- `src/store/tree.rs`
- `src/fs/inode.rs`
- `src/fuse_mount.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/cache/hydration.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/cache/db.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/cache/fs.rs`
- `/Users/rajattiwari/virtualfilesystem/smfs/crates/smfs-core/src/cache/schema.sql`

## Current Inventory

Sparse cache foundation:

- `sparse_cache_views` is keyed by durable view identity: `repo_id`, `root_tree_id`, optional `commit_id`, optional `ref_name`, and optional `ref_version`.
- `sparse_cache_inodes`, `sparse_cache_dentries`, `sparse_cache_chunks`, `sparse_cache_symlinks`, and `sparse_cache_statfs` already represent inode metadata, paths, immutable object chunks, symlink targets, statfs counters, hardlink-style multiple dentries, lookup/forget, and pruning.
- Public APIs already cover opening caches, inserting views, putting/listing inodes and dentries, putting/getting chunks, symlinks, statfs, and lookup lifecycle.
- Missing scheduler primitives are durable job rows, bounded claim/progress APIs, retry/backoff/poison state, redacted job error fields, and an atomic hydrator that writes tree rows and follow-on jobs.

Durable committed read identity:

- `DurableCommittedFsReader` carries `RepoId`, `RefStore`, `CommitStore`, and `ObjectStore`.
- Current roots resolve from mounted session ref or `MAIN_REF`, then load `CommitRecord.root_tree` and the root tree object.
- `load_tree` reads `ObjectKind::Tree`, decodes `TreeObject`, validates entry names, and checks stored repo/id/kind.
- `load_blob_bytes` and `object_len` read blobs by `RepoId`, `ObjectId`, and `ObjectKind::Blob`.
- Tree entries contain `name`, `kind`, `id`, `mode`, `uid`, `gid`, optional MIME type, and custom attrs.
- There is no durable chunk object abstraction yet; the sparse cache should chunk whole blob bytes into fixed `CHUNK_SIZE` rows for this foundation.

smfs lessons:

- Adapt bounded background work, dedupe, progress counters, and separation between scheduling and reconciliation.
- Reject path-primary latest-wins behavior, recent path TTL correctness, wall-clock dirty-vs-remote conflict checks, path-only dedupe, and completing failed hydration as if it succeeded.
- Stratum jobs must be scoped by durable view/object/ref-version identity; path is metadata for materialization, not the source of truth.

## Proposed Model

Add `src/sparse_cache/hydration.rs` and expose it from `src/sparse_cache/mod.rs`.

Extend `src/sparse_cache/schema.sql` with `sparse_cache_hydration_jobs`:

- `job_id INTEGER PRIMARY KEY`
- `view_id INTEGER NOT NULL`
- `scope TEXT NOT NULL CHECK scope IN ('tree', 'chunk')`
- `state TEXT NOT NULL CHECK state IN ('pending', 'running', 'completed', 'failed', 'backoff', 'poisoned')`
- `repo_id TEXT NOT NULL`
- `root_tree_id TEXT NOT NULL`
- `commit_id TEXT`
- `ref_name TEXT`
- `ref_version INTEGER`
- `object_id TEXT NOT NULL`
- `object_kind TEXT NOT NULL CHECK object_kind IN ('blob', 'tree')`
- `chunk_index INTEGER`
- `path TEXT NOT NULL`
- `attempts INTEGER NOT NULL DEFAULT 0`
- `next_run_at_unix_nanos INTEGER NOT NULL DEFAULT 0`
- `last_error_code TEXT`
- `created_at_unix_nanos INTEGER NOT NULL DEFAULT 0`
- `updated_at_unix_nanos INTEGER NOT NULL DEFAULT 0`

Use a nullable-safe unique key over `(view_id, scope, object_id, object_kind, COALESCE(chunk_index, -1), path)` so cache misses dedupe within one durable view. Tree hydration jobs must use `ObjectKind::Tree`; chunk hydration jobs must use `ObjectKind::Blob` and a non-null `chunk_index`.

Because Slice 10 schema version is `1`, bump the sparse cache schema version and add an internal schema migration from `1` to the new version. Version `999` or any unknown version must still fail with the fixed sparse-cache error.

## Scheduler API

Add public model types:

- `HydrationJobScope { Tree, Chunk }`
- `HydrationJobState { Pending, Running, Completed, Failed, Backoff, Poisoned }`
- `HydrationJobTarget`
- `HydrationJob`
- `HydrationProgress`
- `HydrationRunConfig`
- `HydrationRunSummary`

Add `SparseCache` methods:

- `enqueue_hydration_job(target, now_unix_nanos) -> Result<i64, VfsError>`
- `claim_hydration_jobs(limit, now_unix_nanos) -> Result<Vec<HydrationJob>, VfsError>`
- `complete_hydration_job(job_id, now_unix_nanos) -> Result<(), VfsError>`
- `fail_hydration_job(job_id, state, last_error_code, next_run_at_unix_nanos, now_unix_nanos) -> Result<(), VfsError>`
- `hydration_progress(view_id) -> Result<HydrationProgress, VfsError>`

State semantics:

- `pending` and due `backoff` jobs are claimable.
- `claim_hydration_jobs` moves at most `limit` jobs to `running`, increments `attempts`, and orders by `created_at_unix_nanos, job_id`.
- `complete_hydration_job` clears fixed error code and records `completed`.
- `fail_hydration_job` accepts only fixed safe error codes such as `tree_hydration_failed`, `chunk_hydration_failed`, and `hydration_poisoned`; it never stores raw backend/cache errors, SQL, paths, object keys, or bytes.
- `hydration_progress` returns bounded counts by state and total attempts for one view.

## Hydrator API

Add an async foundation function in `hydration.rs`:

- `hydrate_view_once(cache, stores, request, config) -> Result<HydrationRunSummary, VfsError>`

`HydrationViewRequest` should include:

- `repo_id`
- `root_tree_id`
- optional `commit_id`
- optional `ref_name`
- optional `ref_version`

Behavior:

- Insert or reuse the cache view for the durable identity.
- If `commit_id` is present, load the commit and verify `repo_id`, `commit_id`, and `root_tree_id` match before hydrating.
- If `ref_name` and `ref_version` are present, fetch the ref, verify the live ref version and commit/root tree still match the request, then hydrate that immutable snapshot identity. Do not silently follow a newer ref.
- Enqueue the root tree job for `/`.
- Claim at most `max_jobs_per_tick` jobs and hydrate only those jobs.
- Tree jobs read and decode a tree object, write the directory inode, child inodes, child dentries, symlink targets, statfs counters, and enqueue child tree jobs and first chunk jobs for blob/symlink blob objects.
- Chunk jobs read the blob bytes and write only the requested fixed-size chunk row keyed by `(repo_id, object_id, chunk_index)`.
- Repeated runs are idempotent: already completed rows and existing cache rows are reused/replaced with the same durable identity, and duplicate jobs do not multiply.
- All object-store, commit-store, ref-store, SQLite, serde, and tree decode failures collapse to fixed redacted errors/job codes.

This foundation may use deterministic synthetic inode ids derived from `(view_id, object_id, object_kind)` for object-backed entries and inode `1` for the root. If the same object appears under multiple dentries in one view, reuse the same inode and set `nlink` from the number of dentries referencing that object in the hydrated tree scope. Do not infer mutable ref visibility from path-only rows.

## Safety And Redaction

- Public cache/hydration errors must not expose DB URLs, R2 endpoints, object keys, raw backend errors, commit messages, request bodies, idempotency keys, lease tokens, SQL, migration SQL, advisory lock ids, cache SQL, raw paths, object ids, repo ids, symlink targets, chunk bytes, or secrets.
- Custom `Debug` for new public model types must redact repo ids, root tree ids, commit ids, object ids, raw paths, and error codes where needed.
- Tests must not require Postgres, R2, durable-cloud env, network, or live credentials.
- Do not call or alter durable mutation, idempotency, audit, recovery, object cleanup, HTTP routing, runtime selection, MCP, REPL, local `.vfs/state.bin`, or FUSE mount behavior.
- Preserve CAS/source-check/recovery/idempotency semantics by treating hydration as read-only cache population. It must never update refs, write commits, mutate object stores, or mark recovery/idempotency state.

## Rollback Boundary

Rollback is deleting `src/sparse_cache/hydration.rs`, removing the hydration job table/methods, and reverting docs/status updates. Because no runtime path is wired to this scheduler, rollback does not alter committed reads, HTTP behavior, durable-cloud startup, local `.vfs/state.bin`, `stratum-mount`, MCP, REPL, audit, idempotency, recovery, or object cleanup.

## Non-Goals

- Do not enable sparse FUSE mount execution.
- Do not change `stratum-mount`; it remains snapshot-only over `db.snapshot_fs()`.
- Do not enable durable-cloud FUSE, MCP, or REPL.
- Do not add NFS/macOS fallback or mount daemon UX.
- Do not route committed reads through SQLite.
- Do not implement write-back, mutation persistence, flush, commit staging, or durable ref updates from cache.
- Do not change durable-cloud defaults, HTTP route behavior, recovery/idempotency/audit semantics, or object cleanup semantics.
- Do not add Redis, KMS, auth login, workspace management, runs/audit serving, semantic search, execution runner, SDK releases, or crate publishing.

## Task 1: Save And Commit This Plan

**Files:**

- Create: `docs/plans/2026-05-21-hydration-scheduler.md`

**Step 1: Verify docs-only diff**

Run:

```bash
git diff -- docs/plans/2026-05-21-hydration-scheduler.md
git diff --check
```

Expected: only this plan doc is changed and whitespace is clean.

**Step 2: Commit**

Run:

```bash
git add docs/plans/2026-05-21-hydration-scheduler.md
git commit -m "docs: plan hydration scheduler"
```

## Task 2: Add Hydration Job Schema And State API

**Files:**

- Modify: `src/sparse_cache/mod.rs`
- Modify: `src/sparse_cache/schema.sql`

**Step 1: Write failing tests**

Add tests in `src/sparse_cache/mod.rs`:

- `hydration_jobs_dedupe_by_view_identity_scope_object_chunk_and_path`
- `hydration_claim_is_bounded_and_moves_due_jobs_to_running`
- `hydration_failures_record_fixed_redacted_codes_and_backoff`
- `schema_migrates_slice10_cache_to_hydration_schema`

Expected RED:

```bash
cargo test --locked sparse_cache::tests::hydration_jobs_dedupe_by_view_identity_scope_object_chunk_and_path --lib -- --exact --nocapture
```

**Step 2: Implement minimal schema/API**

Implement:

- Schema version migration from `1` to the new scheduler schema version.
- Hydration job table and domain constraints.
- Hydration job model/state/scope/target/progress types.
- `SparseCache` enqueue/claim/complete/fail/progress methods.
- Redacted `Debug` for new model types.

Keep invalid state/scope input as bounded `InvalidArgs` and SQLite failures as the fixed sparse-cache error.

**Step 3: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked sparse_cache::tests::hydration_jobs_dedupe_by_view_identity_scope_object_chunk_and_path --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests::hydration_claim_is_bounded_and_moves_due_jobs_to_running --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests::hydration_failures_record_fixed_redacted_codes_and_backoff --lib -- --exact --nocapture
cargo test --locked sparse_cache::tests::schema_migrates_slice10_cache_to_hydration_schema --lib -- --exact --nocapture
git diff --check
```

Commit:

```bash
git add src/sparse_cache/mod.rs src/sparse_cache/schema.sql
git commit -m "feat: add hydration scheduler job state"
```

## Task 3: Add Provider-Free View Hydration

**Files:**

- Modify: `src/sparse_cache/mod.rs`
- Create: `src/sparse_cache/hydration.rs`

**Step 1: Write failing tests**

Add async tests for:

- `hydrates_tree_entries_inodes_symlinks_and_statfs_from_durable_view`
- `hydrates_blob_chunks_by_repo_object_and_chunk_index`
- `hydration_rejects_stale_ref_version_without_following_latest_ref`
- `hydration_failures_are_redacted_and_mark_jobs_failed`

Expected RED:

```bash
cargo test --locked sparse_cache::hydration::tests::hydrates_tree_entries_inodes_symlinks_and_statfs_from_durable_view --lib -- --exact --nocapture
```

**Step 2: Implement minimal hydrator**

Implement:

- `HydrationViewRequest`
- `HydrationRunConfig`
- `HydrationRunSummary`
- `hydrate_view_once`
- Tree object loading/decoding using `ObjectStore`.
- Optional commit/ref-version verification using `CommitStore` and `RefStore`.
- Directory/file/symlink inode and dentry cache writes.
- Symlink target hydration from symlink blob bytes.
- One fixed-size chunk hydration per chunk job.
- Child tree and first chunk enqueue behavior.
- Statfs counter update for hydrated view rows.

Use `StratumStores::local_memory()` in tests and do not import network/provider clients.

**Step 3: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked sparse_cache::hydration::tests::hydrates_tree_entries_inodes_symlinks_and_statfs_from_durable_view --lib -- --exact --nocapture
cargo test --locked sparse_cache::hydration::tests::hydrates_blob_chunks_by_repo_object_and_chunk_index --lib -- --exact --nocapture
cargo test --locked sparse_cache::hydration::tests::hydration_rejects_stale_ref_version_without_following_latest_ref --lib -- --exact --nocapture
cargo test --locked sparse_cache::hydration::tests::hydration_failures_are_redacted_and_mark_jobs_failed --lib -- --exact --nocapture
git diff --check
```

Commit:

```bash
git add src/sparse_cache/mod.rs src/sparse_cache/hydration.rs
git commit -m "feat: hydrate sparse cache views"
```

## Task 4: Update Docs And Boundary Tests

**Files:**

- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md`
- Optionally modify tests only if existing boundary tests need assertion updates.

**Step 1: Write/update boundary assertions**

Confirm existing runtime/mount tests still prove:

- `stratum-mount` opens `db.snapshot_fs()`.
- Durable-cloud non-server surfaces fail closed.
- Durable-cloud HTTP route behavior is unchanged.

Add focused assertions only if current tests do not cover these boundaries after the new module is added.

**Step 2: Update docs**

Document:

- Slice 11 scheduler foundation is complete.
- Scheduler is library/test-only and not wired into committed reads, HTTP routes, durable-cloud startup, MCP, REPL, `.vfs/state.bin`, or `stratum-mount`.
- Sparse FUSE/read-through/mount daemon/write-back remain future work.
- Local live Postgres/R2 credentials are not required for the new hydration tests.

**Step 3: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked sparse_cache::tests --lib -- --nocapture
cargo test --locked sparse_cache::hydration::tests --lib -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
cargo check --locked --features fuser --bin stratum-mount
```

Commit:

```bash
git add docs/project-status.md docs/http-api-guide.md
git commit -m "docs: record hydration scheduler foundation"
```

## Task 5: Review And Full Verification

**Files:**

- No planned edits unless review finds issues.

**Step 1: Spec/correctness review**

Ask a review subagent to verify every Slice 11 acceptance criterion and non-goal against the diff. Fix Critical and Important findings, then re-review.

**Step 2: Code-quality/security review**

Ask a review subagent to inspect API shape, redaction, test quality, durability boundaries, and accidental runtime wiring. Fix Critical and Important findings, then re-review.

**Step 3: Final gates**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked sparse_cache::tests --lib -- --nocapture
cargo test --locked sparse_cache::hydration::tests --lib -- --nocapture
cargo check --locked -p stratum-core
cargo test --locked -p stratum-core
cargo check --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
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

**Step 4: Commit/merge/push**

After local verification and review fixes:

```bash
git status --short
git push origin v2/foundation
```

Use a temporary clean main worktree because the local main checkout is intentionally dirty. Merge `origin/v2/foundation` into clean `main`, push `main`, and leave the dirty local main checkout untouched.
