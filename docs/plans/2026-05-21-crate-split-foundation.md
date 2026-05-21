# Crate Split Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Establish the first safe Stratum crate boundary by extracting shared core/domain types while preserving current server, CLI, MCP, FUSE, local-state, and durable-cloud behavior.

**Architecture:** Convert the repository into a Cargo workspace and add a small `stratum-core` crate for neutral identifiers, ref names, change-record wire types, and the shared error type. Keep the existing `stratum` crate as the application/runtime crate and provide compatibility re-exports from the existing module paths so this slice creates architectural leverage without a broad runtime move.

**Tech Stack:** Rust 2024, Cargo workspace resolver v3, Serde, SHA-256 object identifiers, existing Axum/Tokio server runtime, optional `postgres` and `fuser` features, existing route and startup tests.

---

## Required Skills And Agent Discipline

Implementation subagents must use:

- `superpowers:test-driven-development`
- `pragmatic-rust-guidelines`
- `rust-best-practices`

Review subagents must compare against this plan, the Slice 9 acceptance criteria, `docs/http-api-guide.md`, and `docs/project-status.md`.

The main session owns integration, local review, verification, commits, merge to `main`, and pushes. Subagents may implement or review scoped tasks, but the main session must inspect diffs and rerun gates before accepting work.

## Required Context Read

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/docs/plans/2026-05-15-backend-roadmap.md`
- `docs/project-status.md`
- `docs/http-api-guide.md`
- `docs/plans/2026-05-21-distributed-lock-service.md`
- `docs/plans/2026-05-20-operator-destructive-cleanup-controls.md`
- `docs/plans/2026-05-20-durable-cloud-default-gate-flip.md`
- `docs/plans/2026-05-20-pre-cutover-load-and-chaos-suite.md`
- `docs/plans/2026-05-19-recovery-scheduler-productionization.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`

## Current Module Graph Inventory

Current public crate root:

- `src/lib.rs` exposes `audit`, `auth`, `backend`, `client`, `cmd`, `config`, `db`, `error`, `fs`, `idempotency`, `io`, `persist`, `posix`, `remote`, `review`, `runs`, `secret_replay`, `server`, `store`, `vcs`, and `workspace`.
- Binaries (`stratum`, `stratum-server`, `stratum-mcp`, `stratumctl`, `stratum-mount`) all depend on the single `stratum` library crate.

Primary dependency direction:

- `server -> backend/auth/audit/db/fs/idempotency/review/runs/store/vcs/workspace`
- `backend -> store/vcs/fs/audit/idempotency/workspace/review/remote/secret_replay/auth`
- `vcs -> store/fs/error`, with durable helpers also depending on `backend::{ObjectStore, RepoId}`
- `workspace -> auth/backend::RepoId/vcs::{MAIN_REF, RefName}/error`
- `idempotency -> error` and `backend::RepoId`
- `audit -> auth::Session/Uid/error`

Cycle/coupling risks:

- `backend::durable_mutation` imports `server::policy`, while `server` imports backend heavily. This blocks extracting `backend` as a crate in Slice 9.
- `backend <-> vcs` is coupled through `CommitId`, `RefName`, `ChangedPath`, `ObjectStore`, and `RepoId`.
- `backend <-> workspace` is coupled through store bundles and `RepoId`.
- `backend <-> idempotency` is coupled through store traits and `RepoId`.
- Whole-module extraction of `audit`, `idempotency`, `workspace`, `backend`, or `server` would pull persistence, auth/session, policy, route, and storage details across the first boundary.

Feature and target inventory:

- Default package is one `stratum` package with five bins: `stratum`, `stratum-server`, `stratum-mcp`, `stratumctl`, and `stratum-mount`.
- `stratum-mount` is behind `required-features = ["fuser"]`.
- Optional `postgres` gates `tokio-postgres`, `deadpool-postgres`, `postgres-native-tls`, `native-tls`, `backend::postgres`, and `backend::postgres_migrations`.
- Optional `fuser` gates `src/fuse_mount.rs` and `src/bin/stratum_mount.rs`.
- Existing CI already has focused default, `postgres`, and `fuser` compile gates; this slice must keep those feature boundaries additive.

## Proposed Crate Graph

Slice 9 target:

```text
stratum-core
  ├── error: VfsError
  ├── object: ObjectId, ObjectKind
  ├── refs: MAIN_REF, CommitId, RefName, RefUpdateExpectation, VcsRef
  └── change: ChangeKind, PathKind, PathRecord, ChangedPath, StatusSummary

stratum
  ├── depends on stratum-core
  ├── keeps server/runtime/backend/fuse/cli/client modules
  ├── re-exports stratum-core types from existing paths
  └── remains the owner of storage traits, route behavior, runtime config, persistence, and bins
```

Follow-on target graph, not implemented in this slice:

```text
stratum-core -> stratum-backend -> stratum-server
             -> stratum-fuse
             -> stratum-cli
```

Why the first boundary is safe:

- The moved types are value/domain types with no route runtime, no Postgres/R2 clients, no local state files, no scheduler, no policy engine, and no object cleanup worker.
- Existing public paths remain available through re-exports: `stratum::error::VfsError`, `stratum::store::{ObjectId, ObjectKind}`, and `stratum::vcs::{CommitId, RefName, ChangedPath, ...}`.
- No durable-cloud route group is mounted or unmounted by this split.
- No advisory lock helper becomes public. Slice 8's Postgres helper remains internal to the application crate.
- No behavior changes are expected beyond Cargo package graph changes.

## Non-Goals

- Do not split `server`, `backend`, `fuse_mount`, `client`, `cmd`, `db`, `auth`, `workspace`, `audit`, or `idempotency` into separate crates in this slice.
- Do not change HTTP route availability, status codes, response bodies, durable-cloud unsupported `501` output, local-state persistence, recovery scheduling, audit semantics, idempotency semantics, or redaction policy.
- Do not publish crates to crates.io.
- Do not introduce Redis, new lock APIs, new runtime gates, new SDK packages, or sparse FUSE/cache work.
- Do not remove existing module paths that downstream code and tests use.

## Task 1: Save And Commit This Plan

**Files:**

- Create: `docs/plans/2026-05-21-crate-split-foundation.md`

**Step 1: Verify docs-only diff**

Run:

```bash
git diff -- docs/plans/2026-05-21-crate-split-foundation.md
git diff --check
```

Expected: only this plan doc is changed and whitespace is clean.

**Step 2: Commit**

Run:

```bash
git add docs/plans/2026-05-21-crate-split-foundation.md
git commit -m "docs: plan crate split foundation"
```

## Task 2: Add `stratum-core` Workspace Crate

**Files:**

- Modify: `Cargo.toml`
- Create: `crates/stratum-core/Cargo.toml`
- Create: `crates/stratum-core/src/lib.rs`
- Create: `crates/stratum-core/src/error.rs`
- Create: `crates/stratum-core/src/object.rs`
- Create: `crates/stratum-core/src/refs.rs`
- Create: `crates/stratum-core/src/change.rs`
- Modify: `src/error.rs`
- Modify: `src/store/mod.rs`
- Modify: `src/store/tree.rs`
- Modify: `src/vcs/refs.rs`
- Modify: `src/vcs/change.rs`
- Modify call sites that currently use `ChangeKind::status_code()`

**Step 1: Add failing compile target**

Run:

```bash
cargo check --locked -p stratum-core
```

Expected before implementation: Cargo reports that package `stratum-core` does not exist.

**Step 2: Create workspace and core crate**

Add a root workspace:

```toml
[workspace]
members = [".", "crates/stratum-core"]
resolver = "3"
```

Add `stratum-core` as a path dependency of the application crate.

The new crate owns:

- `VfsError` with the same `Display`, `Error`, and `From<std::io::Error>` behavior.
- `ObjectId` and `ObjectKind`, including existing SHA-256, hex, short-hex, raw-byte, `Debug`, `Display`, `Serialize`, and `Deserialize` behavior.
- `MAIN_REF`, `CommitId`, `RefName`, `RefUpdateExpectation`, and `VcsRef`, preserving current ref-name validation.
- `ChangeKind`, `PathKind`, `PathRecord`, `ChangedPath`, and `StatusSummary`.

Keep `ChangeKind::status_code()` out of the public core API. Add a crate-local helper in `src/vcs/change.rs`, for example `pub(crate) fn change_kind_status_code(kind: ChangeKind) -> &'static str`, and update internal status rendering to use it.

**Step 3: Add compatibility re-exports**

Keep existing import paths stable:

- `src/error.rs` re-exports `stratum_core::VfsError`.
- `src/store/mod.rs` re-exports `stratum_core::{ObjectId, ObjectKind}` and keeps `blob`, `commit`, `index`, and `tree`.
- `src/vcs/refs.rs` re-exports `stratum_core::{CommitId, MAIN_REF, RefName, RefUpdateExpectation, VcsRef}`.
- `src/vcs/change.rs` re-exports `stratum_core::{ChangeKind, ChangedPath, PathKind, PathRecord, StatusSummary}` and keeps path-map/diff helpers in the application crate.

Remove the orphan `Serialize`/`Deserialize` impls for `ObjectId` from `src/store/tree.rs`; those impls now live in `stratum-core`.

**Step 4: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo check --locked -p stratum-core
cargo check --locked
git diff --check
```

Commit:

```bash
git add Cargo.toml Cargo.lock crates/stratum-core src/error.rs src/store/mod.rs src/store/tree.rs src/vcs/refs.rs src/vcs/change.rs src/vcs/mod.rs src/server/core.rs
git commit -m "refactor: extract shared core types crate"
```

## Task 3: Document Crate Boundary And Status

**Files:**

- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md`

**Step 1: Update docs**

In `docs/project-status.md`, add a Slice 9 section after implementation that states:

- the repo is now a Cargo workspace with a `stratum-core` crate;
- completed extraction is limited to core/domain types;
- `stratum` remains the application/runtime crate for server, backend, CLI, MCP, FUSE, local persistence, durable-cloud routing, Postgres/R2 adapters, audit, idempotency, review, and workspace stores;
- follow-on splits remain future work;
- no fresh live-provider claims should be made unless actually verified.

In `docs/http-api-guide.md`, add a compact crate-boundary note near Backend Durability Status that:

- documents that this crate split does not change HTTP API behavior;
- reiterates durable-cloud unsupported surfaces remain stable `501`;
- states redaction and local-state behavior are unchanged.

**Step 2: Verify and commit**

Run:

```bash
rg -n "stratum-core|Crate Split|crate split" docs/project-status.md docs/http-api-guide.md
git diff --check
```

Commit:

```bash
git add docs/project-status.md docs/http-api-guide.md
git commit -m "docs: record crate split foundation"
```

## Task 4: Full Verification And Review

**Files:**

- No planned source changes beyond fixes from review.

**Step 1: Run required local gates**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo check --locked -p stratum-core
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

Expected: provider-free checks pass. Postgres/R2 live portions skip locally when credentials are absent. Do not claim protected-provider evidence unless inspected separately.

**Step 2: Review**

Run a spec/correctness review and then a code-quality/security review. Reviewers must verify:

- the implemented crate graph matches this plan;
- no route/runtime behavior changed;
- default, `postgres`, and `fuser` configurations still build;
- no feature flag leaked from `stratum` into `stratum-core`;
- no broad internal-only backend/server APIs became public stable API;
- public errors/status/log-adjacent output still redacts DB URLs, R2 endpoints, object keys, raw backend errors, commit messages, request bodies, idempotency keys, lease tokens, SQL, migration SQL, advisory lock ids, and secrets.

Fix blocking findings locally, rerun the affected gates, then run the final required gates before merge.

## Rollback Plan

If the split causes behavior regressions or unbounded compile churn:

1. Revert the implementation commit that adds `stratum-core` and compatibility re-exports.
2. Keep this plan doc as the record of the rejected boundary unless the implementation never reached commit.
3. Rerun `cargo check --locked`, the focused server/runtime tests, and `git diff --check`.
4. Leave the single-crate layout in place until a narrower follow-on plan is written.

## Follow-On Split Work

- Move CAS/ref store contracts only after `backend <-> server::policy` is untangled.
- Move audit domain records only after `AuditActor::from_session` and local persistence are separated.
- Move idempotency records only after HTTP header parsing and store/persistence logic are separated from the record model.
- Move workspace records only after auth/session/token lifecycle and repo-ref defaults are separated.
- Split `stratum-server`, `stratum-cli`, and `stratum-fuse` only after core/backend crate boundaries are compiling independently.
