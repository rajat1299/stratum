# Durable Status/Diff/Revert Parity Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add guarded durable `/vcs/status`, `/vcs/diff`, and `/vcs/revert` parity for durable committed refs and mounted session refs without falling back to local `.vfs/state.bin`.

**Architecture:** Reuse Stratum's existing `ChangedPath` and `PathRecord` model, but build path maps from durable commit/root tree/object stores. Status and diff stay read-only over immutable root-tree identity; revert is a source-checked durable restore that writes a new commit and advances the target ref through durable ref CAS, with audit, idempotency, protected-policy, and recovery handling aligned with the existing guarded durable commit/FS mutation seams.

**Tech Stack:** Rust, Tokio, Axum route tests, Stratum durable `ObjectStore`/`CommitStore`/`RefStore`, existing local in-memory store adapters, optional Postgres feature gates.

---

## Reference Inputs

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/project-status.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`, section `Durable Status/Diff/Revert Parity Addendum`
- Local VCS behavior: `src/vcs/change.rs`, `src/vcs/diff.rs`, `src/vcs/revert.rs`
- Durable committed reads/mutations: `src/backend/committed_read.rs`, `src/backend/durable_mutation.rs`, `src/backend/core_transaction.rs`, `src/server/core.rs`
- Routes/tests: `src/server/routes_vcs.rs`

## MVP Revert Semantics

- Durable revert is a restore-to-commit operation, not text-hunk application.
- The request continues to accept `{"hash": "<target commit or prefix>"}` for local compatibility. In guarded durable mode the route resolves the prefix against durable commit metadata, observes the current target ref/version, and creates a new commit whose root tree equals the requested target commit root tree.
- The new revert commit has the current head as its single parent, author from the authenticated session, message `revert to <target short hash>`, and `changed_paths` computed between current head root and target root.
- The target ref is `main` for global admin calls. A future session-local revert can be added later; this slice does not invent a new public request shape.
- Source expectations are enforced internally: target ref name, expected current head commit, expected current ref version, requested target commit, target root tree, and resulting commit id/root tree are all checked before response.
- Protected ref and protected path policy gates run before mutation. Protected path policy uses durable changed paths between current head and requested target.
- If unresolved durable post-CAS, pre-visibility, or FS mutation recovery claims exist for the same repo/ref/session and could affect the target ref, durable revert returns `409 Conflict` with a redacted recovery message.
- Post-visible recovery intents are persisted before audit/idempotency/workspace-head side effects. Replay preserves the originally stored response kind/status.
- Unsupported subcases remain fail-closed with redacted errors.

## Task 1: Durable Status And Path Maps

**Files:**
- Modify: `src/vcs/change.rs`
- Modify: `src/backend/committed_read.rs`
- Modify: `src/server/core.rs`
- Test: `src/server/routes_vcs.rs`

**Step 1: Write failing durable status tests**

Add tests under `src/server/routes_vcs.rs` that seed durable `main` and a mounted session ref, then call `vcs_status` with no local VCS state.

Required assertions:
- guarded durable `/vcs/status` returns `200 OK`
- body includes existing status lines (`A`, `M`, `D`, `T`, `m`) for session changes
- body includes source identity lines for target ref, optional session ref, base/head commit ids, base/head root tree ids, and changed path count
- status path records include metadata-only changes without fetching blob bytes
- admin gate behavior remains unchanged for non-root/global reads

Run:

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_status --lib -- --nocapture
```

Expected: FAIL because guarded durable status still returns `durable mutable workspace route execution is not supported yet`.

**Step 2: Implement durable path-map primitives**

In `src/vcs/change.rs`, expose a durable tree walker that builds `PathMap` from `TreeObject` entries without reading blob bytes for file/symlink sizes. Use object metadata length through the durable object store when needed; do not hydrate blob bodies only to classify status.

In `src/backend/committed_read.rs` or a new small backend helper module, add read-only durable compare helpers:
- resolve base/head refs from `main` plus `Session::mount().session_ref()`
- load commit metadata and root tree ids
- build before/after `PathMap`
- return a typed summary carrying source identity and `Vec<ChangedPath>`

**Step 3: Wire guarded durable status**

Add `GuardedDurableCommitRoute::vcs_status_as` and `DurableCoreRuntime::durable_vcs_status_as`, then route `LocalCoreRuntime::vcs_status_as` through the capability in guarded durable mode.

Render local-compatible text first, with source identity appended in stable `key: value` lines. Keep local runtime rendering unchanged.

**Step 4: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked server::routes_vcs::tests::guarded_durable_status --lib -- --nocapture
git diff --check
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git add src/vcs/change.rs src/backend/committed_read.rs src/server/core.rs src/server/routes_vcs.rs
git commit -m "feat: add durable vcs status primitives"
```

## Task 2: Durable Diff Rendering

**Files:**
- Modify: `src/vcs/diff.rs`
- Modify: `src/backend/committed_read.rs`
- Modify: `src/server/core.rs`
- Test: `src/server/routes_vcs.rs`

**Step 1: Write failing durable diff tests**

Add durable route tests for:
- modified text file with grouped unified hunks
- added file
- deleted file
- metadata-only change
- binary or non-UTF-8 file
- oversized text file
- directory/file type change
- `?path=` exact and descendant filtering
- redacted internal durable failures that do not echo request path fragments

Run:

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_diff --lib -- --nocapture
```

Expected: FAIL because guarded durable diff still returns the fail-closed unsupported error or full-file equal-line rendering.

**Step 2: Refactor diff renderer for durable content sources**

In `src/vcs/diff.rs`, keep current public behavior for local calls but introduce a small content-provider abstraction or dedicated durable renderer that accepts before/after `PathRecord` plus bounded byte loaders.

Requirements:
- keep `MAX_TEXT_DIFF_BYTES` and `MAX_TEXT_DIFF_CELLS`
- fetch blob bytes only for selected file changes after path filtering and size/type checks
- reject non-UTF-8 as binary; do not use lossy decoding
- binary/oversized/type-change summaries include path, before/after object id when present, size, MIME/type marker, and stable reason text
- grouped unified hunks use limited context and do not render every equal line

**Step 3: Wire guarded durable diff**

Add `GuardedDurableCommitRoute::vcs_diff_as` and `DurableCoreRuntime::durable_vcs_diff_as`. Use the same durable compare source identity as status, but render diff text only for filtered changed paths.

**Step 4: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked vcs::diff --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable_diff --lib -- --nocapture
git diff --check
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git add src/vcs/diff.rs src/backend/committed_read.rs src/server/core.rs src/server/routes_vcs.rs
git commit -m "feat: render durable vcs diffs"
```

## Task 3: Durable Revert, Source Checks, And Recovery

**Files:**
- Modify: `src/server/core.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/backend/core_transaction.rs`
- Modify: `src/audit.rs` only if a missing audit response/recovery helper is truly needed
- Test: `src/server/routes_vcs.rs`

**Step 1: Write failing durable revert tests**

Add guarded durable tests for:
- successful source-checked revert creates a new durable commit and advances `main`
- response returns `reverted_to`, `revert_commit`, `target_ref`, `expected_head`, and `target_commit`
- idempotency replay returns the same status/body and does not create a second revert commit or audit event
- protected ref is blocked before mutation
- protected path is blocked using durable changed paths
- stale ref/version conflict returns `409 Conflict`
- unresolved recovery state for the target returns `409 Conflict` with redacted details
- audit append failure after visible mutation returns the existing committed/audit-failed response shape and stores replay when keyed

Run:

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_revert --lib -- --nocapture
```

Expected: FAIL because guarded durable revert still returns the mutable-workspace unsupported error.

**Step 2: Add durable restore commit builder**

Implement a durable restore helper in `src/server/core.rs` or `src/backend/core_transaction.rs` that:
- resolves target commit prefix through durable commit metadata
- observes current `main` ref target/version
- loads current and target root path maps
- computes changed paths
- writes a new `CommitRecord` with root tree equal to the target commit root
- uses `RefExpectation::Matches` with observed head/version to advance `main`
- fails with redacted durable metadata errors for store failures

Do not apply text patches and do not mutate local `StratumDb`.

**Step 3: Add route recovery/idempotency/audit coupling**

Route implementation must:
- keep local revert path unchanged
- run protected ref and durable protected-path checks before idempotency reservation where possible
- reserve idempotency using actor, workspace id, requested hash, observed expected head/version, and target commit identity
- enqueue post-visible recovery intents before audit/idempotency/workspace-head side effects, using existing durable post-CAS recovery patterns where possible
- complete or record recovery intents according to the same partial-response policy used by guarded durable commit
- return `409 Conflict` for unsafe unresolved recovery claims on the same target

**Step 4: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked server::routes_vcs::tests::guarded_durable_revert --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::revert --lib -- --nocapture
git diff --check
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git add src/server/core.rs src/server/routes_vcs.rs src/backend/core_transaction.rs src/audit.rs
git commit -m "feat: add durable vcs revert"
```

## Task 4: Route Coverage And Review Fixes

**Files:**
- Modify: `src/server/routes_vcs.rs`
- Modify: implementation files touched by review findings only

**Step 1: Replace old fail-closed durable status/diff/revert test**

Replace `guarded_durable_status_and_diff_routes_fail_closed_without_request_leaks` with positive durable coverage plus separate redaction tests for unsupported/internal durable errors.

**Step 2: Run focused VCS route suite**

Run:

```bash
cargo test --locked server::routes_vcs::tests --lib -- --nocapture
```

Expected: PASS.

**Step 3: Dispatch reviews**

Run spec/correctness review first, then code-quality/security review over the implemented range. Fix any Critical/Important findings locally and rerun focused tests.

**Step 4: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked server::routes_vcs::tests --lib -- --nocapture
git diff --check
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git add src/server/routes_vcs.rs src/server/core.rs src/backend/committed_read.rs src/vcs/change.rs src/vcs/diff.rs
git commit -m "test: cover durable vcs parity routes"
```

## Task 5: Docs And Status

**Files:**
- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md`

**Step 1: Update docs for landed behavior only**

Update `docs/project-status.md` with:
- durable status/diff/revert parity completed scope
- exact verification commands and warm perf numbers
- residuals: final object deletion/GC, broad durable runtime startup, full auth/session service, production observability

Update `docs/http-api-guide.md` only for behavior that actually landed.

**Step 2: Verify docs diff and commit**

Run:

```bash
git diff --check
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git add docs/project-status.md docs/http-api-guide.md
git commit -m "docs: record durable status diff revert parity"
```

## Final Verification

Run from `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation`:

```bash
cargo fmt --all -- --check
git diff --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked server::routes_vcs::tests --lib -- --nocapture
cargo test --locked --lib --tests
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Record warm perf `real`, `user`, `sys`, max RSS, and peak memory footprint in `docs/project-status.md`.

## Merge And Push

After all verification passes:

```bash
git status --short --branch
git push origin v2/foundation
```

Then in `/Users/rajattiwari/virtualfilesystem/lattice`, preserve unrelated local changes (`site/index.html`, `.claude/`), merge `v2/foundation` to `main`, rerun main gates, and push `main`.
