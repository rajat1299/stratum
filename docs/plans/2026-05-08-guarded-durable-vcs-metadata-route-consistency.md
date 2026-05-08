# Guarded Durable VCS Metadata Route Consistency Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make guarded durable `POST /vcs/commit` results visible through the guarded VCS metadata routes that are safe to serve from durable commit/ref stores.

**Architecture:** Keep broad durable core runtime fail-closed and add route-level use of the existing guarded durable commit capability for metadata-only VCS reads and ref CAS mutations. `GET /vcs/log`, `GET /vcs/refs`, `POST /vcs/refs`, and `PATCH /vcs/refs/{name}` should use durable commit/ref stores when the guarded capability is present; filesystem reads, status, diff, revert, durable auth/session, and durable source routing stay out of scope.

**Tech Stack:** Rust, Axum, Tokio, existing `CoreDb` seam, existing `GuardedDurableCommitRoute`, durable `CommitStore`/`RefStore`, route tests, and release perf measurement with `/usr/bin/time -l`.

---

## Required Skills

Implementation and review subagents must use:

- `/Users/rajattiwari/.agents/skills/pragmatic-rust-guidelines/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-best-practices/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-async-patterns/SKILL.md`

Use TDD for behavior changes: write the failing test first, run it, then implement the smallest code that makes it pass.

## CTO Plan And Status Read

The CTO plan keeps Rust as the core for filesystem, VCS, content-addressed storage, and correctness-sensitive routing. Current code has a deliberately narrow guarded durable commit route: it sources the worktree from local `StratumDb`, then writes durable objects, commit metadata, `main` ref visibility, workspace head, audit, idempotency, and post-CAS recovery claims.

The live code now has an operator-visible split:

- guarded `POST /vcs/commit` can return a durable commit hash and move durable `main`;
- `GET /vcs/log` and `GET /vcs/refs` still read local VCS metadata through `LocalCoreRuntime`;
- `POST /vcs/refs` and `PATCH /vcs/refs/{name}` still mutate local VCS refs even though durable create/update-ref executors already exist.

This slice closes that metadata inconsistency without pretending durable filesystem serving is ready.

## Extract Pieces Guidance

Useful now:

- `extract pieces.md` capability guidance maps cleanly to this slice. Treat `commit.execute.guarded` and `refs.cas` as narrow live capabilities, while all broader durable FS/VCS serving remains failed-closed.
- Use the guidance's disabled/internal-only/live vocabulary in docs and tests: this is a live guarded metadata capability, not a broad durable core runtime cutover.

Not useful now:

- SMFS queue/claim/finalize patterns are for recovery workers, not this metadata read/control slice.
- Do not import SMFS latest-wins push queues, SQLite inode/chunk cache, or mutable path semantics.
- Mirage resource/search adapters are not durable commit/ref metadata stores.

## Scope

In scope:

- Add RED route tests proving guarded durable commits are visible through durable log and refs.
- Add RED route tests proving guarded durable ref create/update use durable `RefStore` and target durable `CommitStore`, not local VCS state.
- Add a small guarded metadata interface on `GuardedDurableCommitRoute`.
- Implement durable `list_refs` and `vcs_log_as` over `RefStore`/`CommitStore`.
- Reuse the existing durable create-ref and update-ref executor behavior instead of duplicating CAS and target-existence logic in routes.
- Preserve existing HTTP response shapes for log and refs.
- Preserve route-level admin gates, protected-ref checks, idempotency reservation/replay, and audit append behavior for ref create/update.
- Keep `STRATUM_CORE_RUNTIME=durable-cloud` startup fail-closed.
- Update `docs/project-status.md` after verification.

Out of scope:

- No durable filesystem/search/tree serving.
- No durable `GET /vcs/status`, `GET /vcs/diff`, `POST /vcs/revert`, or commit-object tree reconstruction for route reads.
- No durable auth/session path.
- No automatic background recovery scheduler.
- No new Postgres schema unless implementation proves it is necessary.
- No web UI, SDK change, or capability manifest endpoint.

## Design Constraints

- Do not route all `CoreDb` calls to `DurableCoreRuntime`; startup still rejects durable core serving.
- Guarded metadata routing must only activate when `state.core.guarded_durable_commit_route()` returns a capability.
- Durable log must return commit metadata in the existing HTTP shape: short hash, message, author, timestamp.
- Durable refs must return the existing `DbVcsRef`/`ref_json` shape: name, target, version.
- Non-admin users must still be rejected for global VCS metadata and ref management.
- Durable ref create/update must keep duplicate, stale CAS, missing target, and protected-ref behavior aligned with existing route status mapping.
- Error output must not leak raw backend SQL, store detail, tokens, R2 keys, or private request values.
- Keep the hot path allocation-light: list refs/commits once, map into response structs, avoid object-byte reads.

## Acceptance

- After guarded durable commit succeeds, `GET /vcs/log` returns that durable commit even though local `db.vcs_log()` remains empty.
- After guarded durable commit succeeds, `GET /vcs/refs` returns durable `main` pointing at that commit.
- `POST /vcs/refs` can create a durable session ref targeting a durable commit that does not exist in the local VCS log.
- `PATCH /vcs/refs/{name}` can update a durable ref to a newer durable commit using durable ref CAS semantics.
- Existing local/default route behavior is unchanged when guarded durable commit capability is absent.
- `status`, `diff`, and `revert` remain local/fail-closed for durable metadata and are documented as out of scope.
- Focused tests and release perf pass.

## Task 1: Plan Commit

**Files:**

- Create: `docs/plans/2026-05-08-guarded-durable-vcs-metadata-route-consistency.md`

**Steps:**

1. Save this plan.
2. Run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

3. Commit:

```bash
git add docs/plans/2026-05-08-guarded-durable-vcs-metadata-route-consistency.md
git commit -m "docs: plan guarded durable vcs metadata routes"
```

## Task 2: Add RED Route Consistency Tests

**Files:**

- Modify: `src/server/routes_vcs.rs`

**Tests to add first:**

- `guarded_durable_commit_log_and_refs_read_durable_metadata`
  - Create a local memory DB, write a file, create `StratumStores::local_memory()`, and build `guarded_durable_commit_state`.
  - Call `vcs_commit` and capture the returned durable hash.
  - Assert local `db.vcs_log().await.len() == 0`.
  - Call `vcs_log`; expect status `200`, first commit hash matches the durable hash prefix, message/author/timestamp are from durable metadata.
  - Call `vcs_list_refs`; expect durable `main` target equals the durable hash.

- `guarded_durable_ref_create_and_update_routes_use_durable_stores`
  - Create two guarded durable commits.
  - Call `vcs_create_ref` for `agent/root/session-1` targeting the first durable commit.
  - Assert status `201` even though local VCS has no such commit.
  - Call `vcs_update_ref` to move the session ref to the second durable commit with expected target/version.
  - Assert durable `RefStore` target/version updated and local `db.list_refs()` does not contain the session ref.

- `guarded_durable_vcs_log_keeps_admin_gate`
  - Add a non-admin local user.
  - With guarded durable capability present, call `vcs_log` as that user.
  - Expect `403 Forbidden`.

**Verification:**

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_commit_log_and_refs_read_durable_metadata --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable_ref_create_and_update_routes_use_durable_stores --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable_vcs_log_keeps_admin_gate --lib -- --nocapture
```

Expected before implementation: at least the log/refs and ref create/update tests fail because those routes still read or mutate local VCS state.

## Task 3: Add Guarded Durable Metadata Interface

**Files:**

- Modify: `src/server/core.rs`
- Modify: `src/server/routes_vcs.rs`

**Implementation shape:**

- Add methods on `GuardedDurableCommitRoute`:
  - `list_refs(&self) -> Result<Vec<DbVcsRef>, VfsError>`
  - `create_ref(&self, name: &str, target: &str) -> Result<DbVcsRef, VfsError>`
  - `update_ref(&self, name: &str, expected_target: &str, expected_version: u64, target: &str) -> Result<DbVcsRef, VfsError>`
  - `vcs_log_as(&self, session: &Session) -> Result<Vec<CommitObject>, VfsError>`
- Move durable create/update-ref logic behind private `DurableCoreRuntime` inherent helpers so the route capability and `CoreDb` trait implementation share one implementation.
- Implement durable `list_refs` by calling `RefStore::list` and mapping records to `DbVcsRef`.
- Implement durable `vcs_log_as` by enforcing the same admin semantics as local `vcs_log_as`, then mapping `CommitRecord` to `CommitObject`. Use the first parent if present because the current `CommitObject` route response is single-parent shaped and the HTTP log response does not expose parent IDs.
- Do not read object bytes or reconstruct trees.

**Verification:**

```bash
cargo fmt --all -- --check
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
```

## Task 4: Route VCS Metadata Through Guarded Capability

**Files:**

- Modify: `src/server/routes_vcs.rs`

**Implementation shape:**

- In `vcs_log`, after authentication, if guarded capability exists, call `capability.vcs_log_as(&session)` and render the existing log JSON response.
- In `vcs_list_refs`, after admin authorization, if guarded capability exists, call `capability.list_refs()`.
- In `vcs_create_ref`, after admin authorization and idempotency reservation, call guarded `create_ref` when capability exists; otherwise keep `state.core.create_ref`.
- In `vcs_update_ref`, after admin authorization, protected-ref check, and idempotency reservation, call guarded `update_ref` when capability exists; otherwise keep `state.core.update_ref`.
- Keep audit append and idempotency completion in the route exactly as they are today; in durable mode the route's `state.audit` and `state.idempotency` already point at guarded stores.
- Keep `vcs_status`, `vcs_diff`, and `vcs_revert` unchanged.

**Verification:**

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::admin_can_create_list_and_update_refs_over_http --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::non_admin_and_workspace_bearer_cannot_manage_refs --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
```

## Task 5: Focused And Full Verification

Run after code is green:

```bash
cargo fmt --all -- --check
cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::admin_can_create_list_and_update_refs_over_http --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::non_admin_and_workspace_bearer_cannot_manage_refs --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
cargo audit --deny warnings
git diff --check
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

If the first release perf run rebuilds artifacts, run a warm pass and record the warm pass as the regression signal.

## Task 6: Review, Docs, And Integration

**Files:**

- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md` only if route behavior documentation needs clarification.

**Steps:**

1. Run spec/correctness review with `gpt-5.5` high.
2. Fix findings locally.
3. Run code-quality/security review with `gpt-5.5` high.
4. Fix findings locally.
5. Rerun affected gates and release perf after meaningful diffs.
6. Update `docs/project-status.md` with:
   - guarded durable VCS log/refs consistency;
   - durable ref create/update under guarded capability;
   - explicit non-goals for status/diff/revert and durable FS/auth/session serving;
   - latest verification and perf.
7. Commit implementation/review fixes/docs in small commits.
8. Push `v2/foundation`, merge to `main`, rerun main gates, and push `main`.
