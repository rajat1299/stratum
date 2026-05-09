# Durable Mutations And Recovery Ops Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Persist durable filesystem mutations and metadata updates through durable session refs, promote them through guarded durable commit, and automatically drain bounded recovery/cleanup work without relying on local `.vfs/state.bin`.

**Architecture:** Add a durable mutable tree transaction layer over the existing commit/ref/object stores. Workspace-mounted durable writes advance a session ref with CAS-fenced immutable mutation commits; guarded durable commit promotes the session tree into the base ref with the user commit metadata. Durable idempotency, audit, post-visibility recovery, and object cleanup are tied to operation identity so visible mutations can be repaired after process restart. A bounded startup scheduler drains recovery queues, and an operator status route reports redacted queue health.

**Tech Stack:** Rust, Tokio, async-trait, Axum route tests, existing durable backend stores, Postgres migrations/adapters, durable object/tree/blob encodings, existing idempotency/audit stores, existing recovery claim patterns, workspace session mounts, and `/usr/bin/time -l` release perf measurement.

---

## Required Skills

Implementation and review subagents must use:

- `/Users/rajattiwari/.agents/skills/pragmatic-rust-guidelines/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-best-practices/SKILL.md`
- `/Users/rajattiwari/.agents/skills/rust-async-patterns/SKILL.md`

Use TDD for behavior changes: write the failing test, run it red, implement the smallest green change, then refactor.

## Subagent Operating Model

The main session owns planning, integration, local review, verification, commits, merges, and pushes. Workers implement scoped tasks with disjoint write ownership. The main session must inspect each worker diff locally, fix integration issues, rerun focused gates, then commit.

Implementation workers should be reused for review fixes in their own area during this slice. Review agents are separate and must not edit code unless explicitly reassigned.

## Context

Durable core reads now serve committed filesystem content from durable `RefStore`, `CommitStore`, and `ObjectStore` under the guarded durable backend path. Guarded durable `POST /vcs/commit` can write durable object/commit/ref state, but it still uses local `StratumDb` as the desired snapshot. Durable filesystem mutations, metadata updates, mutable workspace refs, automatic recovery scheduling, and object cleanup are not yet wired.

Existing FS routes already provide auth checks, mounted path resolution, protected path checks, route idempotency, mutation audit, and local mutation dispatch through `CoreDb`. The durable path must preserve that route contract while replacing local mutation state with durable session-ref state when a mounted durable workspace is present.

`extract pieces.md` contributes recovery vocabulary only: bounded claims, leases, backoff, poison, and status. Stratum recovery remains keyed by operation/ref/commit identity, not latest-wins mutable path state.

## Durable Mutable Workspace Contract

- Mounted durable workspace writes require a `session_ref`; if a mounted session has no `session_ref`, durable mutation routes fail closed with a clear unsupported-session message.
- The first mutation materializes `session_ref` from `base_ref` using source-checked durable ref creation: source `base_ref` must still point at the mounted base commit, and target `session_ref` must not already exist.
- Each mutation loads the current session-ref tree, applies one filesystem operation, writes new durable tree/blob objects and an internal mutation commit, then CAS-updates `session_ref` from the observed head to the new mutation commit.
- Mutation commits are durable implementation records on session refs. User-visible VCS history is produced by guarded durable commit, which creates a new user commit from the current session tree and CAS-updates the base ref, normally `main`.
- Guarded durable commit from a mounted durable session must use the durable `session_ref` tree as the desired target snapshot. It must no longer consult local `StratumDb` for that mounted durable session.
- Existing local runtime behavior remains unchanged when the guarded durable capability is absent.

## Scope

In scope:

- Durable `write_file`, `mkdir_p`, `rm`, `mv`, `cp`, and `set_metadata` for mounted durable workspace sessions.
- Durable read-after-write from the session ref for FS read/list/stat/tree/find/grep routes.
- Guarded durable commit promotion from session ref to base ref with CAS/fencing.
- Durable mutation idempotency and audit replay after visible partial completion.
- Recovery rows for post-visibility partials and bounded pre-visibility cleanup for unreferenced planned objects.
- Startup bounded recovery/cleanup scheduler for guarded durable stores.
- Admin/operator status for durable mutation recovery, existing guarded commit recovery, pre-visibility recovery, and object cleanup.
- Focused tests proving mutation and commit survive process restart with a fresh local `StratumDb`.
- Race tests proving stale session/base ref updates fail fenced instead of silently overwriting.

Out of scope:

- Full `STRATUM_CORE_RUNTIME=durable-cloud` route enablement.
- Durable mutation for unmounted root/admin sessions unless explicitly added after mounted-session behavior is correct.
- Distributed locks beyond durable ref CAS/source-check fencing.
- Durable revert.
- Semantic search/indexing.
- Sparse remote FUSE.
- Web console.
- Execution runner/sandbox.

## Design Constraints

- Reuse existing `TreeObject`, `TreeEntry`, blob object, `CommitRecord`, and ref update encodings.
- Do not introduce a second tree/blob format.
- Keep store/object/backend errors redacted; responses and logs must not include raw DB URLs, R2 keys, object keys, request bodies, authorization headers, idempotency tokens, or raw SQL details.
- Preserve route-level protected path checks and existing auth/session permission behavior.
- Keep unsupported durable methods explicitly fail-closed with fixed messages.
- Keep lock scopes short; do not hold async locks across object upload or route response serialization.
- Recovery workers must be bounded per tick and must use lease/fencing tokens before marking work complete, failed, or poisoned.
- Idempotency replay must return the same client-visible status/body for a completed mutation. A duplicate request must not create another mutation commit after the operation is visible.
- Audit records must be deterministic enough for recovery to append the intended audit event without including raw request bodies.

## Acceptance

- Durable mounted-session `PUT /fs`, directory `PUT /fs`, `DELETE /fs`, `POST /fs op=copy`, `POST /fs op=move`, and metadata `PATCH /fs` persist through durable stores.
- FS reads after mutations are served from durable session refs, not local `.vfs/state.bin`.
- Guarded durable commit promotes the durable session tree and the committed result survives process restart with a fresh local DB.
- Concurrent stale mutations or commit promotion races are rejected by CAS/source-check fencing and do not clobber newer durable refs.
- Visible partial mutations enqueue recovery; bounded automatic recovery drains audit/idempotency/workspace completion without manual route calls.
- Operator status reports redacted counts/rows for mutation recovery and cleanup.
- Existing guarded durable commit tests still pass.
- Existing local runtime behavior remains unchanged.

## Task 1: Plan Commit

**Files:**

- Create: `docs/plans/2026-05-09-durable-mutations-and-recovery-ops.md`

**Steps:**

1. Save this plan.
2. Run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

3. Commit:

```bash
git add docs/plans/2026-05-09-durable-mutations-and-recovery-ops.md
git commit -m "docs: plan durable mutations recovery ops"
```

## Task 2: Durable Mutable Tree Engine

**Worker ownership:**

- Create: `src/backend/durable_mutation.rs`
- Modify only as needed: `src/backend/mod.rs`, `src/vcs/change.rs`

**Tests to add first:**

- `write_file_creates_session_ref_from_base_with_source_check`
- `mkdir_delete_copy_move_and_metadata_update_session_tree`
- `stale_session_ref_update_is_fenced`
- `mutation_errors_are_redacted`

**Implementation shape:**

- Add a durable mutation engine that can:
  - resolve a base ref and optional session ref;
  - create a session ref from base with source checking;
  - load durable path records from an existing commit tree;
  - apply file write, mkdir, delete, copy, move, and metadata changes;
  - write new tree/blob objects using existing object encodings;
  - insert an internal mutation commit;
  - CAS-update the session ref from the observed head.
- Return structured mutation output: previous commit, new commit, changed paths, response metadata, and cleanup candidates.
- Keep all helpers private unless another module needs a narrow public contract.

**Focused verification:**

```bash
cargo test --locked backend::durable_mutation --lib -- --nocapture
```

## Task 3: Durable Mutation Recovery Ledger

**Worker ownership:**

- Modify: `src/backend/core_transaction.rs`
- Modify: `src/backend/mod.rs`
- Modify: `src/backend/postgres.rs`
- Create migration if needed under `migrations/postgres/`
- Modify: `src/backend/postgres_migrations.rs`
- Modify focused Postgres smoke SQL/tests only as needed

**Tests to add first:**

- `durable_fs_mutation_recovery_claims_are_fenced`
- `durable_fs_mutation_recovery_enqueue_is_idempotent`
- `visible_mutation_recovery_replays_audit_and_idempotency`
- `recovery_status_is_bounded_and_redacted`

**Implementation shape:**

- Add a durable mutation recovery target keyed by repo, workspace/session scope, operation id, target ref, previous commit, new commit, and failed step.
- Model pending, active lease, backing-off, completed, and poisoned states using the existing guarded commit recovery style.
- Store enough redacted envelope data to finish post-visible side effects:
  - idempotency scope/key and final response;
  - audit operation kind and changed paths;
  - optional workspace/session completion details.
- Do not store raw request bodies, auth headers, DB URLs, object credentials, or unbounded path lists.
- Add in-memory and Postgres implementations with lease owner/token fencing.

**Focused verification:**

```bash
cargo test --locked backend::core_transaction::tests::durable_fs_mutation_recovery --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

## Task 4: Wire Durable FS Routes And Core Runtime

**Worker ownership:**

- Modify: `src/server/core.rs`
- Modify: `src/server/routes_fs.rs`
- Modify route tests in `src/server/routes_fs.rs`

**Tests to add first:**

- `guarded_durable_write_read_survives_fresh_local_db`
- `guarded_durable_mkdir_delete_copy_move_metadata_survive_restart`
- `guarded_durable_duplicate_mutation_replays_idempotently`
- `guarded_durable_stale_mutation_returns_conflict`
- `guarded_durable_mutation_without_session_ref_fails_closed`

**Implementation shape:**

- Add guarded durable mutation methods on the route-facing core capability.
- Route mounted-session FS mutations to durable session-ref mutation when guarded durable stores are present.
- Keep local `StratumDb` mutation behavior unchanged outside the guarded durable path.
- Read durable session-ref content after a mounted durable mutation; continue reading committed base ref when no session ref exists.
- Complete route idempotency and audit through the durable recovery-aware path. If the mutation is visible but side-effect completion fails, enqueue recovery and return the existing committed/accepted partial response shape.

**Focused verification:**

```bash
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
```

## Task 5: Promote Session Ref Through Guarded Durable Commit

**Worker ownership:**

- Modify: `src/server/core.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify focused commit tests in `src/server/routes_vcs.rs`

**Tests to add first:**

- `guarded_durable_commit_uses_session_ref_snapshot`
- `guarded_durable_commit_after_mutations_survives_fresh_local_db`
- `guarded_durable_commit_base_ref_race_is_fenced`
- `guarded_durable_commit_keeps_existing_local_snapshot_path_for_non_mounted_calls`

**Implementation shape:**

- For mounted durable sessions, build guarded durable commit write plans from the durable session-ref tree.
- CAS the base ref from the session's mounted base commit to the new user commit.
- Update workspace metadata using existing workspace-head CAS where applicable.
- Record/repair audit and idempotency side effects using existing guarded commit recovery plus any new session completion step.
- Keep the prior local-`StratumDb` snapshot path for non-mounted guarded durable commit calls until a later slice explicitly removes it.

**Focused verification:**

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture
```

## Task 6: Automatic Recovery Scheduler And Operator Status

**Worker ownership:**

- Modify: `src/server/mod.rs`
- Modify: `src/server/routes_vcs.rs` or add a narrow operator route module if cleaner
- Modify scheduler/recovery wiring files only as needed

**Tests to add first:**

- `durable_recovery_scheduler_drains_visible_mutation_work`
- `operator_recovery_status_reports_mutation_and_cleanup_counts`
- `operator_recovery_status_redacts_sensitive_fields`

**Implementation shape:**

- Start a bounded recovery loop when guarded durable stores are configured.
- Each tick drains a small number of:
  - durable FS mutation recovery rows;
  - existing guarded commit post-CAS recovery rows;
  - pre-visibility recovery rows;
  - object cleanup claims/repairs.
- Use configurable but conservative interval and batch limits. Defaults must not spin in tests or production.
- Add an admin-only operator status response with aggregate counts and bounded rows.
- Ensure shutdown is cooperative and tests can drive a single tick deterministically.

**Focused verification:**

```bash
cargo test --locked server::routes_vcs::tests::recovery --lib -- --nocapture
cargo test --locked --test server_startup durable_env -- --nocapture
```

## Task 7: Docs And Status

**Files:**

- Modify: `docs/project-status.md`
- Optionally add focused notes under `docs/` if implementation creates a new operator route contract

**Steps:**

- Mark only landed behavior as complete.
- State that durable mounted-session FS mutations and commit promotion are supported behind the guarded durable backend path.
- State remaining gaps: broad durable runtime startup, durable revert, full policy propagation, sparse remote mount, ACL-aware indexing, web console.
- Record final perf numbers and any optimization made during the slice.

## Task 8: Review, Verification, Commit, Merge

**Review sequence:**

1. Main session inspects all worker diffs locally.
2. Run focused tests for each touched area.
3. Run spec/correctness review with a separate high-reasoning review agent.
4. Fix findings locally or send scoped fixes back to the owning worker.
5. Run code-quality/security review with a separate high-reasoning review agent.
6. Fix findings locally.

**Required final gates:**

```bash
cargo fmt --all -- --check
git diff --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Use a warm perf pass as the regression signal if the first release run rebuilds artifacts.

**Commit plan:**

- `docs: plan durable mutations recovery ops`
- `feat: add durable mutable tree mutations`
- `feat: add durable mutation recovery ledger`
- `feat: route durable filesystem mutations`
- `feat: commit durable session snapshots`
- `feat: schedule durable recovery ops`
- `docs: update durable mutation status`

Then push `v2/foundation`, merge to `main`, rerun main gates, and push `main`.
