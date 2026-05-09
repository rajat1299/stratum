# Durable Visibility And Mutation Safety Repair Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the durable visibility and mutation safety blockers that can leave visible durable mutations unrecoverable, locally invisible writes accepted in guarded durable mode, weak session-ref promotion, or write-only workspace tokens denied before write authorization is evaluated.

**Architecture:** Keep the guarded durable path narrow and fail-closed. Record durable post-visible completion intents before any post-visible route side effects begin, use durable session refs for mounted mutations only, prove session-ref ancestry through commit parents, and add internal durable write preflight that does not depend on public read-scoped APIs.

**Tech Stack:** Rust, Tokio, Axum, existing `StratumStores`, durable object/commit/ref stores, existing post-CAS and durable FS mutation recovery stores, workspace/idempotency/audit stores, Postgres migrations/adapters where the recovery contract already has durable backing.

---

## Context

The latest backend slices moved committed reads and mounted-session FS mutations onto guarded durable stores, but the route still has four launch blockers:

- `src/server/routes_vcs.rs` enqueues post-CAS recovery only after a post-visible side effect fails.
- `src/server/routes_fs.rs` enqueues durable FS mutation recovery only after audit/idempotency failure, not before those side effects start.
- `src/server/core.rs` only routes durable mutable FS operations when `session.mount().is_some()`, so unmounted mutable FS calls can fall back to local `StratumDb` while reads are durable.
- `src/server/routes_vcs.rs::guarded_session_ref_descends_from` accepts same-root internal commits without requiring ancestry to the expected base.
- `src/server/core.rs` uses public `stat_as` for durable write preflight; `DurableCommittedFsReader::stat_as` requires `Access::Read`, so write-only scoped tokens can be blocked before write permission is checked.

Preserve default local runtime behavior when the guarded durable capability is absent. Keep client-visible durable errors redacted.

## Task 1: Post-Visible VCS Recovery Intent

**Files:**
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/backend/core_transaction.rs`
- Test: `src/server/routes_vcs.rs`

**Step 1: Write failing route tests**

Add tests under `server::routes_vcs::tests`:

- `guarded_durable_commit_records_post_visible_intents_before_workspace_side_effect`
- `guarded_durable_commit_post_visible_intent_failure_does_not_start_side_effects`

The first test should use a workspace store whose `update_head_commit_if_current` fails and a post-CAS recovery store that records enqueue calls. Assert recovery rows for the pending post-visible work exist before the first workspace-head attempt returns. The second should use a recovery store that fails on the initial intent enqueue and a workspace store counting calls; assert response is `500`, no workspace/audit/idempotency side effect ran, and no normal partial response is returned.

Run:

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_commit_records_post_visible_intents_before_workspace_side_effect --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable_commit_post_visible_intent_failure_does_not_start_side_effects --lib -- --nocapture
```

Expected: both fail because post-CAS recovery is currently enqueued only after failure.

**Step 2: Add pending-intent helper**

In `guarded_durable_commit_complete_post_cas`, build the `DurableCoreCommitPostCasEnvelope`, then enqueue contextual post-CAS recovery rows before `envelope.complete(...)` for:

- `WorkspaceHeadUpdate` when `workspace_id` is present.
- `AuditAppend` always.
- `IdempotencyCompletion` when an idempotency reservation is present.

Use `guarded_durable_commit_enqueue_post_cas_recovery` with full-commit idempotency kind for the initial idempotency row. If any enqueue fails, return `guarded_durable_commit_visibility_unconfirmed_response()` before side effects.

**Step 3: Mark no-op or complete through existing worker**

Do not invent a second ledger. Reuse existing post-CAS recovery rows as pending intents. Existing completion should either complete or leave rows for repair. The worker must remain idempotent if a row already completed its side effect or if the side effect was already applied by the route.

**Step 4: Verify focused tests**

Run:

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_repair_worker --lib -- --nocapture
```

**Step 5: Commit**

```bash
git add src/server/routes_vcs.rs src/backend/core_transaction.rs
git commit -m "fix: record post-visible durable commit intent"
```

**Step 6: Perf gate**

Run the warm perf command after the code diff:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Record real/user/sys, max RSS, and peak memory footprint.

## Task 2: Fail-Closed Mutable Routing

**Files:**
- Modify: `src/server/core.rs`
- Test: `src/server/core.rs`
- Test: `src/server/routes_fs.rs`

**Step 1: Write failing tests**

Add tests proving guarded durable mode rejects unmounted mutable FS operations:

- Runtime-level checks for `check_write_file_as`, `write_file_as`, `mkdir_p_as`, `rm_as`, `cp_as`, `mv_as`, and metadata update with an unmounted session.
- Route-level `PUT /fs/...` with user auth under guarded durable capability returns fail-closed and does not mutate local `StratumDb`.
- Mounted durable mutation still succeeds.

Run focused tests and confirm failure before code changes.

**Step 2: Change durable routing decision**

In `LocalCoreRuntime`, when `guarded_durable_commit_route` is present:

- Route mounted sessions with a session ref through `GuardedDurableCommitRoute`.
- Return `mutable_workspace_not_supported()` for mutable FS operations without a durable mounted session ref.
- Preserve default local behavior when `guarded_durable_commit_route` is absent.

Keep reads unchanged: guarded durable reads still use the committed durable reader.

**Step 3: Verify**

Run:

```bash
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
```

**Step 4: Commit and perf**

```bash
git add src/server/core.rs src/server/routes_fs.rs
git commit -m "fix: fail closed guarded durable fs mutations"
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

## Task 3: Session-Ref Ancestry Repair

**Files:**
- Modify: `src/server/routes_vcs.rs`
- Test: `src/server/routes_vcs.rs`

**Step 1: Write failing tests**

Add a test that creates:

- Expected durable `main` base commit `A`.
- A second commit `B` with the same root tree as `A`, message `DURABLE_MUTATION_COMMIT_MESSAGE`, but no parent path to `A`.
- Workspace session ref pointing to `B`.

Call guarded durable `POST /vcs/commit` with that workspace and assert `409 Conflict` / ref CAS mismatch. Also keep a positive test where the session ref descends through internal durable mutation commits to `A`.

Run the new negative test and confirm it fails under the current shortcut.

**Step 2: Remove same-root shortcut**

In `guarded_session_ref_descends_from`, remove the root-tree/message shortcut. Require one of:

- `current == expected_base`, or
- a bounded parent walk through single-parent internal durable mutation commits until `expected_base`.

Any non-internal message before reaching the base returns `false`; malformed parent counts return a redacted corrupt-store error; depth limit remains bounded.

**Step 3: Verify**

Run:

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture
```

**Step 4: Commit and perf**

```bash
git add src/server/routes_vcs.rs
git commit -m "fix: require durable session-ref ancestry"
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

## Task 4: Write-Scope Durable Preflight

**Files:**
- Modify: `src/backend/committed_read.rs`
- Modify: `src/server/core.rs`
- Modify: `src/backend/durable_mutation.rs` if route/runtime preflight and engine preflight must share helpers
- Test: `src/server/routes_fs.rs`
- Test: `src/backend/committed_read.rs` or `src/server/core.rs`

**Step 1: Write failing tests**

Add tests proving:

- A workspace bearer with write scope on `/demo` and no read scope can `PUT /fs/allowed.txt`.
- The same write-only token can `PUT` directory creation, `DELETE`, metadata patch, and `MOVE` within write scope when POSIX bits allow.
- Read/list/stat/tree/grep/find remain denied for the same token.

Run the write-only durable test and confirm it fails because route preflight uses read-scoped `stat_as`.

**Step 2: Add internal durable lookup/preflight**

Add an internal committed-tree lookup method that resolves path metadata without requiring `Access::Read` at the target:

- Keep `cat_with_stat_as`, `ls_as`, `stat_as`, `tree_as`, `find_as`, and `grep_as` read-scoped.
- Add a `pub(crate)` method for mutation preflight, such as `lookup_for_mutation_as(path, session)`, returning enough metadata for POSIX bit checks and symlink rejection.
- Traversal should require execute permission bits on parent directories and scope for the requested write/execute operation, but not read scope on the target.

Use this from `DurableCoreRuntime` write preflight helpers instead of public `stat_as` for write/delete/move/metadata/destination checks. Keep copy source read-scoped because copy reads source content.

**Step 3: Align engine preflight**

`DurableMutationEngine` already preflights against path records after source load. Ensure it does not require read scope for write/delete/move/metadata. Keep copy source read-scoped.

**Step 4: Verify**

Run:

```bash
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked backend::committed_read --lib -- --nocapture
cargo test --locked backend::durable_mutation --lib -- --nocapture
```

**Step 5: Commit and perf**

```bash
git add src/backend/committed_read.rs src/server/core.rs src/backend/durable_mutation.rs src/server/routes_fs.rs
git commit -m "fix: allow durable write preflight without read scope"
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

## Task 5: Durable FS Post-Visible Intent

**Files:**
- Modify: `src/server/routes_fs.rs`
- Modify: `src/backend/core_transaction.rs` if helper methods are needed
- Test: `src/server/routes_fs.rs`

**Step 1: Write failing tests**

Add tests proving:

- A visible durable FS mutation records recovery before audit append starts; use an audit store that fails and assert recovery exists.
- If recovery enqueue fails, route returns fail-closed with `durable FS mutation recovery is required`, leaves idempotency in progress, and does not attempt audit/idempotency side effects.
- Recovery survives route restart by running `POST /vcs/recovery/run` or direct worker over the same stores and completing audit/idempotency.

Run the first test and confirm the old code records recovery only after audit failure.

**Step 2: Enqueue intent before side effects**

After a durable mutation returns `DurableMutationOutput`, build the response body and recovery envelope before audit append. Enqueue durable FS mutation recovery rows for pending `AuditAppend` and `IdempotencyCompletion` before calling `append_audit` or completing idempotency.

Use the existing `DurableFsMutationRecoveryStore` as the intent ledger. The idempotency row should carry the intended success response body. For routes without idempotency, only enqueue the audit intent. If the initial enqueue fails, return fail-closed before side effects.

**Step 3: Keep worker idempotent**

Do not duplicate audit on retry. Existing worker tests already verify this; keep/extend them if the row state now starts before side effects.

**Step 4: Verify**

Run:

```bash
cargo test --locked server::routes_fs::tests::guarded_durable_fs --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_fs_mutation_recovery --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
```

**Step 5: Commit and perf**

```bash
git add src/server/routes_fs.rs src/backend/core_transaction.rs
git commit -m "fix: record durable fs mutation recovery intent"
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

## Task 6: Status Docs And Final Gates

**Files:**
- Modify: `docs/project-status.md`

**Step 1: Update status**

Add a concise section for Durable Visibility And Mutation Safety Repair. Include:

- What changed.
- What remains out of scope.
- Focused and full verification results.
- Warm perf real/user/sys, max RSS, and peak memory footprint.

Do not treat the “Recommended Next Slices” section as direction.

**Step 2: Commit docs**

```bash
git add docs/project-status.md
git commit -m "docs: record durable visibility mutation safety repair"
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

**Step 3: Required verification**

Run:

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

If the first release perf run rebuilds artifacts, run a warm pass and record the warm pass.

**Step 4: Review**

Run spec/correctness review, then code-quality/security review. Verify findings locally before applying. Fix valid blockers, rerun focused gates, and update the docs verification section if results change.

**Step 5: Push and merge**

```bash
git status --short --branch
git push origin v2/foundation
cd /Users/rajattiwari/virtualfilesystem/lattice
git status --short --branch
git fetch origin
git merge --ff-only origin/main || true
git merge --ff-only v2/foundation
cargo fmt --all -- --check
git diff --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --lib --tests
git push origin main
```

Preserve unrelated main worktree changes such as `site/index.html` and untracked `.claude/`.
