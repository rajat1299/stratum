# Approval Workflow Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Harden merge, reject, and revert semantics so terminal change requests cannot keep changing approval state, direct HTTP revert cannot bypass protected path rules, and merge freshness checks are enforced atomically with the target ref update.

**Architecture:** Keep this as a local/file-backed foundation slice. Use route-level idempotency replay before fresh terminal-state checks, store-level validation as the authoritative mutation guard, and existing protected-rule models for direct VCS revert checks.

**Tech Stack:** Rust, Axum route handlers, Tokio tests, local review store, Stratum VCS/ref APIs.

---

### Task 1: Terminal Change Request Mutation Guards

**Files:**
- Modify: `src/review.rs`
- Modify: `src/server/routes_review.rs`

**Steps:**
1. Add store-level validation that new approvals, approval dismissals, and new review comments only mutate open change requests.
2. Preserve persisted historical records on terminal change requests; only new mutations should be blocked.
3. Keep idempotency replay before terminal-state failure in HTTP routes so retries of already-recorded approval/comment/dismissal responses still replay after merge or reject.
4. Add route tests for:
   - approval creation rejected after terminal transition without audit mutation
   - approval dismissal rejected after terminal transition without audit mutation
   - review comment creation rejected after terminal transition without audit mutation
   - approval/comment/dismissal idempotency replay after terminal transition
   - merge idempotency replay after the change request is already merged

### Task 2: Atomic Merge Freshness

**Files:**
- Modify: `src/db.rs`
- Modify: `src/server/routes_review.rs`

**Steps:**
1. Add a DB helper that checks the source ref still points at the recorded `head_commit` under the same DB write lock used to compare-and-swap the target ref.
2. Use that helper from change-request merge instead of independently reading source/target before updating the target.
3. Preserve current stale source/target `409 Conflict` semantics and response bodies.
4. Add focused tests proving merge idempotency replays after success and stale source/target paths leave the change request open.

### Task 3: Protected Path Guard For Direct VCS Revert

**Files:**
- Modify: `src/vcs/mod.rs`
- Modify: `src/db.rs`
- Modify: `src/server/routes_vcs.rs`

**Steps:**
1. Add a read-only VCS helper that resolves a revert target prefix and returns paths changed between the current `main` head and the target commit when they are in the same ancestry line.
2. Before `POST /vcs/revert` reserves an idempotency key, reject the request if any active protected path rule matching `main` or all targets matches a path the revert would change.
3. Keep protected-ref checks before idempotency, matching the existing direct VCS mutation contract.
4. Add route tests proving protected path revert returns `403`, does not replay an idempotency key, leaves content/head unchanged, and emits no audit event.

### Task 4: Docs And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Steps:**
1. Document that approval/comment/dismissal mutations are limited to open change requests while matching idempotency replays still work.
2. Document that direct HTTP VCS revert is blocked when it would touch protected path rules for `main`.
3. Record the slice result and residual distributed-lock/backend limitations in project status.

### Verification

Run focused checks before commit:

```bash
cargo fmt --all -- --check
cargo test --locked server::routes_review::tests::approval_workflow -- --nocapture
cargo test --locked server::routes_vcs::tests::protected_path -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
git diff --check
```

Run the full gate before merge:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```
