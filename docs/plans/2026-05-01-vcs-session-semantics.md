# VCS Session Semantics Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Tighten the v2 ref/session foundation by exposing admin-gated HTTP ref APIs, preserving compare-and-swap guarantees, and making session refs and workspace ref ownership explicit in the public contract.

**Architecture:** Reuse the existing `Vcs`, `RefName`, `RefUpdateExpectation`, persisted refs, and `StratumDb` ref methods. Add HTTP route handlers that authenticate through existing middleware and require admin-equivalent sessions before calling raw DB ref methods. Keep workspace metadata changes additive and migration-safe.

**Tech Stack:** Rust, Axum 0.8, Serde, existing Stratum VCS and workspace metadata stores.

---

## Task 1: Add Admin-Gated HTTP Ref API

**Files:**
- Modify: `src/server/routes_vcs.rs`
- Modify: `docs/http-api-guide.md`

**Requirements:**
- Add routes:
  - `GET /vcs/refs`
  - `POST /vcs/refs`
  - `PATCH /vcs/refs/{*name}`
- Require an authenticated admin-equivalent session for all ref endpoints.
- Reject scoped workspace bearer sessions for these global ref endpoints.
- `GET /vcs/refs` returns `{ "refs": [...] }`.
- `POST /vcs/refs` accepts `{ "name": "...", "target": "<64-char commit hex>" }` and returns `201 Created`.
- `PATCH /vcs/refs/{*name}` accepts `{ "target": "...", "expected_target": "...", "expected_version": 1 }`.
- Ref update must call existing CAS-backed DB methods and return `409 Conflict` on stale expectations.
- Duplicate ref creation must return `409 Conflict`.
- Unknown target commits should not mutate refs.
- Update docs with the request/response shapes and note that `agent/<actor>/<session>` is the session-ref namespace.

**TDD steps:**
1. Add route tests for admin list/create/update refs in `src/server/routes_vcs.rs`.
2. Add route tests for duplicate create and stale update returning `409 Conflict` without mutation.
3. Add route tests for non-admin user and workspace bearer sessions being rejected.
4. Run the focused route tests and verify they fail before implementation:

```bash
cargo test --locked server::routes_vcs::tests::admin_can_create_list_and_update_refs_over_http -- --nocapture
```

5. Implement the handlers and error mapping.
6. Re-run:

```bash
cargo test --locked server::routes_vcs::tests -- --nocapture
```

**Commit:**

```bash
git add src/server/routes_vcs.rs docs/http-api-guide.md
git commit -m "feat: expose vcs refs over http"
```

---

## Task 2: Make Workspace Ref Ownership Explicit

**Files:**
- Modify: `src/workspace/mod.rs`
- Modify: `src/auth/session.rs`
- Modify: `src/server/middleware.rs`
- Modify: `src/server/routes_workspace.rs`
- Modify: `docs/http-api-guide.md`

**Requirements:**
- Add migration-safe workspace metadata fields for:
  - `base_ref`, defaulting to `main`
  - `session_ref`, defaulting to `None`
- Return these fields from workspace create/get/list and token issuance responses.
- Add the same ref information to mounted workspace sessions so future VCS operations can distinguish the workspace root from the workspace ref.
- Do not change filesystem path scoping behavior.
- Preserve old local workspace metadata files through an explicit metadata version migration; do not rely on binary serde default-field behavior.

**TDD steps:**
1. Add workspace store tests proving new records default to `base_ref = "main"` and `session_ref = None`.
2. Add durable metadata reload tests proving the new fields survive save/reopen.
3. Add a legacy metadata decode test proving v2 records migrate to the new fields.
4. Add middleware tests proving a workspace bearer `SessionMount` exposes the workspace ref metadata.
5. Verify the new tests fail before implementation.
6. Implement the smallest migration-safe metadata version bump and session mount extension.
7. Re-run:

```bash
cargo test --locked workspace::tests middleware::tests routes_workspace::tests -- --nocapture
```

**Commit:**

```bash
git add src/workspace/mod.rs src/auth/session.rs src/server/middleware.rs src/server/routes_workspace.rs docs/http-api-guide.md
git commit -m "feat: record workspace ref ownership"
```

---

## Task 3: Review, Docs, And Verification

**Files:**
- Modify: `docs/project-status.md`
- Modify only files required by review findings.

**Requirements:**
- Dispatch a fresh spec reviewer against this plan and the CTO plan's ref/session requirements.
- Dispatch a fresh code quality/security reviewer focused on authorization, CAS correctness, error status mapping, metadata migration safety, and accidental workspace-token privilege expansion.
- Fix accepted review findings in a separate commit.
- Update `docs/project-status.md` to make this the latest completed slice and record verification.

**Verification:**

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

**Commit:**

```bash
git add <changed-files>
git commit -m "fix: address vcs session review findings"
```
