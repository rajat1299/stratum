# HTTP Idempotency Coverage Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Extend the existing durable HTTP idempotency foundation beyond run creation so clients can safely retry filesystem writes, VCS commits/reverts, VCS ref mutations, and workspace creation without duplicating committed side effects.

**Architecture:** Keep the existing `src/idempotency.rs` store as the durable key/fingerprint/JSON-response layer. Extract route-agnostic HTTP helper functions into `src/server/idempotency.rs`, then wire each mutating route so it authenticates and authorizes before reservation, fingerprints endpoint semantics, aborts reservations for no-mutation failures, completes reservations for committed success/partial/audit-failure responses, and re-authorizes before replay. Workspace-token issuance is intentionally not replayed in this slice because replay would persist a raw `workspace_token` in the current plaintext local idempotency store.

**Tech Stack:** Rust, Axum, Tokio, serde JSON, existing Stratum idempotency/audit/workspace/VCS modules.

---

## Product Decision

`POST /workspaces/{id}/tokens` must not use the current JSON-response idempotency store because its success response contains a raw workspace token. Persisting that response would create a second raw-secret store. For now, reject `Idempotency-Key` on that endpoint with a clear `400 Bad Request` explaining that idempotent workspace-token issuance requires secret-aware replay storage.

This is the best production default until Stratum has KMS-backed encrypted replay records, response redaction with deterministic token derivation, or another explicit secret lifecycle design.

---

## Task 1: Shared HTTP Idempotency Helpers

**Files:**
- Create: `src/server/idempotency.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/server/routes_runs.rs`

**Requirements:**
- Move the route-local `Idempotency-Key` parsing and replay response logic from `routes_runs.rs` into `src/server/idempotency.rs`.
- Enforce exactly zero or one `Idempotency-Key`.
- Preserve existing key validation through `IdempotencyKey::parse_header_value`.
- Provide common JSON replay responses with `X-Stratum-Idempotent-Replay: true`.
- Provide common conflict responses:
  - `Idempotency-Key was reused with a different request`
  - `Idempotency-Key request is already in progress`
- Do not put authorization decisions in the helper.
- Update `POST /runs` to use the shared helper without changing behavior.

**Verification:**

```bash
cargo test --locked --lib routes_runs -- --nocapture
```

**Commit:**

```bash
git add src/server/idempotency.rs src/server/mod.rs src/server/routes_runs.rs
git commit -m "refactor: extract http idempotency helpers"
```

---

## Task 2: Filesystem Mutator Idempotency

**Files:**
- Modify: `src/server/routes_fs.rs`
- Modify: `docs/http-api-guide.md`

**Requirements:**
- Add optional `Idempotency-Key` support to:
  - `PUT /fs/{path}` for file writes and directory creation.
  - `DELETE /fs/{path}`.
  - `POST /fs/{path}?op=copy&dst=...`.
  - `POST /fs/{path}?op=move&dst=...`.
- Authenticate and resolve/authorize all affected paths before `begin`.
- Fingerprints must include route, effective actor UID/delegate identity, mounted workspace ID when present, resolved backing path(s), projected response path(s), operation, relevant query params, `x-stratum-type`, and for file writes a body SHA-256 plus byte length.
- Same key plus same fingerprint replays the original JSON response with replay header and emits no extra mutation audit event.
- Same key plus different fingerprint returns `409 Conflict` without mutation.
- Same key plus pending fingerprint returns `409 Conflict` without mutation.
- No-mutation failures abort the reservation.
- Audit-failure responses after committed mutations complete the reservation with the exact client-visible response.
- Add focused tests:
  - `PUT` file retry replays and does not append another audit event.
  - `PUT` same key with different body conflicts without overwriting.
  - `DELETE` retry replays instead of failing on already-deleted path.
  - `POST op=move` retry replays instead of failing on missing source.
  - replay is rejected when the current token/session no longer has write scope.

**Verification:**

```bash
cargo test --locked --lib routes_fs -- --nocapture
```

**Commit:**

```bash
git add src/server/routes_fs.rs docs/http-api-guide.md
git commit -m "feat: make filesystem mutations idempotent"
```

---

## Task 3: VCS Mutator Idempotency

**Files:**
- Modify: `src/server/routes_vcs.rs`
- Modify: `docs/http-api-guide.md`

**Requirements:**
- Add optional `Idempotency-Key` support to:
  - `POST /vcs/commit`
  - `POST /vcs/revert`
  - `POST /vcs/refs`
  - `PATCH /vcs/refs/{name}`
- Authenticate and authorize before reservation.
- Validate workspace header before reservation for commit/revert.
- Fingerprints must include route, effective actor UID/delegate identity, optional workspace ID, commit/revert/ref request data, ref path name, expected ref target, and expected ref version.
- Same key plus same fingerprint replays the original JSON response with replay header and emits no extra mutation audit event.
- Same key plus different fingerprint returns `409 Conflict` without mutation.
- CAS/no-mutation failures abort the reservation.
- Workspace-head partial failures and audit-failure responses after committed mutations complete the reservation with the exact client-visible response.
- Add focused tests:
  - `POST /vcs/refs` retry returns original `201` instead of duplicate-ref conflict.
  - `PATCH /vcs/refs/{name}` retry returns original updated ref instead of stale-CAS conflict.
  - `POST /vcs/commit` retry does not create a second commit or audit event.
  - `POST /vcs/revert` retry replays the first response.
  - same key with different request returns conflict without mutation.

**Verification:**

```bash
cargo test --locked --lib routes_vcs -- --nocapture
```

**Commit:**

```bash
git add src/server/routes_vcs.rs docs/http-api-guide.md
git commit -m "feat: make vcs mutations idempotent"
```

---

## Task 4: Workspace Creation And Token Endpoint Policy

**Files:**
- Modify: `src/server/routes_workspace.rs`
- Modify: `docs/http-api-guide.md`

**Requirements:**
- Add optional `Idempotency-Key` support to `POST /workspaces`.
- Authenticate and authorize admin before reservation.
- Fingerprint route, effective admin actor/delegate identity, workspace name, root path, defaulted `base_ref`, and optional `session_ref`.
- Same key plus same fingerprint replays the original `201` workspace JSON with replay header and emits no extra mutation audit event.
- Same key plus different fingerprint returns `409 Conflict` without mutation.
- No-mutation failures abort the reservation.
- Audit-failure responses after committed workspace creation complete the reservation with the exact client-visible response.
- Reject `Idempotency-Key` on `POST /workspaces/{id}/tokens` with `400 Bad Request`, before authenticating the raw `agent_token`, using a message that explains secret-aware replay storage is required.
- Add focused tests:
  - workspace creation retry returns the same workspace ID and does not append another audit event.
  - same key with different workspace body conflicts without creating a second workspace.
  - workspace creation audit failure completes the idempotency record and replays the committed-failure response.
  - workspace-token issuance with `Idempotency-Key` is rejected before token issuance.

**Verification:**

```bash
cargo test --locked --lib routes_workspace -- --nocapture
```

**Commit:**

```bash
git add src/server/routes_workspace.rs docs/http-api-guide.md
git commit -m "feat: make workspace creation idempotent"
```

---

## Task 5: Docs, Status, And Review Fixes

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`
- Modify implementation files only for review findings.

**Requirements:**
- Document the shared idempotency contract for supported mutating endpoints.
- Document unsupported workspace-token idempotency and why raw-token replay is not enabled.
- Update `docs/project-status.md` with the new slice, remaining risks, and next recommended work.
- Dispatch a spec/security reviewer focused on auth-before-reservation, replay authorization, fingerprint coverage, audit/idempotency interaction, and secret handling.
- Dispatch a quality/correctness reviewer focused on helper design, route behavior, tests, and error-path classification.
- Fix reviewer findings in a separate commit.

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
git add docs/http-api-guide.md docs/project-status.md src/server/*.rs
git commit -m "fix: address idempotency coverage review findings"
```
