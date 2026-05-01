# Change Requests And Protected Paths Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add the first local control-plane foundation for protected refs, protected path prefixes, and change requests so direct sensitive mutations can be blocked before full approval workflows exist.

**Architecture:** Add a small review/control-plane store beside the existing workspace, idempotency, and audit stores. Keep rules exact and conservative in this slice: protected ref rules match exact VCS ref names, protected path rules match normalized absolute path prefixes, and change-request merge is fast-forward-only through the existing ref compare-and-swap path. Wire HTTP routes after the model exists, and enforce protected rules in current HTTP mutation paths without claiming this is the future policy service.

**Tech Stack:** Rust, Axum 0.8, Serde, existing Stratum VCS refs, local file-backed metadata stores, HTTP idempotency helpers, and audit scaffolding.

---

## Task 1: Add Review Store Models

**Files:**
- Create: `src/review.rs`
- Modify: `src/lib.rs`
- Modify: `src/config.rs`
- Modify: `src/server/mod.rs`

**Requirements:**
- Define `ProtectedRefRule` with `id`, exact `ref_name`, `required_approvals`, `created_by`, and `active`.
- Define `ProtectedPathRule` with `id`, absolute normalized `path_prefix`, optional exact `target_ref`, `required_approvals`, `created_by`, and `active`.
- Define `ChangeRequest` with `id`, `title`, optional `description`, `source_ref`, `target_ref`, `base_commit`, `head_commit`, `status`, `created_by`, and monotonically increasing `version`.
- Status values for this slice: `open`, `merged`, `rejected`.
- Add `ReviewStore` trait plus `InMemoryReviewStore` and `LocalReviewStore`.
- Store path defaults to `<STRATUM_DATA_DIR>/.vfs/review.bin`, overrideable through `STRATUM_REVIEW_PATH`.
- Validate refs with existing `RefName`.
- Validate path prefixes as absolute, normalized, non-empty paths without `..`; `/legal` matches `/legal` and `/legal/draft.txt`, not `/legalese`.
- Local persistence must version the file and reject corrupt/duplicate records.
- `ServerState` owns a shared review store and `build_router` opens the local store by default.

**TDD steps:**
1. Add tests proving protected path prefix matching handles `/`, exact path, child path, and false prefix siblings.
2. Add tests proving invalid refs and invalid path prefixes are rejected.
3. Add tests proving in-memory create/list/get and status transitions increment versions.
4. Add local-store reload tests for rules and change requests.
5. Verify the focused tests fail before implementation:

```bash
cargo test --locked review::tests -- --nocapture
```

6. Implement the minimal models/store and server wiring.
7. Re-run:

```bash
cargo test --locked review::tests -- --nocapture
```

**Commit:**

```bash
git add src/review.rs src/lib.rs src/config.rs src/server/mod.rs
git commit -m "feat: add review control-plane store"
```

---

## Task 2: Add HTTP Contract And Conservative Enforcement

**Files:**
- Create: `src/server/routes_review.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/audit.rs`
- Modify: `docs/http-api-guide.md`

**Requirements:**
- Add admin-gated endpoints:
  - `GET /protected/refs`
  - `POST /protected/refs`
  - `GET /protected/paths`
  - `POST /protected/paths`
  - `GET /change-requests`
  - `POST /change-requests`
  - `GET /change-requests/{id}`
  - `POST /change-requests/{id}/reject`
  - `POST /change-requests/{id}/merge`
- All endpoints require admin-equivalent user auth and reject scoped workspace bearer sessions.
- Mutating endpoints support optional `Idempotency-Key` and never persist raw secrets or file contents.
- `POST /change-requests` validates source/target refs exist and captures source target as `head_commit` plus target target as `base_commit`.
- `POST /change-requests/{id}/merge` is fast-forward-only: it updates `target_ref` to `head_commit` only if the target ref still points to `base_commit` and the source ref still points to `head_commit`; then marks the CR `merged`.
- `POST /change-requests/{id}/reject` marks an open CR `rejected`.
- Protected ref rules block direct `POST /vcs/commit`, `POST /vcs/revert`, and `PATCH /vcs/refs/{name}` when the affected ref is protected.
- Protected path rules block direct HTTP filesystem writes, metadata patches, deletes, copies, and moves when any touched backing path matches an active protected path prefix.
- Change-request merge may update a protected target ref; direct ref routes may not.
- Emit audit events for protected rule creation and change-request create/reject/merge. Details stay metadata-only.
- Document request/response bodies, status transitions, protected enforcement, idempotency, and deferred approval/revert policy.

**TDD steps:**
1. Add route tests proving admins can create/list protected ref and path rules.
2. Add route tests proving non-admin and workspace bearer sessions are rejected.
3. Add route tests proving mutating endpoints replay with the same idempotency key and conflict with a different fingerprint.
4. Add route tests proving a protected ref blocks direct commit/revert/ref update but allows fast-forward CR merge.
5. Add route tests proving protected path rules block write, metadata patch, delete, copy destination, and move source/destination.
6. Verify focused tests fail before implementation:

```bash
cargo test --locked server::routes_review::tests -- --nocapture
cargo test --locked server::routes_vcs::tests::protected_ref_rules_block_direct_vcs_mutations -- --nocapture
cargo test --locked server::routes_fs::tests::protected_path_rules_block_direct_http_writes -- --nocapture
```

7. Implement handlers, enforcement helpers, audit events, and docs.
8. Re-run:

```bash
cargo test --locked server::routes_review::tests server::routes_vcs::tests server::routes_fs::tests -- --nocapture
```

**Commit:**

```bash
git add src/server/routes_review.rs src/server/mod.rs src/server/routes_fs.rs src/server/routes_vcs.rs src/audit.rs docs/http-api-guide.md
git commit -m "feat: add change request http contract"
```

---

## Task 3: Review Fixes, Status, And Verification

**Files:**
- Modify: `docs/project-status.md`
- Modify only files required by accepted review findings.

**Requirements:**
- Dispatch a spec/security reviewer focused on the CTO plan, API contract, auth boundaries, idempotency replay safety, protected-rule bypasses, and audit content.
- Dispatch a code-quality/correctness reviewer focused on concurrency, state transitions, path-prefix matching, local persistence migration, and test quality.
- Locally inspect all diffs and do not trust reviewer or worker output without verification.
- Fix accepted review findings in a separate commit.
- Update `docs/project-status.md` with the new completed slice, residual risks, exact verification commands, and current commit references.

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
git commit -m "fix: address change request review findings"
```
