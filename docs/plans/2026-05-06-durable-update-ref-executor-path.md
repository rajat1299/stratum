# Durable Update-Ref Executor Path Implementation Plan

> **For Codex/subagents:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development and follow `/Users/rajattiwari/.agents/skills/pragmatic-rust-guidelines/SKILL.md`, `/Users/rajattiwari/.agents/skills/rust-best-practices/SKILL.md`, and `/Users/rajattiwari/.agents/skills/rust-async-patterns/SKILL.md`.

**Goal:** Add the first narrow durable `CoreDb` executor path for the commit-oriented update-ref route while keeping durable HTTP serving and all broader filesystem/VCS execution fail-closed.

**Architecture:** Implement `DurableCoreRuntime::update_ref` over the existing durable commit and ref store contracts. The method validates ref names and commit IDs with redacted durable errors, checks the current ref expectation before target-commit validation to preserve existing CAS semantics, then performs the ref compare-and-swap through `RefStore`. It does not create commits, write object bytes, update workspaces, append audit records, or complete idempotency records. `STRATUM_CORE_RUNTIME=durable-cloud` remains startup fail-closed, so this is an internal route-facing executor method behind the seam, not live durable HTTP serving.

**Tech Stack:** Rust, Axum server core seam, existing backend `CommitStore` and `RefStore` traits, durable transaction semantics, Tokio tests, cargo release perf gates with `/usr/bin/time -l`.

---

## Scope

In scope:

- Enable exactly one durable route-facing executor method: `CoreDb::update_ref` on `DurableCoreRuntime`, the method behind `PATCH /vcs/refs/{name}`.
- Validate ref names, expected target commit IDs, and new target commit IDs without echoing raw invalid request values from the durable path.
- Load the current ref and verify `(expected_target, expected_version)` before checking the new target commit metadata, matching the existing local VCS rule that unknown targets are reported only after the CAS expectation is satisfied.
- Require the new target commit metadata to exist in `stores.commits`.
- Use `stores.refs.update` with `RefExpectation::Matches`.
- Return the existing `DbVcsRef` response shape.
- Preserve stale-CAS no-mutation behavior.
- Keep `route_execution_enabled()` false for broad durable route serving until startup/auth are enabled.
- Keep `login`, `authenticate_token`, filesystem/search/tree, `list_refs`, `create_ref`, `commit_as`, log/status/diff/revert fail-closed.
- Update `docs/http-api-guide.md` and `docs/project-status.md`.

Out of scope:

- No live startup support for `STRATUM_CORE_RUNTIME=durable-cloud`.
- No HTTP durable serving test that bypasses auth; auth still fails closed in durable core.
- No `POST /vcs/commit` executor, object-byte upload, tree construction, commit metadata insertion, workspace-head update, audit append, idempotency completion, distributed locking, or background repair worker.
- No Postgres/R2 route wiring, connection pooling, TLS/KMS/secrets hardening, or hosted runtime cutover.
- No broad change to local `StratumDb` route behavior.

## Performance Rules For This Slice

- After each meaningful code or docs diff, run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

- Record warm wall/user/sys time, maximum resident set size, and peak memory footprint from `/usr/bin/time -l`.
- Keep the executor path metadata-only: one current-ref read, one target-commit metadata lookup after CAS expectation passes, one ref CAS update, and no object-byte or tree loads.
- Clone only small ref/commit IDs and names required by store trait ownership.
- Do not add background tasks, caches, or eager local `StratumDb` allocation to the durable runtime.
- GPU efficiency is not applicable to this backend storage path unless a later semantic-indexing slice introduces GPU-backed work.

## Task 1: Add Durable Update-Ref Red Tests

**Files:**

- Modify: `src/server/core.rs`

**Step 1: Write failing tests**

Add focused tests in `server::core::tests::durable_core_runtime`:

- `update_ref_rejects_invalid_target_without_leaking_raw_value`
  - constructs `DurableCoreRuntime::new(RepoId::local(), StratumStores::local_memory())`.
  - calls `update_ref("agent/alice/session-1", valid_expected_hex, 1, "target-private-token")`.
  - asserts `VfsError::InvalidArgs`.
  - asserts the message says the ref target commit id is invalid.
  - asserts the message does not contain `target-private-token`, `private-token`, `STRATUM_CORE_RUNTIME`, or `durable-cloud`.
- `update_ref_rejects_invalid_expected_target_without_leaking_raw_value`
  - calls `update_ref("agent/alice/session-1", "expected-private-token", 1, valid_target_hex)`.
  - asserts `VfsError::InvalidArgs`.
  - asserts the message says the expected ref target commit id is invalid and does not echo the raw value.
- `update_ref_rejects_stale_expectation_without_mutation`
  - seeds commit A, commit B, and an existing ref pointing at commit A version 1.
  - calls durable `update_ref` with expected target B and target B.
  - asserts a compare-and-swap conflict-style `InvalidArgs`.
  - asserts the error does not echo the ref name.
  - asserts the underlying ref still points at commit A version 1.
- `update_ref_rejects_missing_target_after_expectation_without_mutation`
  - seeds commit A and an existing ref pointing at commit A version 1.
  - calls durable `update_ref` with expected target A, expected version 1, and a valid but missing target commit ID.
  - asserts `VfsError::ObjectNotFound`.
  - asserts the underlying ref still points at commit A version 1.
- `update_ref_updates_existing_ref_for_existing_commit`
  - seeds commit A, commit B, and an existing ref pointing at commit A version 1.
  - calls durable `update_ref` with expected target A, expected version 1, and target B.
  - asserts returned `DbVcsRef { name, target: B, version: 2 }`.
  - asserts the underlying `stores.refs.get` row matches target B and version 2.

Update the existing `route_methods_fail_closed` test so it no longer expects `update_ref` to fail closed. Keep representative fail-closed checks for auth, filesystem, `list_refs`, `create_ref`, and `commit_as`, and preserve redaction assertions.

**Step 2: Verify RED**

Run:

```bash
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
```

Expected before implementation: the new update-ref tests fail because durable `update_ref` still returns `NotSupported`.

## Task 2: Implement Durable Update-Ref Executor

**Files:**

- Modify: `src/server/core.rs`

**Step 1: Add narrow helpers**

Add private helpers near the durable runtime implementation:

- `parse_durable_ref_name(name: &str) -> Result<RefName, VfsError>`
  - wraps `RefName::new`.
  - maps validation failures to `VfsError::InvalidArgs { message: "invalid ref name".to_string() }`.
- `parse_durable_commit_id(value: &str, label: &'static str) -> Result<CommitId, VfsError>`
  - wraps `ObjectId::from_hex`.
  - maps parse failures to `VfsError::InvalidArgs { message: format!("invalid {label}") }` without echoing the raw value.
  - use labels `expected ref target commit id` and `ref target commit id`.
- `durable_ref_cas_mismatch() -> VfsError`
  - returns `VfsError::InvalidArgs { message: "ref compare-and-swap mismatch".to_string() }` so the existing route status mapper still produces `409 Conflict` without leaking ref names.
- `db_vcs_ref_from_record(record: RefRecord) -> DbVcsRef`
  - converts durable ref records into the existing API response shape.
- `sanitize_durable_ref_update_error(error: VfsError) -> VfsError`
  - maps backend CAS mismatch messages to `durable_ref_cas_mismatch()`.
  - returns all other errors unchanged.

**Step 2: Implement `DurableCoreRuntime::update_ref`**

Implementation requirements:

- Parse the ref name, expected target commit ID, and new target commit ID before touching stores.
- Load `self.stores.refs.get(&self.repo_id, &name).await?`.
- If the current row is missing, the target differs from `expected_target`, or `current.version.value() != expected_version`, return `durable_ref_cas_mismatch()`.
- Check `self.stores.commits.get(&self.repo_id, target).await?` only after the expected current ref matches.
- If the new target commit is missing, return `VfsError::ObjectNotFound { id: target.to_hex() }`.
- Apply the durable CAS:

```rust
self.stores.refs.update(RefUpdate {
    repo_id: self.repo_id.clone(),
    name,
    target,
    expectation: RefExpectation::Matches {
        target: expected_target,
        version: current.version,
    },
}).await
```

- Sanitize backend CAS mismatch errors before returning.
- Convert the resulting `RefRecord` into `DbVcsRef`.
- Leave every other durable `CoreDb` method fail-closed.

**Step 3: Verify GREEN**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Expected: the durable core tests pass; warm release perf remains in the prior storage-path band and does not show a meaningful memory increase.

**Step 4: Commit**

```bash
git add src/server/core.rs
git commit -m "feat: add durable update-ref executor path"
```

## Task 3: Preserve Startup And Route Fail-Closed Contracts

**Files:**

- Modify: `src/server/core.rs` only if focused tests expose a gap.
- Modify: `tests/server_startup.rs` only if existing coverage no longer proves startup fail-closed behavior.

**Step 1: Verify startup fail-closed behavior**

Run:

```bash
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
```

Expected:

- `STRATUM_CORE_RUNTIME=durable-cloud` still fails before local state open, durable backend validation, migration preflight, or serving.
- No `.vfs/state.bin` or local control-plane files are created by unsupported durable core startup.

**Step 2: Verify non-update-ref methods still fail closed**

The updated `server::core::tests::durable_core_runtime::route_methods_fail_closed` must still prove:

- auth methods fail closed and do not leak usernames/tokens.
- filesystem write/read representatives fail closed and do not leak paths or bodies.
- `list_refs`, `create_ref`, and `commit_as` fail closed and do not leak refs, commit messages, or target strings.

**Step 3: Commit only if additional test/code changes were required**

```bash
git add src/server/core.rs tests/server_startup.rs
git commit -m "test: preserve durable core fail-closed boundaries"
```

Skip this commit if Task 2 already covers the necessary assertions without additional changes.

## Task 4: Docs And Status

**Files:**

- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update HTTP/backend durability docs**

Record that:

- Durable `CoreDb::update_ref` is now executable internally over durable commit/ref stores.
- The method checks the current ref expectation before target commit metadata, then applies ref CAS through the durable ref store.
- Broad durable HTTP serving remains disabled because startup and durable auth still fail closed.
- `POST /vcs/commit`, filesystem routes, ref creation, list refs, log/status/diff/revert, audit/idempotency/workspace-head integration, and object-byte routing remain future work.

**Step 2: Update living status**

Add a completed slice section for Durable Update-Ref Executor Path:

- What is built: redacted durable parsing, current-ref CAS precheck, target commit metadata validation after expectation match, durable ref CAS, stale/missing-target no-mutation behavior, unchanged startup fail-closed behavior.
- What is not built: live durable server startup, auth, full commit transaction, object bytes, audit/idempotency/workspace-head transaction, create/list refs, filesystem execution, Postgres/R2 live route wiring.
- Recommended next slice: `create_ref` executor parity or the first commit transaction executor skeleton, depending on review findings.

**Step 3: Verify docs diff**

Run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git diff --check
```

**Step 4: Commit**

```bash
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: record durable update-ref executor path"
```

## Task 5: Review, Verification, And Integration

**Files:**

- All changed files

**Step 1: Subagent review loops**

After implementation, run fresh local subagents:

- Spec reviewer: verify the slice enables only durable `update_ref`, preserves startup/auth/route fail-closed behavior, checks CAS expectation before target commit existence, preserves no-mutation behavior, and keeps docs/status aligned.
- Code-quality reviewer: verify Rust API shape, error hygiene, object-safety, async trait usage, allocation/cloning footprint, test quality, and performance risks.

Fix any accepted findings locally in small review-fix commits.

**Step 2: Focused gates**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
cargo clippy --locked --lib --tests -- -D warnings
git diff --check
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

**Step 3: Full v2/foundation gates**

Run long tests with a sleep before the command:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture
git diff --check
```

**Step 4: Push and merge**

After v2 verification:

```bash
git status --short --branch
git push origin v2/foundation
```

Then merge to main from `/Users/rajattiwari/virtualfilesystem/lattice`, preserving unrelated untracked `.claude/`:

```bash
git fetch origin
git checkout main
git pull --ff-only origin main
git merge --no-ff origin/v2/foundation
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git diff --check
git push origin main
```
