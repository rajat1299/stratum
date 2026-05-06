# Durable Create-Ref Executor Parity Implementation Plan

> **For Codex/subagents:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development and follow `/Users/rajattiwari/.agents/skills/pragmatic-rust-guidelines/SKILL.md`, `/Users/rajattiwari/.agents/skills/rust-best-practices/SKILL.md`, and `/Users/rajattiwari/.agents/skills/rust-async-patterns/SKILL.md`.

**Goal:** Add internal durable `CoreDb::create_ref` execution over durable commit/ref stores while keeping durable startup, auth, and broad HTTP filesystem/VCS serving fail-closed.

**Architecture:** Extend `DurableCoreRuntime` with a metadata-only create-ref executor that reuses the existing durable parsing and `DbVcsRef` conversion helpers. The method validates ref and commit identifiers with redacted durable errors, rejects existing refs before target metadata lookup to preserve local VCS duplicate-first behavior, requires the target commit metadata to exist, then creates the ref through `RefStore` with `RefExpectation::MustNotExist`. It does not create commits, write object bytes, update workspace head, append audit records, complete idempotency records, or enable live durable HTTP serving.

**Tech Stack:** Rust, Axum server core seam, existing backend `CommitStore` and `RefStore` traits, durable ref CAS semantics, Tokio tests, cargo release perf gates with `/usr/bin/time -l`.

---

## Scope

In scope:

- Enable exactly one more internal route-facing durable executor method: `CoreDb::create_ref` on `DurableCoreRuntime`, the method behind `POST /vcs/refs`.
- Reuse the existing durable helpers for redacted ref-name parsing, target commit-id parsing, backend CAS sanitization, and `DbVcsRef` conversion where they fit.
- Add a small redacted duplicate-ref helper that returns a conflict-shaped error without embedding the raw ref name.
- Check whether the ref already exists before checking target commit metadata, matching local `StratumDb` / `Vcs::create_ref` ordering.
- Require the target commit metadata to exist in `stores.commits` before creating the durable ref.
- Create the ref through `stores.refs.update` with `RefExpectation::MustNotExist`.
- Recheck duplicate existence before returning a missing-target error so a race between the first ref read and target lookup does not leak the wrong failure class.
- Return the existing `DbVcsRef` response shape.
- Keep `route_execution_enabled()` false.
- Keep `login`, `authenticate_token`, filesystem/search/tree, `list_refs`, `commit_as`, log/status/diff/revert fail-closed.
- Update `docs/http-api-guide.md` and `docs/project-status.md`.

Out of scope:

- No live startup support for `STRATUM_CORE_RUNTIME=durable-cloud`.
- No HTTP durable serving test that bypasses auth; auth still fails closed in durable core.
- No `POST /vcs/commit` executor, object-byte upload, tree construction, commit metadata insertion, workspace-head update, audit append, idempotency completion, distributed locking, or background repair worker.
- No `list_refs` durable executor.
- No Postgres/R2 route wiring, connection pooling, TLS/KMS/secrets hardening, or hosted runtime cutover.
- No broad change to local `StratumDb` route behavior.

## Performance Rules For This Slice

- After each meaningful code or docs diff, run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

- Record warm wall/user/sys time, maximum resident set size, and peak memory footprint from `/usr/bin/time -l`.
- Keep the executor metadata-only: at most one duplicate ref read, one target commit metadata lookup, one duplicate recheck on missing target, and one ref CAS create.
- Clone only small repo/ref/commit IDs required by store trait ownership.
- Do not add background tasks, caches, local `StratumDb` allocation, or eager object/tree loads to the durable runtime.
- GPU efficiency is not applicable to this backend storage path unless a later semantic-indexing slice introduces GPU-backed work.

## Task 1: Add Durable Create-Ref Red Tests

**Files:**

- Modify: `src/server/core.rs`

**Step 1: Write failing tests**

Add focused tests in `server::core::tests::durable_core_runtime`:

- `create_ref_rejects_invalid_target_without_leaking_raw_value`
  - constructs `DurableCoreRuntime::new(RepoId::local(), StratumStores::local_memory())`.
  - calls `create_ref("agent/alice/session-1", "target-private-token")`.
  - asserts `VfsError::InvalidArgs`.
  - asserts the message is `invalid ref target commit id`.
  - asserts the message does not contain `target-private-token`, `private-token`, `STRATUM_CORE_RUNTIME`, or `durable-cloud`.
- `create_ref_rejects_invalid_ref_name_without_leaking_raw_value`
  - calls `create_ref("agent/alice/private-token/extra", valid_target_hex)`.
  - asserts `VfsError::InvalidArgs`.
  - asserts the message is `invalid ref name`.
  - asserts the message does not contain the raw ref name, `alice`, `private-token`, `STRATUM_CORE_RUNTIME`, or `durable-cloud`.
- `create_ref_rejects_duplicate_ref_without_mutation_or_raw_name`
  - seeds commit A and an existing ref pointing at commit A version 1 through `stores.refs.update` with `RefExpectation::MustNotExist`.
  - calls durable `create_ref` with the same ref name and a valid but missing target commit ID.
  - asserts a conflict-shaped duplicate error that does not contain the ref name or sensitive ref fragments.
  - asserts the underlying ref still points at commit A version 1, proving duplicate is checked before target metadata and no mutation happened.
- `create_ref_rejects_missing_target_without_mutation`
  - uses a valid missing target commit ID with no existing ref.
  - asserts `VfsError::ObjectNotFound { id: target_hex }`.
  - asserts the ref remains absent.
- `create_ref_rechecks_duplicate_before_missing_target_error`
  - wraps the commit store with a test `CommitStore` that creates the target ref during the missing-target lookup.
  - calls durable `create_ref` for the missing target.
  - asserts the result is the redacted duplicate conflict, not `ObjectNotFound`.
  - asserts the raced ref is present at version 1.
- `create_ref_creates_ref_for_existing_commit`
  - seeds target commit metadata.
  - calls durable `create_ref("agent/alice/session-1", target_hex)`.
  - asserts returned `DbVcsRef { name, target, version: 1 }`.
  - asserts the underlying `stores.refs.get` row matches target and version 1.

Update `route_methods_fail_closed` so it no longer expects `create_ref` to fail closed. Keep representative fail-closed checks for auth, filesystem, `list_refs`, and `commit_as`, and preserve redaction assertions.

**Step 2: Verify RED**

Run:

```bash
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
```

Expected before implementation: the new create-ref tests fail because durable `create_ref` still returns `NotSupported`.

## Task 2: Implement Durable Create-Ref Executor

**Files:**

- Modify: `src/server/core.rs`

**Step 1: Add narrow helper**

Add a private helper near the durable runtime implementation:

- `durable_ref_already_exists() -> VfsError`
  - returns `VfsError::AlreadyExists { path: "ref".to_string() }`.
  - this preserves the route's conflict status mapping while avoiding raw ref-name leakage from durable internals.

Reuse existing helpers:

- `parse_durable_ref_name`
- `parse_durable_commit_id` with label `ref target commit id`
- `db_vcs_ref_from_record`
- `sanitize_durable_ref_update_error` as the model for mapping backend CAS mismatch messages.

Add either a new `sanitize_durable_ref_create_error(error: VfsError) -> VfsError` or parameterize the existing sanitizer so backend `ref compare-and-swap mismatch: {name}` from `RefExpectation::MustNotExist` becomes `durable_ref_already_exists()`.

**Step 2: Implement `DurableCoreRuntime::create_ref`**

Implementation requirements:

- Parse the ref name and target commit ID before touching stores.
- Load `self.stores.refs.get(&self.repo_id, &name).await?`.
- If the current row exists, return `durable_ref_already_exists()`.
- Check `self.stores.commits.get(&self.repo_id, target).await?` only after duplicate-ref precheck.
- If the target commit is missing:
  - reload the ref.
  - if it now exists, return `durable_ref_already_exists()`.
  - otherwise return `VfsError::ObjectNotFound { id: target.to_hex() }`.
- Apply the durable create CAS:

```rust
self.stores.refs.update(RefUpdate {
    repo_id: self.repo_id.clone(),
    name,
    target,
    expectation: RefExpectation::MustNotExist,
}).await
```

- Sanitize backend CAS mismatch errors before returning.
- Convert the resulting `RefRecord` into `DbVcsRef`.
- Leave every other durable `CoreDb` method fail-closed except the already enabled `update_ref`.

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
git commit -m "feat: add durable create-ref executor path"
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

**Step 2: Verify non-ref-management methods still fail closed**

The updated `server::core::tests::durable_core_runtime::route_methods_fail_closed` must still prove:

- auth methods fail closed and do not leak usernames/tokens.
- filesystem write/read representatives fail closed and do not leak paths or bodies.
- `list_refs` and `commit_as` fail closed and do not leak request values.
- `create_ref` and `update_ref` are no longer part of this fail-closed assertion because they are the two internal durable ref-management executor paths.

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

- Durable `CoreDb::create_ref` is now executable internally over durable commit/ref stores.
- It rejects duplicate refs before target commit metadata lookup, requires target commit metadata before ref creation, and uses durable `RefStore` CAS with `RefExpectation::MustNotExist`.
- The existing durable `CoreDb::update_ref` remains executable internally.
- Broad durable HTTP serving remains disabled because startup and durable auth still fail closed.
- `POST /vcs/commit`, filesystem routes, list refs, log/status/diff/revert, audit/idempotency/workspace-head integration, and object-byte routing remain future work.

**Step 2: Update living status**

Add a completed slice section for Durable Create-Ref Executor Parity:

- What is built: redacted durable parsing, duplicate-ref precheck, target commit metadata validation after duplicate check, durable ref create CAS, duplicate/missing-target no-mutation behavior, unchanged startup fail-closed behavior.
- What is not built: live durable server startup, auth, full commit transaction, object bytes, audit/idempotency/workspace-head transaction, list refs, filesystem execution, Postgres/R2 live route wiring.
- Recommended next slice: first durable commit transaction executor skeleton, starting with the transaction boundaries and metadata-only preflight before any object-byte live routing.

**Step 3: Verify docs diff**

Run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git diff --check
```

**Step 4: Commit**

```bash
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: record durable create-ref executor path"
```

## Task 5: Review, Verification, And Integration

**Files:**

- All changed files

**Step 1: Subagent review loops**

After implementation, run fresh local subagents:

- Spec reviewer: verify the slice enables durable `create_ref` and preserves durable `update_ref`, preserves startup/auth/route fail-closed behavior, checks duplicate refs before target commit existence, preserves no-mutation behavior, sanitizes duplicate/CAS errors, and keeps docs/status aligned.
- Code-quality reviewer: verify Rust API shape, error hygiene, object-safety, async trait usage, allocation/cloning footprint, test quality, and performance risks.

**Step 2: Fix findings locally**

- Inspect subagent findings against the actual diff.
- Fix valid findings locally.
- Rerun focused tests and hard release perf after meaningful diffs.
- Do not accept subagent output blindly.

**Step 3: Full v2 verification**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
cargo test --locked --release --test perf_compare -- --nocapture
git diff --check
```

**Step 4: Push and merge**

Run:

```bash
git status --short
git push origin v2/foundation
git -C /Users/rajattiwari/virtualfilesystem/lattice fetch origin
git -C /Users/rajattiwari/virtualfilesystem/lattice merge origin/v2/foundation
```

Then verify main:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git diff --check
git status --short
git push origin main
```

