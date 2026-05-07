# Durable Ref CAS Visibility Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add the internal durable commit ref compare-and-swap visibility step that makes an already-inserted durable commit reachable through `main`.

**Architecture:** Keep this slice in `src/backend/core_transaction.rs` beside the durable write-plan, object convergence, and commit metadata insert executors. Add a small redacted CAS visibility summary and a method that validates a matching `DurableCoreCommitMetadataInsert`, derives the ref expectation from `DurableCoreCommitSourceSnapshot`, and applies `RefStore::update` to `main`. Do not wire this into live HTTP `POST /vcs/commit`; `DurableCoreRuntime::commit_as` and durable startup remain fail-closed.

**Tech Stack:** Rust, Tokio async tests, existing `RefStore`, `RefUpdate`, `RefExpectation`, `RefRecord`, `DurableCoreCommitObjectTreeWritePlan`, `DurableCoreCommitMetadataInsert`, local in-memory stores, focused fake stores, cargo gates, and release perf measurement with `/usr/bin/time -l`.

---

## CTO Plan And Status Read

The CTO plan's durable commit path makes commits visible only after object convergence and commit metadata insertion complete. Current project status says durable object convergence and commit metadata insert are built, and the recommended next slice is durable ref CAS visibility: create unborn `main` or compare-and-swap existing `main` using the parent preflight expectation, while route execution stays disabled.

SMFS is still not a code source for this slice. Its durable queue claim/finalize/backoff model belongs to the later post-CAS completion/recovery envelope. For this slice, source freshness is the parent ref target/version in `DurableCoreCommitSourceSnapshot`, not timestamps.

## Scope

In scope:

- Add an internal `DurableCoreCommitRefCasVisibility` summary.
- Add a method on `DurableCoreCommitObjectTreeWritePlan` that accepts a `DurableCoreCommitMetadataInsert` and `&dyn RefStore`.
- Validate that the metadata insert belongs to the same plan before touching refs:
  - repo ID is used from the metadata insert;
  - root tree matches the plan root tree;
  - parents match the source parent state;
  - changed-path count matches the plan;
  - target commit ID comes from the metadata insert.
- Derive `RefExpectation::MustNotExist` for unborn source state.
- Derive `RefExpectation::Matches { target, version }` for existing source state.
- Apply durable `RefStore::update` to `MAIN_REF`.
- Validate the returned `RefRecord` field-by-field and return a redacted summary.
- Sanitize stale/CAS failures to a fixed `ref compare-and-swap mismatch` error with no ref name, commit IDs, path strings, SQL details, or request data.
- Sanitize non-CAS ref-store failures to a fixed `CorruptStore` such as `durable commit ref visibility update failed`.
- Keep route execution, workspace-head update, audit append, idempotency completion/replay, repair scheduling, distributed locks/fencing, and durable auth/session out of scope.

Out of scope:

- No live durable `POST /vcs/commit` route execution.
- No `WorkspaceMetadataStore` head update.
- No audit append.
- No idempotency completion or replay.
- No post-CAS recovery queue.
- No durable filesystem/search/tree serving.
- No changes to local `StratumDb` route behavior.
- No Postgres schema changes unless a focused test proves the current `RefStore` contract cannot express the visibility step.

## Performance Rules

After every meaningful code or docs diff, run:

```bash
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Record real/user/sys time, max RSS, and peak memory footprint. This metadata/ref slice should not affect hot local VFS runtime paths; inspect any unexpected perf or memory movement before committing.

## Task 1: Add RED Tests For Ref CAS Visibility

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Step 1: Write failing tests**

Add a sibling test module such as `backend::core_transaction::tests::durable_core_commit_ref_cas_visibility`.

Cover:

- `ref_cas_visibility_creates_unborn_main_after_metadata_insert`
  - build an unborn plan with private path/content;
  - converge objects into `LocalMemoryObjectStore`;
  - insert metadata into `LocalMemoryCommitStore`;
  - call the new visibility method with `LocalMemoryRefStore`;
  - assert returned summary points `main` at the inserted commit with version 1;
  - assert `refs.get(repo, main)` returns the same target/version.
- `ref_cas_visibility_updates_existing_main_using_parent_target_and_version`
  - seed parent commit metadata;
  - create `main` at the parent commit to obtain version 1;
  - build an existing-parent source snapshot with that target/version;
  - insert new commit metadata;
  - call visibility and assert `main` points at the new commit with version 2.
- `ref_cas_visibility_rejects_stale_unborn_main_without_mutation`
  - build an unborn source snapshot and metadata insert;
  - create `main` at another target before visibility;
  - assert fixed CAS mismatch and unchanged ref target/version.
- `ref_cas_visibility_rejects_stale_existing_main_without_mutation`
  - build an existing-parent source snapshot at version 1;
  - advance `main` to a racing target before visibility;
  - assert fixed CAS mismatch and unchanged racing ref target/version.
- `ref_cas_visibility_rejects_mismatched_metadata_insert_without_ref_mutation`
  - mutate a cloned metadata summary so root tree or parents do not match the plan;
  - assert fixed redacted corruption and no ref update.
- `ref_cas_visibility_sanitizes_leaky_ref_store_cas_errors`
  - use a fake `RefStore` returning `InvalidArgs { message: "ref compare-and-swap mismatch: refs/heads/main private-token ..." }`;
  - assert the rendered error contains only the fixed CAS message and not the private suffix.
- `ref_cas_visibility_wraps_leaky_non_cas_ref_store_errors`
  - use a fake `RefStore` returning SQL/path/commit-like secrets;
  - assert the fixed redacted visibility-update error.
- `ref_cas_visibility_rejects_store_returning_mismatched_record`
  - fake `RefStore::update` returns a record whose target/name/repo/version does not match;
  - assert fixed redacted corruption.
- `ref_cas_visibility_debug_redacts_private_commit_context`
  - format the visibility summary;
  - assert repo/ref name, commit ID, and version are visible, but author/message/path/bytes are not.

**Step 2: Verify RED**

Run:

```bash
cargo test --locked backend::core_transaction::tests::durable_core_commit_ref_cas_visibility --lib -- --nocapture
```

Expected before implementation: fail to compile because the visibility summary and method do not exist.

## Task 2: Implement The Ref CAS Visibility Step

**Files:**

- Modify: `src/backend/core_transaction.rs`

**Step 1: Import ref store contracts**

Add the needed backend imports in `src/backend/core_transaction.rs`:

- `RefExpectation`
- `RefRecord`
- `RefStore`
- `RefUpdate`

**Step 2: Add the redacted summary type**

Add `DurableCoreCommitRefCasVisibility` near `DurableCoreCommitMetadataInsert`:

- fields: `repo_id: RepoId`, `ref_name: &'static str` or `String`, `commit_id: CommitId`, `version: RefVersion`;
- accessors: `repo_id()`, `ref_name()`, `commit_id()`, `version()`;
- custom `Debug` that prints repo ID, ref name, commit ID, and version only.

**Step 3: Add helper validation**

Before touching refs, validate:

- `metadata.root_tree_id() == self.root_tree_id()`;
- `metadata.parents()` equals `[]` for unborn source;
- `metadata.parents()` equals `[target]` for existing source;
- `metadata.changed_path_count() == self.changed_paths().len()`.

On mismatch, return:

```rust
VfsError::CorruptStore {
    message: "durable commit ref visibility input does not match write plan".to_string(),
}
```

**Step 4: Apply ref CAS**

Implement a method like:

```rust
impl DurableCoreCommitObjectTreeWritePlan {
    pub(crate) async fn apply_ref_cas_visibility(
        &self,
        metadata: &DurableCoreCommitMetadataInsert,
        ref_store: &dyn RefStore,
    ) -> Result<DurableCoreCommitRefCasVisibility, VfsError>
}
```

Implementation outline:

- Build `RefName::new(MAIN_REF)` and map impossible parse failure to fixed `CorruptStore`.
- Build expectation from `self.source().parent_state()`.
- Call `ref_store.update(RefUpdate { repo_id: metadata.repo_id().clone(), name, target: metadata.commit_id(), expectation })`.
- Map CAS mismatch errors whose message starts with `ref compare-and-swap mismatch` to:

```rust
VfsError::InvalidArgs {
    message: "ref compare-and-swap mismatch".to_string(),
}
```

- Map all other ref-store errors to:

```rust
VfsError::CorruptStore {
    message: "durable commit ref visibility update failed".to_string(),
}
```

- Validate returned record:
  - repo matches metadata repo;
  - name is `main`;
  - target is metadata commit ID;
  - unborn version is 1;
  - existing version is source version + 1.
- On returned-record mismatch, return:

```rust
VfsError::CorruptStore {
    message: "durable commit ref visibility returned mismatched record".to_string(),
}
```

- Return `DurableCoreCommitRefCasVisibility`.
- Do not access workspace metadata, audit, idempotency, auth, server route code, or local `StratumDb`.

**Step 5: Verify GREEN**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked backend::core_transaction::tests::durable_core_commit_ref_cas_visibility --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_metadata_insert --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_object_convergence --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_write_plan --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Expected: new visibility tests pass, existing durable commit stages still pass, and durable routes remain fail-closed.

**Step 6: Commit implementation**

```bash
git add src/backend/core_transaction.rs
git commit -m "feat: add durable commit ref cas visibility"
```

## Task 3: Review And Status

**Files:**

- Modify: `src/backend/core_transaction.rs` if reviews find issues.
- Modify: `docs/project-status.md`.

**Step 1: Request reviews**

Ask two reviewers:

- Spec/correctness review:
  - Does the method require metadata insert before ref CAS?
  - Does it derive unborn/existing expectations from the source snapshot?
  - Does it make the commit visible only through `main` ref CAS?
  - Does it keep workspace head, audit, idempotency, repair, auth, and routes untouched?
  - Are stale races sanitized as CAS mismatch?
- Code-quality/security/perf review:
  - Is validation before mutation complete enough?
  - Are error mappings redacted and not overbroad in ways that hide CAS mismatches?
  - Are returned-record validations strict?
  - Are tests maintainable and allocation/cloning kept low?

Fix findings locally and rerun focused tests plus the release perf gate after each meaningful diff.

**Step 2: Update project status**

Record:

- Durable ref CAS visibility now creates unborn `main` or CAS-updates existing `main` after commit metadata insert.
- Stale parent/version races return sanitized CAS mismatch without moving the ref.
- Non-CAS ref-store failures and mismatched returned records are redacted.
- This is the first internal durable commit step that makes a commit visible through a ref.
- It still stops before workspace-head update, audit append, idempotency completion/replay, repair scheduling, live durable routing, and durable auth/session.
- Next slice is post-CAS completion and recovery envelope.

**Step 3: Commit status**

```bash
git add docs/project-status.md
git commit -m "docs: record durable commit ref visibility"
```

## Task 4: Full Gates, Push, And Main Merge

Run from the v2 worktree:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture
cargo audit --deny warnings
cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git diff --check
```

Push and merge:

```bash
git push origin v2/foundation
cd /Users/rajattiwari/virtualfilesystem/lattice
git fetch origin
git merge --no-edit origin/v2/foundation
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
git diff --check
git push origin main
```

Leave pre-existing untracked `.claude/` in the main worktree untouched.
