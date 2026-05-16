# Durable-Cloud Mounted-Session FS Mutations Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable `STRATUM_CORE_RUNTIME=durable-cloud` to serve mounted workspace filesystem mutations for sessions with durable `session_ref`s, while every other durable-cloud mutation surface stays fail-closed.

**Architecture:** Reuse the existing guarded durable mounted-session FS mutation engine and route-level policy/idempotency/audit/recovery plumbing. Add a narrow FS-mutation capability seam for durable-cloud instead of exposing the broader guarded durable commit route, then mount only `PUT/PATCH/DELETE/POST /fs` on the durable-cloud router. Keep commit/ref CAS behavior unchanged by delegating to the existing durable mutation executor.

**Tech Stack:** Rust, Axum, Tokio, `StratumStores`, durable Postgres/R2 store traits, in-memory store tests, HTTP capability manifest fixtures.

---

## Grounding

Required context:

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/project-status.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/docs/plans/2026-05-15-backend-roadmap.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-15-live-gates-in-ci.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-15-capability-manifest-endpoint.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-14-hosted-storage-operations-hardening.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-12-broad-durable-core-runtime-incremental-enablement.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-09-durable-visibility-mutation-safety-repair.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-09-policy-review-audit-parity.md`

Important constraints:

- No SMFS/Mirage extraction for this slice.
- Do not alter durable commit/ref CAS semantics. Reuse the existing durable FS mutation executor.
- Do not enable durable-cloud VCS, review, protected-rule, auth-login, workspace, run, audit, or recovery operator routes.
- Keep `STRATUM_DURABLE_COMMIT_ROUTE=1` local-state guarded behavior unchanged.
- Keep durable-cloud logs and HTTP errors secret-safe: no DB URLs, R2 endpoints, object keys, repo ids from mismatches, bearer tokens, request bodies, raw backend errors, or local paths.

## Current Seams

- `src/server/mod.rs:452` builds durable-cloud with `DurableCoreRuntime`, `ServerLocalDb::unavailable()`, `routes_fs::durable_read_routes()`, `routes_vcs::durable_read_routes()`, and stable 501 unsupported groups.
- `src/server/routes_fs.rs:1264` currently mounts durable-cloud FS reads and sends all FS mutation methods to `durable_cloud_route_not_supported`.
- `src/server/routes_fs.rs:476` discovers durable FS mutation capability only through `state.core.guarded_durable_commit_route()`, so durable-cloud cannot use it.
- `src/server/routes_fs.rs:1383`, `1700`, `1877`, and `2033` already implement write, metadata patch, delete, copy, and move with policy tokens, idempotency, audit, and durable FS mutation recovery.
- `src/server/core.rs:220` defines `GuardedDurableCommitRoute`, a wrapper over `DurableCoreRuntime` that exposes the existing durable FS mutation output methods.
- `src/server/core.rs:780-1285` already has durable mutable-session preflights and output methods (`durable_write_file_output_as`, `durable_mkdir_p_output_as`, `durable_rm_output_as`, `durable_cp_output_as`, `durable_mv_output_as`, `durable_set_metadata_output_as`).
- `src/server/core.rs:2089` implements `CoreDb` for `DurableCoreRuntime`; FS mutation preflights and mutation methods currently return `route_not_supported`.
- `src/server/routes_capabilities.rs:323` advertises durable-cloud filesystem mutations as unavailable today.
- `src/server/routes_fs.rs:2772-3335` already has in-memory durable workspace/token/router helpers that can be reused for durable-cloud route tests.

## Task 1: Add Durable-Cloud FS Mutation Route Tests First

**Files:**

- Modify: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/src/server/routes_fs.rs`
- Possibly modify: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/src/server/mod.rs`

**Step 1: Add durable-cloud mounted-session test helpers**

Reuse existing helpers where possible:

- `durable_core_router_with_workspace_store`
- `durable_workspace_bearer_headers`
- `seed_durable_workspace_base`
- `create_local_repo_workspace_with_refs`
- `workspace_headers`
- `response_json`

Add a helper that creates a durable-cloud router backed by `StratumStores::local_memory()` and an `InMemoryWorkspaceMetadataStore` workspace with:

- repo id matching the router repo
- root path `/demo`
- `base_ref` set to `main`
- configurable `session_ref`
- read/write prefixes under `/demo`
- a principal record so durable workspace bearer auth succeeds

Do not create or open `StratumDb` in this helper.

**Step 2: Write failing smoke test for all mounted-session FS mutations**

Add `durable_cloud_mounted_session_mkdir_write_copy_move_patch_delete_survives_restart`:

1. Seed durable `main` with `/demo`.
2. Build durable-cloud router with a mounted workspace bearer token and `session_ref`.
3. Call:
   - `PUT /fs/new.txt` with an `Idempotency-Key`
   - `PUT /fs/subdir` with `x-stratum-type: directory`
   - `POST /fs/new.txt?op=copy&dst=/copy.txt`
   - `POST /fs/copy.txt?op=move&dst=/subdir/moved.txt`
   - `PATCH /fs/subdir/moved.txt` with metadata JSON
   - `DELETE /fs/new.txt`
4. Read back `GET /fs/subdir/moved.txt?stat=true` and `GET /fs/subdir/moved.txt`.
5. Build a fresh durable-cloud router from the same stores and repeat the read to prove the mutation lived in durable stores, not local state.
6. Assert `main` remains at the base commit and only the workspace `session_ref` advances.

Run:

```bash
cargo test --locked server::routes_fs::tests::durable_cloud_mounted_session_mkdir_write_copy_move_patch_delete_survives_restart --lib -- --nocapture
```

Expected before implementation: FAIL with stable 501 or durable-cloud route-not-supported.

**Step 3: Write fail-closed route tests**

Add focused tests:

- `durable_cloud_fs_mutation_without_session_ref_fails_closed_without_mutation`
- `durable_cloud_fs_mutation_rejects_cross_repo_workspace_before_mutation`
- `durable_cloud_fs_mutation_rejects_write_outside_scope_without_mutation`
- `durable_cloud_unmounted_fs_mutation_still_returns_stable_501`

Assertions:

- No session ref is created or advanced when the request fails.
- Cross-repo and conflicting `X-Stratum-Repo` errors do not echo either repo id.
- Out-of-scope paths use projected or redacted external paths, not backing local paths.
- Unmounted durable-cloud FS mutation still returns the stable unsupported JSON 501.

Run:

```bash
cargo test --locked server::routes_fs::tests::durable_cloud_fs_mutation --lib -- --nocapture
```

Expected before implementation: FAIL for mounted-session success, PASS or FAIL-closed for unsupported cases depending on route mount order.

**Step 4: Add idempotency/audit/recovery checks**

Add `durable_cloud_fs_mutation_idempotency_replay_and_audit_identity_are_durable`:

- Send the same `PUT /fs/replay.txt` twice with the same idempotency key.
- Assert only one durable FS mutation commit is created for that operation.
- Assert audit details include `operation_id`, `target_ref`, `previous_commit`, `new_commit`, and `changed_path_count`.
- Assert the audit event does not contain request body content.
- Rebuild the router with the same stores and replay again to prove idempotency replay uses durable stores.

Run:

```bash
cargo test --locked server::routes_fs::tests::durable_cloud_fs_mutation_idempotency_replay_and_audit_identity_are_durable --lib -- --nocapture
```

Expected before implementation: FAIL with 501.

**Step 5: Commit tests**

```bash
git add src/server/routes_fs.rs src/server/mod.rs
git commit -m "test: cover durable-cloud mounted-session fs mutations"
```

## Task 2: Add a Narrow Durable FS Mutation Capability Seam

**Files:**

- Modify: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/src/server/core.rs`
- Modify: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/src/server/routes_fs.rs`

**Step 1: Introduce a filesystem-only capability wrapper**

Create a new `DurableFsMutationRoute` or similarly named wrapper in `src/server/core.rs`. It can wrap `DurableCoreRuntime` like `GuardedDurableCommitRoute`, but expose only FS read/preflight/mutation methods needed by `routes_fs.rs`:

- `repo_id`
- `stores`
- `for_repo`
- `cat_with_stat_as`, `ls_as`, `stat_as`, `tree_as`, `find_as`, `grep_as` if read helpers need the same wrapper
- `final_existing_write_path_as`
- `check_write_file_as`
- `check_set_metadata_as`
- `check_mkdir_p_as`
- `check_rm_as`
- `check_cp_as`
- `check_cp_replay_as`
- `check_mv_as`
- `check_mv_replay_as`
- `copy_move_destination_path_as`
- `mutation_path_is_directory_as`
- `write_file_with_metadata_output_as`
- `set_metadata_output_as`
- `mkdir_p_output_as`
- `rm_output_as`
- `cp_output_as`
- `mv_output_as`
- `mutable_workspace_not_supported`
- `mutable_session_ref_required`

Keep non-token mutation variants returning `policy_token_required()` if they are exposed at all.

**Step 2: Add `CoreDb` seam**

Add a default trait method:

```rust
fn durable_fs_mutation_route(&self) -> Option<DurableFsMutationRoute> {
    None
}
```

Implement it for:

- `LocalCoreRuntime`: return `self.guarded_durable_commit_route.clone().map(DurableFsMutationRoute::from_guarded_or_runtime)` or construct from the same repo/stores without changing local behavior.
- `DurableCoreRuntime`: return a durable FS mutation route over `self.repo_id` and `self.stores`.

Do not make `DurableCoreRuntime::guarded_durable_commit_route()` return `Some`. That would expose VCS/recovery assumptions beyond this slice.

**Step 3: Route helper switch**

Change `routes_fs.rs` helpers to use the new filesystem-only seam:

- `guarded_durable_fs_capability` can become `durable_fs_read_capability` if the wrapper is shared for reads, or remain guarded-only if durable-cloud reads can continue through `state.core`.
- `durable_fs_mutation_capability` must call `state.core.durable_fs_mutation_route()` after checking mounted workspace plus `session_ref`, then `for_repo(resolve repo)`.
- `durable_fs_mutation_recovery_from_output`, `enqueue_durable_fs_mutation_recovery`, `enqueue_durable_fs_mutation_post_visible_recovery`, `complete_durable_fs_mutation_recovery_intent`, `record_durable_fs_mutation_recovery_failure`, and `replace_durable_fs_mutation_idempotency_claim_response` must depend on the FS-only capability or its stores, not on `guarded_durable_commit_route()`.

**Step 4: Keep unsupported cases fail-closed**

The new helper must preserve these outcomes:

- no mount under local guarded route: `durable mutable workspace route execution is not supported yet`
- mount without `session_ref`: `durable mutable workspace route requires a session ref`
- durable-cloud unmounted request: stable durable-cloud unsupported JSON 501 if it hits an unsupported route, or an existing auth/repo error before route execution where middleware requires it
- non-token direct durable mutations: `policy token required`

Run focused tests:

```bash
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_fs::tests::durable_cloud_fs_mutation --lib -- --nocapture
```

**Step 5: Commit seam**

```bash
git add src/server/core.rs src/server/routes_fs.rs
git commit -m "feat: add durable fs mutation capability seam"
```

## Task 3: Mount Durable-Cloud FS Mutation Routes Only

**Files:**

- Modify: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/src/server/routes_fs.rs`
- Modify: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/src/server/mod.rs`

**Step 1: Add durable-cloud FS route builder**

Replace or supplement `durable_read_routes()` with a builder that mounts:

- `GET /fs`, `GET /fs/{*path}`
- `PUT /fs`, `PUT /fs/{*path}`
- `PATCH /fs`, `PATCH /fs/{*path}`
- `DELETE /fs`, `DELETE /fs/{*path}`
- `POST /fs`, `POST /fs/{*path}`
- existing search/tree reads

The mutation methods should call the existing `put_fs`, `patch_fs`, `delete_fs`, and `post_fs` handlers. Do not mount any VCS mutation handlers.

**Step 2: Preserve stable 501 for unmounted or unsupported route groups**

Update `durable_core_router_returns_stable_501_for_unsupported_groups` in `src/server/mod.rs`:

- Remove the mounted-session FS mutation case from the unsupported list.
- Add explicit VCS/review/auth/workspace/run/audit unsupported cases.
- If a raw unmounted `PUT /fs/file.md` now reaches auth first, move stable 501 coverage into a route-specific no-auth or no-workspace test that matches the final router semantics.

**Step 3: Run focused router tests**

```bash
cargo test --locked server::tests::durable_core_router_returns_stable_501_for_unsupported_groups --lib -- --nocapture
cargo test --locked server::routes_fs::tests::durable_cloud --lib -- --nocapture
```

**Step 4: Commit route mount**

```bash
git add src/server/routes_fs.rs src/server/mod.rs
git commit -m "feat: route durable-cloud mounted fs mutations"
```

## Task 4: Update Capability Manifest and SDK Fixtures

**Files:**

- Modify: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/src/server/routes_capabilities.rs`
- Modify: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/sdk/contracts/capabilities.v1.durable-cloud.json`
- Modify if generated: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/sdk/contracts/capabilities.v1.json`
- Modify: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/http-api-guide.md`

**Step 1: Make durable-cloud filesystem mutations available in manifest**

Change `filesystem_routes(durable_cloud)` so durable-cloud advertises:

- `filesystem.write.available == true`
- `filesystem.patch.available == true`
- `filesystem.delete.available == true`
- `filesystem.copy.available == true`
- `filesystem.move.available == true`

Keep VCS commit/revert/ref create/ref update, auth, workspace, audit, runs, review, protected routes unavailable with the stable durable-cloud unsupported reason.

**Step 2: Update manifest tests**

Rename or update `durable_cloud_capabilities_advertise_read_only_current_router` to describe mounted-session FS mutations, then update `durable_cloud_full_router_mounts_capabilities_without_auth` so FS mutation routes are expected to be mounted.

Run:

```bash
cargo test --locked server::routes_capabilities::tests::durable_cloud --lib -- --nocapture
```

Expected: tests pass after manifest update.

**Step 3: Regenerate checked-in SDK fixture**

Run the existing fixture update command only if the repository supports it through `STRATUM_UPDATE_CAPABILITY_FIXTURES=1`:

```bash
STRATUM_UPDATE_CAPABILITY_FIXTURES=1 cargo test --locked server::routes_capabilities::tests::update_checked_in_sdk_contract_fixture_when_requested --lib -- --nocapture
```

Then run without the env var to verify fixtures are current:

```bash
cargo test --locked server::routes_capabilities::tests::checked_in_sdk_contract_fixtures_match_manifest --lib -- --nocapture
```

**Step 4: Commit capabilities**

```bash
git add src/server/routes_capabilities.rs sdk/contracts/capabilities.v1*.json docs/http-api-guide.md
git commit -m "docs: advertise durable-cloud fs mutations"
```

## Task 5: Startup, No-Local-State, and Concurrency Coverage

**Files:**

- Modify: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/tests/server_startup.rs`
- Modify: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/src/server/routes_fs.rs`
- Modify only if needed: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/src/backend/runtime.rs`

**Step 1: Preserve startup gates and no local `.vfs/state.bin`**

Extend existing durable-cloud startup tests to assert that enabling FS mutations does not require:

- `STRATUM_DURABLE_COMMIT_ROUTE=1`
- a local `StratumDb`
- `.vfs/state.bin`

Run:

```bash
cargo test --locked --test server_startup durable -- --nocapture
```

**Step 2: Add bounded concurrent mounted-session write test**

Add `durable_cloud_concurrent_mounted_session_writes_advance_session_ref_without_local_state`:

- Seed `/demo`.
- Create one mounted workspace token with a session ref.
- Spawn a bounded number of concurrent `PUT /fs/concurrent-N.txt` requests, each with a unique idempotency key.
- Assert each succeeds or, if session-ref CAS conflicts can occur under current executor semantics, assert the route returns the existing redacted CAS/conflict error without leaking raw backend details.
- Read all successful files through a fresh durable-cloud router.
- Assert no local `.vfs/state.bin` exists in the test temp directory.

This is a correctness and churn smoke test, not a microbenchmark. Do not add production locking or connection-pool changes in this slice.

Run:

```bash
cargo test --locked server::routes_fs::tests::durable_cloud_concurrent_mounted_session_writes --lib -- --nocapture
```

**Step 3: Commit startup/concurrency coverage**

```bash
git add tests/server_startup.rs src/server/routes_fs.rs src/backend/runtime.rs
git commit -m "test: preserve durable-cloud fs mutation startup boundaries"
```

## Task 6: Documentation and Status

**Files:**

- Modify: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/http-api-guide.md`
- Modify: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/project-status.md`
- Modify if needed: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-16-durable-cloud-mounted-session-fs-mutations.md`

**Step 1: Update HTTP API guide**

Change the durable-cloud section from read-only to:

- committed FS/search/tree reads remain available
- mounted workspace sessions with durable `session_ref`s can call `PUT/PATCH/DELETE/POST /fs`
- mutations materialize/update the durable session ref through the existing durable FS mutation commit path
- unmounted or no-session-ref mutable workspace requests fail closed
- VCS mutations, review, protected-rule, auth-login, workspace, run, audit, and recovery operator routes remain unsupported 501
- local developers need durable backend gates plus workspace bearer/session setup; there is no local `.vfs/state.bin` fallback

**Step 2: Update project status**

Add a dated slice entry near the durable-cloud read-router section:

- summary of routes enabled
- tests and gates run
- live Postgres/R2 status, explicitly saying skipped if credentials are absent
- remaining out-of-scope items

Also update the current-state summary at the bottom so it no longer says durable-cloud FS mutations are future work.

**Step 3: Commit docs**

```bash
git add docs/http-api-guide.md docs/project-status.md docs/plans/2026-05-16-durable-cloud-mounted-session-fs-mutations.md
git commit -m "docs: record durable-cloud fs mutation slice"
```

## Task 7: Review and Verification

**Files:**

- Review all changed files.

**Step 1: Spec/correctness review**

Ask a review agent to check:

- mounted-session durable-cloud FS mutations only
- session ref and repo routing fail-closed behavior
- policy-token enforcement below route handlers
- idempotency/audit/recovery behavior
- no local `.vfs/state.bin` fallback
- no commit/ref CAS behavior changes

**Step 2: Code-quality/security review**

Ask a second review agent to check:

- route mount ordering
- redaction and secret-safe logs/errors
- capability manifest contract accuracy
- concurrency/CAS handling
- test reliability and flake risk
- no broad durable-cloud mutation surface opened

**Step 3: Fix findings locally**

Inspect reviewer claims against local diffs. Apply only confirmed fixes.

**Step 4: Run required verification**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked server::routes_fs::tests::durable_cloud --lib -- --nocapture
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_capabilities::tests::durable_cloud --lib -- --nocapture
cargo test --locked server::routes_capabilities::tests::checked_in_sdk_contract_fixtures_match_manifest --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

If live credentials are available, also run the exact live CI commands for Postgres and R2. If they are unavailable, record that live gates were skipped by existing skip semantics.

**Step 5: Final commit if fixes were needed**

```bash
git status --short
git add <confirmed files>
git commit -m "fix: harden durable-cloud fs mutation routing"
```

## Rollback Plan

- Revert the durable-cloud FS route builder change to mount `durable_read_routes()` only.
- Leave the guarded local-state durable FS mutation path untouched.
- Preserve capability fixtures and docs in the same revert so the manifest again matches the mounted router.
- Do not disable local/unit gates to hide live infra failures.
