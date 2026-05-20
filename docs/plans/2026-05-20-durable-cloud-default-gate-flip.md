# Durable-Cloud Default Gate Flip Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `STRATUM_CORE_RUNTIME=durable-cloud` selectable without `STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV` while keeping local-state as the default runtime and preserving fail-closed durable-cloud startup.

**Architecture:** Treat Slice 6 as a readiness-gate flip, not a broad runtime default change. Remove the dev-enable flag from durable-cloud readiness parsing, keep the durable auth/session, policy, repo-routing, recovery, repo id, idempotency, hosted Postgres posture, and hosted R2 posture gates, and keep `STRATUM_DURABLE_COMMIT_ROUTE=1` local-state only. Add a redacted protected live gate that can prove durable-cloud startup against clean Postgres+R2 credentials without changing default local test behavior.

**Tech Stack:** Rust 2024, Axum, Tokio, `tokio-postgres` and `deadpool-postgres` behind the `postgres` feature, R2/S3-compatible object storage through the existing byte-store adapter, Bash CI gate scripts, GitHub Actions, existing server startup process tests.

---

## Context

Slice 5.5 completed the provider-free pre-cutover load and chaos suite. The remaining Slice 6 boundary is the dev-only durable-cloud enable flag:

- `src/backend/runtime.rs` currently requires `STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV=1` in `DurableCoreRuntimeReadinessConfig::from_lookup()`.
- `tests/server_startup.rs` helper `configure_durable_core_gates()` still sets that flag.
- `docs/http-api-guide.md` and `docs/project-status.md` still describe durable-cloud as dev/test gated.
- `STRATUM_CORE_RUNTIME` must continue to default to `local-state`.
- `STRATUM_DURABLE_COMMIT_ROUTE=1` must remain rejected under `durable-cloud`; Slice 6.5 owns removing the guarded-route/dev-gate compatibility boundary.
- Local live Postgres/R2 credentials may be absent. Do not claim fresh live evidence unless a local run or protected CI proves it.

## Task 1: Runtime Readiness Gate Flip

**Files:**
- Modify: `src/backend/runtime.rs`

**Step 1: Add failing runtime parser tests**

Add tests proving durable-cloud readiness no longer needs the dev-enable flag and still fails closed on the next required readiness gate.

```rust
#[test]
fn durable_core_runtime_does_not_require_dev_enable_gate() {
    let config =
        BackendRuntimeConfig::from_lookup(lookup(&complete_durable_core_entries())).unwrap();

    assert_eq!(config.mode(), BackendRuntimeMode::Durable);
    assert_eq!(config.core_runtime_mode(), CoreRuntimeMode::DurableCloud);
    assert!(config.durable_core_runtime_ready());
}

#[test]
fn durable_core_runtime_missing_auth_session_gate_fails_before_durable_env_validation() {
    let err = BackendRuntimeConfig::from_lookup(lookup(&[
        (BACKEND_ENV, "durable"),
        (CORE_RUNTIME_ENV, "durable-cloud"),
    ]))
    .expect_err("durable-cloud should require readiness gates before durable env");

    let message = err.to_string();
    assert!(matches!(err, VfsError::NotSupported { .. }));
    assert!(message.contains(DURABLE_AUTH_SESSION_READY_ENV));
    assert!(!message.contains(DURABLE_CORE_RUNTIME_ENABLE_DEV_ENV));
    assert!(!message.contains(POSTGRES_URL_ENV));
    assert!(!message.contains(R2_SECRET_ACCESS_KEY_ENV));
}
```

Update `durable_core_entries()` so it no longer includes `DURABLE_CORE_RUNTIME_ENABLE_DEV_ENV`.

**Step 2: Run red tests**

Run:

```bash
cargo test --locked backend::runtime::tests::durable_core_runtime_does_not_require_dev_enable_gate --lib -- --exact --nocapture
cargo test --locked backend::runtime::tests::durable_core_runtime_missing_auth_session_gate_fails_before_durable_env_validation --lib -- --exact --nocapture
```

Expected before implementation: the first test fails because `STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV` is still required; the second fails if the old dev gate remains the first reported missing gate.

**Step 3: Implement the minimum runtime change**

In `DurableCoreRuntimeReadinessConfig::from_lookup()`, remove the `require_gate(lookup, DURABLE_CORE_RUNTIME_ENABLE_DEV_ENV)?;` call. Keep the constant available for cleanup in Slice 6.5 unless the compiler proves it is unused and must be locally allowed or removed from tests only.

Do not change:

- `CoreRuntimeMode::from_env_value()` defaults.
- `BackendRuntimeConfig::from_lookup()` requirement that durable-cloud also sets `STRATUM_BACKEND=durable`.
- The durable auth/session, policy, repo-routing, recovery, repo id, idempotency, Postgres posture, and R2 posture gates.
- Rejection of `STRATUM_DURABLE_COMMIT_ROUTE=1` under durable-cloud.

**Step 4: Run green tests**

Run:

```bash
cargo test --locked backend::runtime::tests::durable_core_runtime --lib -- --nocapture
```

Expected: focused runtime durable-cloud tests pass and do not require the dev-enable flag.

**Step 5: Commit**

```bash
git add src/backend/runtime.rs
git commit -m "feat: drop durable-cloud dev readiness gate"
```

## Task 2: Startup Coverage And Live Durable-Cloud Gate

**Files:**
- Modify: `tests/server_startup.rs`
- Create: `scripts/ci-live-durable-cloud-gate.sh`
- Modify: `scripts/check-pre-cutover-load-chaos.sh`
- Modify: `.github/workflows/rust-ci.yml`

**Step 1: Add failing startup tests**

Update `configure_durable_core_gates()` so it does not set `STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV`.

Add or rename startup tests to prove:

- durable-cloud with `STRATUM_BACKEND=durable` and no readiness env fails on `STRATUM_DURABLE_AUTH_SESSION_READY`, not the dev flag;
- a complete durable-core readiness set without the dev flag reaches durable backend/storage validation before local files are created;
- durable-cloud complete env still fails closed before local state when the binary lacks `postgres`;
- `STRATUM_DURABLE_COMMIT_ROUTE=1` remains rejected under durable-cloud before local files are created;
- redaction assertions do not expose raw Postgres URLs, R2 endpoints, object keys, backend errors, request bodies, idempotency keys, tokens, SQL, migration SQL, or secrets in startup output.

Use a focused test like:

```rust
#[test]
fn durable_core_runtime_without_dev_gate_reaches_durable_env_validation_before_local_files() {
    let data_dir = TempDataDir::new("durable-core-without-dev-gate");
    let mut command = server_command(data_dir.path());
    command.env("STRATUM_BACKEND", "durable");
    configure_durable_core_gates(&mut command, "repo_without_dev_gate");

    let output = command.output().expect("stratum-server should execute");

    assert!(!output.status.success());
    let text = combined_output(&output);
    assert!(text.contains("missing required durable backend environment variables"));
    assert!(!text.contains("STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV"));
    assert_no_secret_leaks(&text);
    assert!(!data_dir.path().join(".vfs").exists());
    assert_no_local_core_state_file(data_dir.path());
    assert_no_local_control_plane_files(data_dir.path());
}
```

**Step 2: Run red startup selectors**

Run:

```bash
cargo test --locked --test server_startup durable_core_runtime_without_dev_gate_reaches_durable_env_validation_before_local_files -- --exact --nocapture
cargo test --locked --test server_startup durable_core_runtime_with_durable_backend_fails_before_backend_env_validation_or_local_files -- --exact --nocapture
cargo test --locked --features postgres --test server_startup durable_core_runtime_complete_env_opens_durable_stores_without_local_state -- --exact --nocapture
```

Expected before implementation: the no-dev startup test fails because the dev-enable flag is still required; the Postgres-feature test skips locally when `STRATUM_POSTGRES_TEST_URL` is absent.

**Step 3: Make live R2 configuration injectable for the Postgres startup process test**

Keep provider-backed server startup skipped unless both live Postgres and live R2 credentials are intentionally present. Add a helper inside the `postgres_process_tests` module that reads real `STRATUM_R2_*` env only when all required R2 vars are set. If they are absent, keep the existing example-invalid config and skip only the durable-cloud live startup test when required.

The durable-cloud live startup test should:

- use the isolated Postgres schema from `TestPostgres`;
- use real R2 env only for the durable-cloud startup test;
- start with `STRATUM_CORE_RUNTIME=durable-cloud` and no `STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV`;
- assert `/health` reports `core_runtime == "durable-cloud"`;
- assert no `.vfs/state.bin` or local control-plane files are created;
- assert server output is redacted.

**Step 4: Add redacted live durable-cloud wrapper**

Create `scripts/ci-live-durable-cloud-gate.sh`:

- mask Postgres and R2 secret-bearing env values in GitHub Actions;
- skip cleanly when either Postgres or R2 config is incomplete and `STRATUM_LIVE_GATE_REQUIRED` is not `1`;
- fail closed with exit `2` when required config is incomplete and `STRATUM_LIVE_GATE_REQUIRED=1`;
- run the Postgres and R2 live wrappers first;
- run the focused `server_startup durable` selector with `--features postgres` and output captured/redacted on failure;
- never print raw provider command output on failure.

**Step 5: Chain the live wrapper from the pre-cutover suite**

In `scripts/check-pre-cutover-load-chaos.sh`, when `STRATUM_PRE_CUTOVER_LIVE=1`, call:

```bash
printf '==> optional live durable-cloud startup gate\n'
"$repo_root/scripts/ci-live-durable-cloud-gate.sh"
```

This keeps the default suite provider-free and adds the combined Postgres+R2 durable-cloud proof only when live mode is explicitly enabled.

**Step 6: Wire protected CI**

In `.github/workflows/rust-ci.yml`, add a protected/scheduled `live-durable-cloud` job using the `live-gates` environment. It should run only outside pull requests when the event is scheduled or the ref is protected, and pass both Postgres and R2 secrets into `scripts/ci-live-durable-cloud-gate.sh` with `STRATUM_LIVE_GATE_REQUIRED=1`.

**Step 7: Run green startup and script checks**

Run:

```bash
bash -n scripts/ci-live-durable-cloud-gate.sh
bash -n scripts/check-pre-cutover-load-chaos.sh
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_POSTGRES_TEST_URL=postgres://example.invalid/stratum STRATUM_R2_TEST_ENABLED=1 STRATUM_R2_BUCKET=ambient STRATUM_R2_ENDPOINT=https://ambient.example STRATUM_R2_ACCESS_KEY_ID=ambient STRATUM_R2_SECRET_ACCESS_KEY=ambient GITHUB_ACTIONS=true STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
```

Expected: local selectors pass or skip live portions without provider access. The default pre-cutover suite remains provider-free.

**Step 8: Commit**

```bash
git add tests/server_startup.rs scripts/ci-live-durable-cloud-gate.sh scripts/check-pre-cutover-load-chaos.sh .github/workflows/rust-ci.yml
git commit -m "test: prove durable-cloud startup without dev gate"
```

## Task 3: Operator Docs And Status

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`
- Modify: `docs/cli-cloud-bridge.md`

**Step 1: Update HTTP guide**

Update the durable-cloud startup section to say:

- `STRATUM_CORE_RUNTIME` still defaults to `local-state`;
- `STRATUM_CORE_RUNTIME=durable-cloud` no longer requires `STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV`;
- durable-cloud still requires `STRATUM_BACKEND=durable`, auth/session readiness, policy readiness, repo-routing readiness, recovery readiness, non-local repo id, idempotency retention/quota config, explicit Postgres posture, and explicit R2 posture;
- missing/incomplete config fails closed before local `.vfs/state.bin` creation;
- unsupported surfaces remain stable `501` under durable-cloud;
- `STRATUM_DURABLE_COMMIT_ROUTE=1` remains local-state only;
- live provider execution remains explicit through `STRATUM_PRE_CUTOVER_LIVE=1` and protected CI wrappers;
- rollback: reintroduce the explicit dev gate in runtime parsing and leave `STRATUM_CORE_RUNTIME` unset or `local-state`.

**Step 2: Update project status**

Add a new top section for Slice 6 with:

- completed scope;
- exact local verification commands actually run;
- live provider evidence status: either local live run details or protected CI result inspected after push;
- residual risks and out-of-scope items unchanged from the handoff.

Update known residual risks so durable-cloud is no longer described as dev/test gated, but do not claim full production traffic cutover. Keep local-state as the default runtime.

**Step 3: Update CLI cloud bridge wording**

Remove stale "current dev/test read-only durable-cloud router" wording. State that `stratumctl` can target the durable-cloud HTTP router with explicit repo context, supported durable-cloud surfaces are the current FS/search/tree/VCS/review/protected route set, and unsupported groups still return the stable `501`.

**Step 4: Run docs checks**

Run:

```bash
rg -n "dev/test gated|STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV=1|default remains gated|durable-cloud default remains gated" docs src scripts .github
git diff --check
```

Expected: no stale docs claim that durable-cloud requires `STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV=1`. References to the variable may remain only as historical/rollback/deprecation context.

**Step 5: Commit**

```bash
git add docs/http-api-guide.md docs/project-status.md docs/cli-cloud-bridge.md
git commit -m "docs: document durable-cloud gate flip"
```

## Task 4: Review, Fixes, Verification, And Integration

**Files:**
- Review full diff.
- Modify only files needed to fix review findings.

**Step 1: Run spec/correctness review**

Ask a fresh reviewer to compare the full implementation against this plan and the handoff acceptance criteria. Focus on:

- local-state default unchanged;
- durable-cloud no longer needs `STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV`;
- incomplete durable-cloud config still fails before local-state creation or fallback;
- unsupported durable-cloud surfaces remain fail-closed;
- live provider gates are explicit, redacted, and optional unless required;
- no Slice 6.5 guarded-route removal snuck in;
- docs include operator migration and rollback.

**Step 2: Run code-quality/security review**

Ask a fresh reviewer to inspect for:

- secret leakage in scripts, tests, errors, output, docs examples, and CI summaries;
- brittle tests or environment coupling;
- unsafe assumptions about provider availability;
- Rust style, ownership, error handling, and feature-gated behavior;
- shell portability and fail-closed exit codes.

**Step 3: Fix findings locally**

Use the narrowest patch that resolves each valid finding. Rerun the failing focused selector after each fix.

**Step 4: Final local verification**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_POSTGRES_TEST_URL=postgres://example.invalid/stratum STRATUM_R2_TEST_ENABLED=1 STRATUM_R2_BUCKET=ambient STRATUM_R2_ENDPOINT=https://ambient.example STRATUM_R2_ACCESS_KEY_ID=ambient STRATUM_R2_SECRET_ACCESS_KEY=ambient GITHUB_ACTIONS=true STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

If local real provider credentials exist, also run:

```bash
STRATUM_PRE_CUTOVER_LIVE=1 STRATUM_LIVE_GATE_REQUIRED=1 ./scripts/check-pre-cutover-load-chaos.sh
```

If local credentials are absent, inspect protected CI after push before claiming live Postgres+R2 durable-cloud evidence.

**Step 5: Commit review fixes**

```bash
git add <fixed files>
git commit -m "fix: address durable-cloud gate flip review"
```

Only create this commit if review fixes are needed.

**Step 6: Push and merge**

Push `v2/foundation`, inspect protected CI for live evidence, then merge to `main` through a temporary clean main worktree because the local main checkout is intentionally dirty:

```bash
git push origin v2/foundation
git worktree add /tmp/stratum-main-merge origin/main
cd /tmp/stratum-main-merge
git switch -c main-merge-$(date +%Y%m%d%H%M%S)
git merge --no-ff v2/foundation -m "merge: durable-cloud default gate flip"
git push origin HEAD:main
cd /Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation
git worktree remove /tmp/stratum-main-merge
```

Do not touch the dirty local `main` checkout.

## Verification Matrix

Required local gates:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_POSTGRES_TEST_URL=postgres://example.invalid/stratum STRATUM_R2_TEST_ENABLED=1 STRATUM_R2_BUCKET=ambient STRATUM_R2_ENDPOINT=https://ambient.example STRATUM_R2_ACCESS_KEY_ID=ambient STRATUM_R2_SECRET_ACCESS_KEY=ambient GITHUB_ACTIONS=true STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

Required live evidence, local or protected CI:

```bash
STRATUM_PRE_CUTOVER_LIVE=1 STRATUM_LIVE_GATE_REQUIRED=1 ./scripts/check-pre-cutover-load-chaos.sh
```

If local credentials are absent, the final status must say live Postgres+R2 durable-cloud evidence came from protected CI only after inspecting the run.
