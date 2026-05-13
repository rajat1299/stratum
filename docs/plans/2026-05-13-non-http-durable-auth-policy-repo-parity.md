# Non-HTTP Durable Auth Policy Repo Parity Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ensure MCP, `stratumctl`, FUSE mount, REPL/local binaries, and embedded direct callers cannot silently bypass durable-cloud auth, policy, repo-routing, and runtime gates.

**Architecture:** Keep HTTP as the only durable-cloud serving boundary for this slice. `stratumctl` stays HTTP-backed and carries auth plus repo context consistently; local direct callers stay local-state only and fail closed when `STRATUM_CORE_RUNTIME=durable-cloud` is selected. MCP and FUSE do not gain durable writes; MCP remains local-state only in durable-cloud mode until a remote HTTP read-only MCP mode or shared policy/audit/idempotency path lands, and FUSE fails closed until sparse mount/session design exists.

**Tech Stack:** Rust, Clap, Reqwest, Axum test routers, existing `BackendRuntimeConfig`/`CoreRuntimeMode` gate code, existing HTTP durable-cloud read router, existing local `StratumDb` callers, project docs.

**Implementation Result:** Added a typed non-server runtime guard used by `stratum-mcp`, `stratum-mount`, and the REPL before local state open; added `stratumctl --repo` / `STRATUM_REPO` and client-wide `X-Stratum-Repo` headers; documented that direct MCP/FUSE/REPL/embedded callers are local-only while `stratumctl` is the HTTP-backed durable-cloud operator surface. MCP/FUSE durable-cloud modes fail closed rather than opening local `.vfs/state.bin`.

---

## Reference Material Used

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/project-status.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-12-broad-durable-core-runtime-incremental-enablement.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`
- Reconnaissance on `src/backend/runtime.rs`, `src/bin/stratum_mcp.rs`, `src/bin/stratumctl.rs`, `src/bin/stratum_mount.rs`, `src/main.rs`, `src/client/mod.rs`, `src/server/mod.rs`, `tests/server_startup.rs`, `docs/mcp-guide.md`, and `docs/cli-cloud-bridge.md`.

## Capability Matrix

Mirage's manifest idea is useful here as a small per-surface contract, not as a new implementation dependency.

| Surface | Local-State | Durable-Cloud In This Slice | Rationale |
|---|---|---|---|
| HTTP `stratum-server` | live | read-only FS/search/tree and VCS metadata behind explicit gates | Existing broad durable-cloud read router is the policy/auth/repo boundary. |
| `stratumctl` | live HTTP client | live read-only HTTP client when caller supplies auth and repo context; mutations return server `501` | CLI should not open local state for hosted operations. |
| MCP stdio server | live local direct `StratumDb` | fail-closed before local state open | Direct tools bypass HTTP policy/audit/idempotency; remote MCP reads can land later. |
| FUSE `stratum-mount` | live snapshot mount | fail-closed before local state open | Sparse durable mount/session/cache semantics are out of scope. |
| REPL/local binary `src/main.rs` | live local direct `StratumDb` | fail-closed before local state open | REPL is explicitly local-state; durable-cloud must not silently become local. |
| Embedded direct `StratumDb` callers | local-only API | documented local-only; shared guard available to binaries | Direct callers are not a durable policy boundary. |

## Non-Goals

- No sparse remote FUSE.
- No MCP durable writes.
- No remote MCP write surface.
- No broad durable mutations.
- No final-object deletion/GC.
- No production hosted rollout.
- No semantic search, web console, or execution runner.

## Task 1: Add A Non-Server Local-State Runtime Guard

**Files:**
- Modify: `src/backend/runtime.rs`
- Test: `src/backend/runtime.rs`

**Step 1: Write failing tests**

Add tests for a helper such as:

```rust
ensure_local_state_runtime_for_non_server_surface_from_lookup(
    NonServerRuntimeSurface::StratumMcp,
    |name| (name == CORE_RUNTIME_ENV).then(|| "durable-cloud".to_string()),
)
```

Cover:

- Empty or `local-state` runtime is accepted.
- `durable-cloud`, `durable`, and `postgres-r2` are rejected with a stable redacted message naming the surface and `STRATUM_CORE_RUNTIME`.
- Unknown values still use the existing invalid runtime error.
- The helper reads only `STRATUM_CORE_RUNTIME`, not durable backend secret env.

Run:

```bash
cargo test --locked backend::runtime --lib -- --nocapture
```

Expected: fail before the helper exists.

**Step 2: Implement the guard**

Add public helper(s) in `src/backend/runtime.rs`:

```rust
pub fn ensure_local_state_runtime_for_non_server_surface(
    surface: NonServerRuntimeSurface,
) -> Result<(), VfsError>
```

and a lookup-backed variant for tests. The helper should parse `CoreRuntimeMode` directly from `STRATUM_CORE_RUNTIME`, reject non-UTF8 process env values with a redacted invalid-runtime error, and return `NotSupported` for `DurableCloud` without parsing durable backend env.

**Step 3: Verify**

Run:

```bash
cargo test --locked backend::runtime --lib -- --nocapture
```

Expected: PASS.

## Task 2: Fail Closed Before Local DB Open In Direct Local Binaries

**Files:**
- Modify: `src/bin/stratum_mcp.rs`
- Modify: `src/bin/stratum_mount.rs`
- Modify: `src/main.rs`
- Test: `src/bin/stratum_mcp.rs`
- Test if practical: small helper tests around the shared guard in `src/backend/runtime.rs`

**Step 1: Write failing MCP tests**

In `src/bin/stratum_mcp.rs`, add a test proving an open helper rejects durable-cloud before `StratumDb::open`:

```rust
let config = Config::default().with_data_dir(temp_dir);
let err = open_local_mcp_db_from_lookup(config, |name| {
    (name == CORE_RUNTIME_ENV).then(|| "durable-cloud".to_string())
})
.unwrap_err();
assert!(matches!(err, VfsError::NotSupported { .. }));
assert!(temp_dir.join(".vfs").join("state.bin").exists());
```

Pre-create a corrupt local `state.bin` so the test proves the guard returns before `StratumDb::open`; otherwise a fresh local open would not create `state.bin`. Use a lookup function or injected guard so the test does not mutate global environment.

Run:

```bash
cargo test --locked --bin stratum-mcp -- --nocapture
```

Expected: fail before the helper exists.

**Step 2: Add binary startup guards**

- `stratum_mcp`: call the guard before `StratumDb::open`; on error, log/print the redacted message and exit nonzero.
- `stratum_mount`: call the guard before `Config::from_env().with_compatibility_target(...)` and before `StratumDb::open`.
- REPL `src/main.rs`: call the guard before `Config::from_env()`/`StratumDb::open`; durable-cloud must exit nonzero rather than falling back to `open_memory()`.

Keep local-state behavior unchanged.

**Step 3: Verify**

Run:

```bash
cargo test --locked --bin stratum-mcp -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
```

Expected: PASS.

## Task 3: Add `stratumctl` Repo Context And Header Parity

**Files:**
- Modify: `src/bin/stratumctl.rs`
- Modify: `src/client/mod.rs`
- Test: `src/bin/stratumctl.rs`
- Test: `src/client/mod.rs`

**Step 1: Write failing CLI/client tests**

Add tests proving:

- `stratumctl --repo tenant-a ls /` parses `repo: Some("tenant-a")`.
- `STRATUM_REPO` maps to the same field through Clap's env support.
- `StratumClient` includes `X-Stratum-Repo` on request headers when configured.
- Read command methods that previously used `json(...)` still send `Authorization`, `X-Stratum-Workspace`, and `X-Stratum-Repo`: `ls`, `grep`, `find`, `log`, workspace admin commands.
- Text response methods also send those headers: `cat`, `tree`, `status`, `diff`.
- Mutations preserve auth/repo context and surface a stable server `501` body.

Use an Axum echo test server in `src/client/mod.rs` rather than spawning the real durable backend.

Run:

```bash
cargo test --locked --bin stratumctl -- --nocapture
cargo test --locked client --lib -- --nocapture
```

Expected: fail before repo context exists.

**Step 2: Implement repo context**

- Add `#[arg(long, env = "STRATUM_REPO")] repo: Option<String>` to `Cli`.
- Add repo context to `StratumClient`, for example `repo: Option<String>` plus `with_repo(...)`.
- Insert `x-stratum-repo` in `headers()` when repo is configured, validating the repo id locally and returning a redacted `invalid repo header` for malformed values.
- Construct `StratumClient::new(cli.url, auth).with_repo(cli.repo)` in `stratumctl`.
- Keep existing auth precedence: workspace bearer, bearer token, user, root. Repo is orthogonal to auth.

**Step 3: Verify**

Run:

```bash
cargo test --locked --bin stratumctl -- --nocapture
cargo test --locked client --lib -- --nocapture
```

Expected: PASS.

## Task 4: Document Durable Surface Boundaries

**Files:**
- Modify: `docs/mcp-guide.md`
- Modify: `docs/cli-cloud-bridge.md`
- Modify: `docs/project-status.md`
- Modify: `docs/plans/2026-05-13-non-http-durable-auth-policy-repo-parity.md`

**Step 1: Update docs**

Record:

- MCP is local-state only under this slice and fails closed with `STRATUM_CORE_RUNTIME=durable-cloud` before opening `.vfs/state.bin`.
- FUSE `stratum-mount` is local-state snapshot only and fails closed in durable-cloud mode until sparse mount/session design lands.
- REPL/local binaries are local-state only.
- Direct `StratumDb` embedding is local-only and is not a hosted durable policy boundary.
- `stratumctl` is the supported non-HTTP operator surface for durable-cloud reads because it uses HTTP.
- `--repo` / `STRATUM_REPO` selects hosted durable repo context, sent as `X-Stratum-Repo`.
- Unsupported durable-cloud mutations return the stable redacted `501`.
- Capability matrix from this plan.

**Step 2: Verify docs diff**

Run:

```bash
git diff --check
```

Expected: PASS.

## Required Reviews

Spec/correctness review focus:

- No non-server binary opens or saves local `.vfs/state.bin` under `STRATUM_CORE_RUNTIME=durable-cloud`.
- The guard fails before durable backend env parsing or secret validation.
- `stratumctl` sends auth/workspace/repo headers consistently across read and mutation methods.
- MCP and FUSE do not claim durable-cloud support or durable writes.
- Existing local-state behavior is unchanged.

Code-quality/security review focus:

- Guard helper is centralized and hard to bypass from binaries.
- Error messages are stable and redacted; no raw tokens, backend URLs, R2 keys, idempotency keys, request bodies, or file content leak.
- `ClientAuth::WorkspaceBearer` debug/log paths do not expose token material beyond existing behavior.
- Header construction rejects malformed repo values safely.
- Tests avoid global environment races.

## Final Verification

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked --bin stratum-mcp -- --nocapture
cargo test --locked --bin stratumctl -- --nocapture
cargo test --locked client --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```

Result on 2026-05-13 from the `v2/foundation` worktree: all listed gates passed using the crate's actual MCP binary target name, `stratum-mcp`. An additional `cargo check --locked --features fuser --bin stratum-mount` passed to cover the feature-gated FUSE binary touched by this slice. Final release perf passed **37** tests in **45.68s real**, with **1,904,214,016 bytes max RSS** and **100,024,872 bytes peak memory footprint**. Spec review reported no findings; code-quality/security review findings were fixed before the final verification sweep.
