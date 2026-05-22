# Sparse Mount Daemon UX Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a conservative provider-free daemon UX foundation for `stratumctl mount` status, logs, and unmount controls without enabling production sparse mounts.

**Architecture:** Keep daemon lifecycle control in the `stratum` runtime crate as a local model/control seam around PID, socket, log, IPC, and redacted public status. Add `stratumctl mount status|logs|unmount` commands that exercise that seam, with all filesystem/process/IPC effects injectable for tests. Do not wire sparse cache into `stratum-mount`, do not start real FUSE/NFS daemons, and preserve the existing snapshot `stratum-mount` rollback path.

**Tech Stack:** Rust 2024, Cargo workspace, Clap, Serde JSON for bounded local IPC DTOs, existing protocol-neutral `mount_adapter` types, provider-free unit tests with tempdirs/fakes, optional `fuser` compile checks, and no privileged FUSE/NFS/Postgres/R2/network requirements.

---

## Required Skills And Agent Discipline

Implementation subagents must use:

- `superpowers:test-driven-development`
- `pragmatic-rust-guidelines`
- `rust-best-practices`

Review subagents must compare against this plan, the Slice 13 roadmap exit criterion, `docs/project-status.md`, `docs/http-api-guide.md`, and the Slice 10-12 sparse cache/hydration/mount plans.

The main session owns integration, local review, final verification, commits, merge to `main`, and pushes. Subagents may implement or review scoped work, but the main session must inspect diffs and rerun gates before accepting work.

## Required Context Read

- `markdownfs_v2_cto_architecture_plan.md`
- `docs/plans/2026-05-15-backend-roadmap.md`
- `docs/project-status.md`
- `docs/http-api-guide.md`
- `docs/plans/2026-05-21-sparse-vfs-cache-schema.md`
- `docs/plans/2026-05-21-hydration-scheduler.md`
- `docs/plans/2026-05-22-fuse-adapter-macos-fallback.md`
- `src/mount_adapter.rs`
- `src/sparse_cache/mod.rs`
- `src/sparse_cache/schema.sql`
- `src/sparse_cache/hydration.rs`
- `src/sparse_cache/mount.rs`
- `src/fuse_mount.rs`
- `src/bin/stratum_mount.rs`
- `src/bin/stratumctl.rs`
- `src/backend/runtime.rs`
- `src/backend/committed_read.rs`
- `src/fs/inode.rs`
- `src/posix.rs`
- `$WORKSPACE/smfs/crates/smfs-core/src/vfs/traits.rs`
- `$WORKSPACE/smfs/crates/smfs-core/src/vfs/types.rs`
- `$WORKSPACE/smfs/crates/smfs-core/src/mount/fuse.rs`
- `$WORKSPACE/smfs/crates/smfs-core/src/mount/nfs.rs`
- `$WORKSPACE/smfs/crates/smfs-core/src/daemon/`

## Current Inventory

Current Stratum mount behavior:

- `stratumctl` currently has HTTP/VCS `status` and `log` commands, but no mount daemon subcommand.
- `stratum-mount` is still a separate optional-`fuser` binary with only `--mountpoint` and `--read-only`.
- `stratum-mount` calls `ensure_local_state_runtime_for_non_server_surface(NonServerRuntimeSurface::StratumMount)` before opening local state, then mounts a cloned `db.snapshot_fs()`.
- `src/fuse_mount.rs` remains the local snapshot FUSE implementation. Its `AutoUnmount` option is not a daemon UX or control plane.

Slice 10-12 sparse foundation:

- `src/sparse_cache` models durable identity-scoped SQLite cache views, hydration jobs/progress, and a read-only `SparseCacheMount`.
- `src/mount_adapter.rs` models protocol-neutral read-only operations and dependency-free FUSE/NFS-shaped wire helpers.
- Sparse mount operations are provider-free helper/callback foundations only. There is no real sparse FUSE/NFS daemon, daemon lifecycle, status, logs, unmount command, write-back, or durable sparse mount cutover.

Runtime and rollback boundaries:

- Direct MCP/FUSE/REPL/non-server surfaces must keep rejecting `STRATUM_CORE_RUNTIME=durable-cloud`.
- `stratum-mount` must remain the rollback path: local-state only, snapshot-only, no auto-save or persistent write-back.
- HTTP routes, committed reads, capability fixtures, durable runtime selection, `.vfs/state.bin`, audit, idempotency, recovery, and object cleanup must not change in this slice.

smfs patterns to adapt carefully:

- Useful: per-identity `sockets/`, `pids/`, and `logs/` metadata, Unix-socket JSON-lines request/response shape, status/unmount requests, stale PID/socket cleanup, and logs UX.
- Useful macOS model: NFS-over-localhost as the default non-Linux backend choice, avoiding macFUSE/kernel-extension assumptions.
- Reject for now: real daemon detachment, real port binding, real FUSE/NFS mounting, `/sbin/mount_nfs`, `umount`, push/pull sync queues, path-primary latest-wins behavior, and provider/API-specific status fields.

## Proposed Model

Add `src/mount_daemon.rs` and expose it from `src/lib.rs`.

Provider-free public model types:

- `MountDaemonTag`: validated local identity for one mount control plane. Accept only non-empty ASCII alphanumeric plus `-`, `_`, and `.`, with a small bounded length. Reject `/`, `\`, NUL, whitespace, `.` and `..`.
- `MountDaemonPaths`: derived PID/socket/log paths under a runtime root. `Debug` must redact full paths and expose only basename/length-style metadata.
- `MountDaemonState`: `Stopped`, `Running`, `StalePid`, `Crashed`, `Unmounting`, `Unavailable`.
- `MountDaemonBackend`: reuse or wrap `MountBackend` from `src/mount_adapter.rs`, with Linux default `Fuse` and macOS/other default `Nfs`.
- `MountDaemonStatus`: redacted public status with tag, backend, state, pid presence, socket presence, log presence, optional uptime, optional hydration progress counts, and fixed status reason codes. Do not expose local socket/log/cache paths, repo ids, object ids, raw mount paths, backend errors, or secrets in `Debug`.
- `MountDaemonLogView`: bounded sanitized log lines plus total returned count and truncation flag.
- `MountDaemonUnmountResult`: `NotRunning`, `StaleCleaned`, `Requested`, `AlreadyUnmounting`, `Crashed`, or `Unavailable`, with fixed redacted messages.

IPC DTOs:

- `MountDaemonIpcRequest { version: 1, command }`
- `MountDaemonIpcCommand { Ping, Status, Logs { lines }, Unmount }`
- `MountDaemonIpcResponse { Pong, Status, Logs, UnmountAck, Error { code } }`

IPC and lifecycle behavior is represented at the unit/callback level. A real daemon process may later implement this DTO, but this slice only adds a mockable local control seam and CLI commands that can inspect metadata and use an injected IPC client in tests.

Effect seams:

- `MountDaemonProcessProbe`: checks whether a PID is alive. Production may use `kill -0` on Unix; tests use fakes. No direct unsafe code.
- `MountDaemonFileStore`: reads/writes/removes PID/socket/log metadata and tails log files. Tests use tempdirs.
- `MountDaemonIpcClient`: sends one bounded local IPC request. Tests use fakes. Production can return a fixed unavailable error until a real daemon server exists, or use Unix sockets if implemented without privileged mounts.

Status semantics:

- Missing PID and missing socket => `Stopped`.
- Invalid PID file, dead PID, or socket leftover with no live PID => `StalePid`.
- Live PID plus missing/unreachable socket => `Crashed`.
- Successful IPC status response => `Running` or `Unmounting` per response.
- All raw IO/IPC/process errors collapse to fixed status reason codes.

Unmount semantics:

- `Stopped` => idempotent `NotRunning`.
- `StalePid` => remove stale PID/socket metadata and return `StaleCleaned`.
- `Running` with IPC ack => `Requested`.
- `Unmounting` => `AlreadyUnmounting`.
- `Crashed` => return deterministic redacted `Crashed`; do not invoke `umount`, `fusermount`, or `/sbin/umount`.

Logs semantics:

- `logs` returns a bounded sanitized tail from the local log file when present.
- Missing log => empty view with `available: false`.
- Sanitization must redact obvious secret-bearing fragments such as `token=`, `secret=`, `password=`, bearer tokens, DB URLs, R2/S3 endpoints, object keys, SQL text, and raw backend/provider errors in the tested cases.

## CLI UX

Extend `stratumctl` with a local mount subcommand:

```text
stratumctl mount status [--tag <tag>] [--runtime-dir <path>] [--json]
stratumctl mount logs [--tag <tag>] [--runtime-dir <path>] [--lines <n>] [--json]
stratumctl mount unmount [--tag <tag>] [--runtime-dir <path>] [--json]
```

Defaults:

- `--tag` defaults to `default`.
- `--runtime-dir` defaults to `STRATUM_MOUNT_RUNTIME_DIR` when set, otherwise a deterministic local runtime directory under the OS temp/cache area. Tests must pass an explicit temp runtime dir.
- `--lines` is bounded, for example `1..=200`, default `50`.

The CLI should render concise text by default and JSON when `--json` is set. Text output must use redacted state/reason messages, not raw paths or raw IO errors.

Do not add `stratumctl mount start` or daemonize behavior in this slice unless a later task explicitly scopes it. This slice is status/logs/unmount UX foundation only.

## Safety And Redaction

- Public errors/status/log-adjacent outputs must not expose DB URLs, R2 endpoints, object keys, raw backend/provider errors, commit messages, request bodies, idempotency keys, lease tokens, SQL, migration SQL, advisory lock ids, local cache paths, socket paths, log paths, repo ids, object ids, raw mount paths, symlink targets, chunk bytes, bearer tokens, or secrets.
- Tests must not require privileged FUSE/NFS mounts, Postgres, R2, durable-cloud env, network, or live credentials.
- Do not import smfs latest-wins/path-primary semantics.
- Do not persist sentinel sizes. Existing `MountFileSize::Unknown` policy remains unchanged.
- Do not enable write-back, mutation persistence, flush, fsync, commit staging, durable ref updates, or direct durable-cloud mount access.

## Rollback Boundary

Rollback is deleting `src/mount_daemon.rs`, removing the `stratumctl mount` subcommand, and reverting docs/status updates. Because this slice does not wire real sparse mounting into runtime paths, rollback does not alter committed reads, HTTP behavior, durable-cloud startup, local `.vfs/state.bin`, snapshot `stratum-mount`, MCP, REPL, audit, idempotency, recovery, or object cleanup.

## Non-Goals

- Do not replace `stratum-mount` with sparse mount behavior.
- Do not add real sparse FUSE/NFS serving or a production daemon process.
- Do not add `stratumctl mount start`.
- Do not invoke `/sbin/mount_nfs`, `mount -t nfs`, `umount`, `fusermount`, or macFUSE tooling in normal tests.
- Do not add macFUSE kext/notarization/product packaging.
- Do not enable durable-cloud FUSE/MCP/REPL or remote durable MCP serving.
- Do not route committed reads through SQLite.
- Do not add write-back, mutation persistence, flush/fsync semantics, or commit staging.
- Do not change HTTP route behavior, capability fixtures, SDK packages, auth login, workspace management productization, audit/runs durable serving, semantic search, execution runner, Redis, KMS, object cleanup, or crate publishing.

## Task 1: Save And Commit This Plan

**Files:**

- Create: `docs/plans/2026-05-22-sparse-mount-daemon-ux.md`

**Step 1: Verify docs-only diff**

Run:

```bash
git diff -- docs/plans/2026-05-22-sparse-mount-daemon-ux.md
git diff --check
```

Expected: only this plan doc is changed and whitespace is clean.

**Step 2: Commit**

Run:

```bash
git add docs/plans/2026-05-22-sparse-mount-daemon-ux.md
git commit -m "docs: plan sparse mount daemon ux"
```

## Task 2: Add Mount Daemon Status And Path Model

**Files:**

- Modify: `src/lib.rs`
- Create: `src/mount_daemon.rs`

**Step 1: Write failing model tests**

Add tests in `src/mount_daemon.rs`:

- `mount_daemon_tag_rejects_path_traversal_and_nul`
- `mount_daemon_paths_are_deterministic_under_runtime_root`
- `mount_daemon_backend_default_matches_mount_adapter_default`
- `mount_daemon_status_debug_redacts_paths_and_identity`
- `mount_daemon_public_error_is_fixed_and_redacted`

Expected RED:

```bash
cargo test --locked mount_daemon::tests::mount_daemon_tag_rejects_path_traversal_and_nul --lib -- --exact --nocapture
```

It should fail before the module exists.

**Step 2: Implement minimal model**

Implement:

- `MountDaemonTag`
- `MountDaemonPaths`
- `MountDaemonState`
- `MountDaemonStatus`
- `MountDaemonErrorCode`
- `MountDaemonError`
- `MountDaemonBackend` or direct reuse of `crate::mount_adapter::MountBackend`

Keep all `Debug` and `Display` output redacted. Do not add filesystem/process/IPC effects yet.

**Step 3: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked mount_daemon::tests::mount_daemon_tag_rejects_path_traversal_and_nul --lib -- --exact --nocapture
cargo test --locked mount_daemon::tests::mount_daemon_paths_are_deterministic_under_runtime_root --lib -- --exact --nocapture
cargo test --locked mount_daemon::tests::mount_daemon_backend_default_matches_mount_adapter_default --lib -- --exact --nocapture
cargo test --locked mount_daemon::tests::mount_daemon_status_debug_redacts_paths_and_identity --lib -- --exact --nocapture
cargo test --locked mount_daemon::tests::mount_daemon_public_error_is_fixed_and_redacted --lib -- --exact --nocapture
git diff --check
```

Commit:

```bash
git add src/lib.rs src/mount_daemon.rs
git commit -m "feat: model sparse mount daemon status"
```

## Task 3: Add Provider-Free Lifecycle Probes And Status Resolution

**Files:**

- Modify: `src/mount_daemon.rs`

**Step 1: Write failing lifecycle tests**

Add tests:

- `status_reports_stopped_when_pid_and_socket_are_absent`
- `status_reports_stale_pid_when_pid_is_dead_and_socket_exists`
- `status_reports_crashed_when_pid_is_alive_but_socket_is_unreachable`
- `status_reports_running_from_ipc_status_response`
- `status_collapses_io_and_ipc_failures_to_redacted_reason_codes`

Expected RED:

```bash
cargo test --locked mount_daemon::tests::status_reports_stale_pid_when_pid_is_dead_and_socket_exists --lib -- --exact --nocapture
```

**Step 2: Implement effect seams and status resolver**

Implement:

- `MountDaemonProcessProbe` trait with a test fake.
- `MountDaemonFileStore` trait or a small probe struct that can read PID metadata, test socket/log existence, remove stale files, and tail logs through injectable methods.
- `MountDaemonIpcClient` trait with `status`, `logs`, and `unmount` request methods.
- `MountDaemonController<P, F, I>` or equivalent small coordinator.
- `resolve_status(tag, runtime_root, probes)` provider-free logic matching the status semantics above.

Production process probing may use `kill -0` on Unix through `std::process::Command`, but tests must not depend on real PIDs.

**Step 3: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked mount_daemon::tests::status_reports_stopped_when_pid_and_socket_are_absent --lib -- --exact --nocapture
cargo test --locked mount_daemon::tests::status_reports_stale_pid_when_pid_is_dead_and_socket_exists --lib -- --exact --nocapture
cargo test --locked mount_daemon::tests::status_reports_crashed_when_pid_is_alive_but_socket_is_unreachable --lib -- --exact --nocapture
cargo test --locked mount_daemon::tests::status_reports_running_from_ipc_status_response --lib -- --exact --nocapture
cargo test --locked mount_daemon::tests::status_collapses_io_and_ipc_failures_to_redacted_reason_codes --lib -- --exact --nocapture
git diff --check
```

Commit:

```bash
git add src/mount_daemon.rs
git commit -m "feat: resolve sparse mount daemon status"
```

## Task 4: Add IPC DTOs, Log Views, And Unmount Control

**Files:**

- Modify: `src/mount_daemon.rs`

**Step 1: Write failing control tests**

Add tests:

- `ipc_request_rejects_unknown_version_and_command`
- `logs_returns_bounded_sanitized_tail_without_raw_secrets`
- `unmount_is_idempotent_when_daemon_is_stopped`
- `unmount_cleans_stale_pid_and_socket_metadata`
- `unmount_requests_graceful_shutdown_for_running_daemon`
- `unmount_reports_crashed_daemon_without_invoking_privileged_unmount`

Expected RED:

```bash
cargo test --locked mount_daemon::tests::unmount_cleans_stale_pid_and_socket_metadata --lib -- --exact --nocapture
```

**Step 2: Implement control APIs**

Implement:

- `MountDaemonIpcRequest`, `MountDaemonIpcCommand`, and `MountDaemonIpcResponse`.
- `MountDaemonLogView` with bounded line count.
- Log sanitization helpers for the tested secret-bearing patterns.
- `logs(tag, runtime_root, lines, probes)` controller method.
- `unmount(tag, runtime_root, probes)` controller method matching the unmount semantics above.

Do not call real `umount`, `fusermount`, `/sbin/umount`, `/sbin/mount_nfs`, or `mount`.

**Step 3: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked mount_daemon::tests::ipc_request_rejects_unknown_version_and_command --lib -- --exact --nocapture
cargo test --locked mount_daemon::tests::logs_returns_bounded_sanitized_tail_without_raw_secrets --lib -- --exact --nocapture
cargo test --locked mount_daemon::tests::unmount_is_idempotent_when_daemon_is_stopped --lib -- --exact --nocapture
cargo test --locked mount_daemon::tests::unmount_cleans_stale_pid_and_socket_metadata --lib -- --exact --nocapture
cargo test --locked mount_daemon::tests::unmount_requests_graceful_shutdown_for_running_daemon --lib -- --exact --nocapture
cargo test --locked mount_daemon::tests::unmount_reports_crashed_daemon_without_invoking_privileged_unmount --lib -- --exact --nocapture
git diff --check
```

Commit:

```bash
git add src/mount_daemon.rs
git commit -m "feat: add sparse mount daemon controls"
```

## Task 5: Wire `stratumctl mount` Commands

**Files:**

- Modify: `src/bin/stratumctl.rs`

**Step 1: Write failing CLI tests**

Add tests in the existing `src/bin/stratumctl.rs` test module:

- `mount_status_command_parses_default_tag`
- `mount_logs_command_bounds_requested_lines`
- `mount_unmount_command_parses_runtime_dir`
- `mount_command_rejects_unsafe_tag_before_control_execution`
- `mount_command_text_rendering_redacts_runtime_paths`
- `mount_command_json_rendering_reports_status_without_secrets`

Expected RED:

```bash
cargo test --locked --bin stratumctl mount_status_command_parses_default_tag -- --exact --nocapture
```

If Cargo cannot select bin tests directly in this workspace, use:

```bash
cargo test --locked mount_status_command_parses_default_tag --bin stratumctl -- --exact --nocapture
```

**Step 2: Implement CLI command and renderers**

Extend `Command` with:

- `Command::Mount { command: MountCommand }`

Add:

- `MountCommand::Status { tag, runtime_dir, json }`
- `MountCommand::Logs { tag, runtime_dir, lines, json }`
- `MountCommand::Unmount { tag, runtime_dir, json }`

Wire the command to `src/mount_daemon.rs` using production effect seams. Keep existing HTTP client commands unchanged.

Text rendering should print concise redacted output such as state, backend, pid presence, socket/log presence, and fixed reason code. JSON rendering should serialize the redacted DTOs only.

**Step 3: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked --bin stratumctl mount_status_command_parses_default_tag -- --exact --nocapture
cargo test --locked --bin stratumctl mount_logs_command_bounds_requested_lines -- --exact --nocapture
cargo test --locked --bin stratumctl mount_unmount_command_parses_runtime_dir -- --exact --nocapture
cargo test --locked --bin stratumctl mount_command_rejects_unsafe_tag_before_control_execution -- --exact --nocapture
cargo test --locked --bin stratumctl mount_command_text_rendering_redacts_runtime_paths -- --exact --nocapture
cargo test --locked --bin stratumctl mount_command_json_rendering_reports_status_without_secrets -- --exact --nocapture
git diff --check
```

Commit:

```bash
git add src/bin/stratumctl.rs
git commit -m "feat: add stratumctl mount controls"
```

## Task 6: Preserve Runtime Guards And Snapshot Rollback

**Files:**

- Modify as needed: `src/backend/runtime.rs`
- Modify as needed: `src/bin/stratum_mount.rs`
- Test-only additions as needed in existing modules

**Step 1: Write failing boundary tests if coverage is missing**

Add or extend tests proving:

- `STRATUM_CORE_RUNTIME=durable-cloud` still rejects `NonServerRuntimeSurface::StratumMount`.
- `stratumctl mount` control commands do not open local `.vfs/state.bin`.
- `stratum-mount` still has only snapshot mount behavior and no daemon-control side effects.
- No HTTP capability fixtures or route behavior are changed.

Prefer existing `backend::runtime` tests for the durable-cloud guard. Do not change production behavior unless a test proves an actual gap.

**Step 2: Implement only missing guard coverage**

If existing code already satisfies the boundary, add tests only. If a small implementation fix is needed, keep it narrowly scoped.

**Step 3: Verify and commit**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked backend::runtime --lib -- --nocapture
cargo check --locked --features fuser --bin stratum-mount
git diff --check
```

Commit only if there are changes:

```bash
git add src/backend/runtime.rs src/bin/stratum_mount.rs src/bin/stratumctl.rs
git commit -m "test: preserve mount runtime boundaries"
```

## Task 7: Update Docs And Status

**Files:**

- Modify: `docs/project-status.md`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/plans/2026-05-22-sparse-mount-daemon-ux.md` if final notes are useful

**Step 1: Update docs**

Document:

- Slice 13 completed scope and grounding files.
- `stratumctl mount status|logs|unmount` is a local daemon UX foundation only.
- PID/socket/log/IPC metadata and stale/crashed daemon handling are modeled provider-free.
- macOS default sparse backend remains NFS-over-localhost as a modeled future path, not production cutover.
- `stratum-mount` remains snapshot-only and local-state-only.
- Durable-cloud FUSE/MCP/REPL/non-server surfaces remain fail-closed.
- No HTTP route behavior, committed-read source selection, write-back, or durable sparse mount cutover changed.

**Step 2: Verify docs diff**

Run:

```bash
git diff -- docs/project-status.md docs/http-api-guide.md docs/plans/2026-05-22-sparse-mount-daemon-ux.md
git diff --check
```

**Step 3: Commit**

```bash
git add docs/project-status.md docs/http-api-guide.md docs/plans/2026-05-22-sparse-mount-daemon-ux.md
git commit -m "docs: record sparse mount daemon ux"
```

## Final Verification

Run after all implementation/review fixes:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked mount_daemon::tests --lib -- --nocapture
cargo test --locked --bin stratumctl mount_ -- --nocapture
cargo test --locked mount_adapter::tests --lib -- --nocapture
cargo test --locked sparse_cache::mount::tests --lib -- --nocapture
cargo test --locked sparse_cache::tests --lib -- --nocapture
cargo test --locked sparse_cache::hydration::tests --lib -- --nocapture
cargo check --locked -p stratum-core
cargo test --locked -p stratum-core
cargo check --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
cargo test --locked --features fuser fuse_mount --lib -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

Local live Postgres/R2 portions may skip when provider env is unset. Do not claim protected-provider CI unless it is actually inspected.
