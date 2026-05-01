# Stratum Project Status

- Last updated: 2026-05-01
- Branch: `v2/foundation`
- Baseline merge to `main`: `3b61961` (`Merge branch 'v2/foundation' into main`)
- Latest completed slice: change-request/protected-change foundation

This is a living engineering status file. Keep it factual, repo-grounded, and short enough that a teammate can use it as a starting point before reading the deeper docs.

## Product Positioning

Stratum is currently best described as a versioned workspace for AI agents: durable files, search, permissions, commits, rollback, HTTP access, MCP access, and a remote-first CLI over one shared virtual filesystem.

The near-term product boundary is important:

- It is an agent workspace and control layer for inspectable agent work.
- It is not yet a general sandboxed execution platform.
- It is not positioned as a generic POSIX cloud filesystem replacement.
- The CTO direction points v2 toward a durable, POSIX-compatible, versioned agent filesystem for long-lived enterprise documents, with Rust retained as the core and TypeScript/Python SDKs added later.

Grounding:

- `README.md`
- `docs/agent-workspace-positioning.md`
- `docs/cli-cloud-bridge.md`
- `docs/execution-roadmap.md`

## Completed Foundation Capabilities

The `v2/foundation` branch has moved a meaningful part of the Phase 0 / Milestone 1 foundation into the repo.

### Filesystem And Access Surfaces

- Rust virtual filesystem core remains the product foundation.
- CLI/REPL, HTTP API, MCP server, `stratumctl`, and optional FUSE entry points exist.
- Regular file names are now allowed by default; markdown-only behavior is a compatibility mode through `STRATUM_COMPAT_TARGET=markdown`.
- HTTP API covers filesystem read/write/list/stat, search/find/tree, VCS, workspace metadata, workspace tokens, run-record creation/reads, local audit-event reads, and protected-change control-plane records.
- Most mutating HTTP endpoints now support optional `Idempotency-Key` retries with scoped request fingerprints and replay authorization.
- File stat now exposes MIME type, computed content hash, and bounded custom attrs; HTTP supports metadata updates.
- MCP exposes agent file/search/versioning tools and now supports workspace-mounted scoped sessions.

Grounding: `README.md`, `docs/http-api-guide.md`, `docs/mcp-guide.md`, `src/bin/stratum_mcp.rs`, `src/bin/stratumctl.rs`.

### Auth, ACL, And Scoped Sessions

- User/group/agent-token auth exists with Unix-style permission checks.
- HTTP no longer falls back to implicit root for missing or unsupported auth.
- Scoped session prefixes are enforced centrally in `StratumDb` for reads and writes.
- Workspace bearer tokens carry persisted read/write prefixes.
- Workspace bearer sessions mount the workspace root as `/`, while enforcing backing absolute scopes.
- MCP can resolve `STRATUM_MCP_WORKSPACE_ID` plus `STRATUM_MCP_WORKSPACE_TOKEN` into a scoped non-root mounted session.
- Workspace metadata and workspace token hashes are durable in the local metadata store.

Relevant commits:

- `2f1692a` - enforce scoped API and MCP permissions
- `01aa0dc` - enforce scoped session prefixes
- `63f09fa`, `3650a31`, `67826ed` - persist, constrain, and validate workspace token scopes
- `ae61679` - issue scoped workspace tokens over HTTP
- `1420437` - support scoped workspace tokens in MCP
- `82c7bca` - persist workspace metadata

Grounding: `src/auth/session.rs`, `src/db.rs`, `src/workspace/mod.rs`, `src/server/middleware.rs`, `src/server/routes_workspace.rs`.

### VCS, Refs, Status, And Diff

- Commit/log/revert exist over content-addressed VCS storage.
- Refs foundation exists, including persisted refs and compare-and-swap style ref update behavior.
- HTTP exposes admin-gated ref list/create/update endpoints under `/vcs/refs`.
- Ref updates use expected target plus expected version compare-and-swap semantics; stale updates and duplicate creates return `409 Conflict` without mutation.
- Active protected ref rules block direct commit, revert, and ref-update mutations against matching target refs.
- Session refs use the `agent/<actor>/<session>` namespace.
- Workspace records now carry explicit `base_ref` and `session_ref` ownership fields, and mounted workspace sessions expose the same ref metadata.
- VCS status and text diff foundations exist for human review of changed paths.
- Global VCS HTTP endpoints remain admin-gated.

Relevant commits:

- `20dd0e7` - add VCS refs foundation
- `647b93f` - add VCS status and diff foundation
- `0b460f5` - plan VCS session semantics
- `cae5921` - record workspace ref ownership
- `1232961` - expose VCS refs over HTTP
- `e497327` - address VCS/session review findings

Grounding: `src/vcs/`, `src/server/routes_vcs.rs`, `docs/version-control.md`, `docs/http-api-guide.md`.

## Change Requests And Protected Changes

The change-request/protected-change foundation slice adds the first review control-plane contract before full approval workflows.

What is built:

- `src/review.rs` defines protected ref rules, protected path-prefix rules, change-request records, open/merged/rejected state transitions, and in-memory plus local file-backed stores.
- Local review state is stored at `<STRATUM_DATA_DIR>/.vfs/review.bin` by default, or `STRATUM_REVIEW_PATH` when set.
- The local review store uses a single-writer lock, matching the existing local audit/workspace metadata store pattern.
- HTTP exposes admin-gated endpoints for `GET/POST /protected/refs`, `GET/POST /protected/paths`, `GET/POST /change-requests`, `GET /change-requests/{id}`, `POST /change-requests/{id}/reject`, and `POST /change-requests/{id}/merge`.
- Mutating review endpoints accept optional `Idempotency-Key` values and only replay non-secret JSON responses after current admin authorization succeeds.
- Change-request creation snapshots source and target ref heads as `head_commit` and `base_commit`.
- Change-request merge is a fast-forward contract: source and target refs must still match the recorded head/base commits, then the target ref is compare-and-swap updated to the recorded head.
- Direct protected ref mutations are blocked for `POST /vcs/commit`, `POST /vcs/revert`, and `PATCH /vcs/refs/{name}` when an active matching rule applies.
- Direct protected path mutations are blocked for HTTP file writes, directory creates, metadata patches, deletes, copy destinations, and move source/destination paths when an active matching path-prefix rule applies.
- File writes and metadata patches check both the requested path and the final symlink target they would mutate.
- Deletes and move sources also block ancestor paths that contain protected descendants.
- Protected rule creation and change-request create/reject/merge mutations emit local audit events without persisting request descriptions or file content.
- Review-route merge/reject transitions use a process-local transition lock to avoid same-process merge/reject races in this local foundation.

What is not built:

- No approval records, reviewer identity model, required-approval counting, comments, or approval dismissal.
- No protected-path-aware content merge/rebase; change-request merge is fast-forward only.
- No distributed policy engine or database transaction boundary for multi-node deployments.
- No web review console, notifications, or merge queue.
- No protected-change enforcement through MCP, CLI, POSIX/FUSE, or direct embedded `StratumDb` callers yet.
- No production audit pipeline for policy decisions beyond the local mutation audit events.

Relevant commits:

- `bfe1eed` - plan change request protected paths
- `1804c90` - add review control-plane store
- `6698e74` - add change request http contract
- `72ed1c7` - enforce protected change rules
- `a66a069` - address protected change review findings

Grounding: `src/review.rs`, `src/server/routes_review.rs`, `src/server/routes_fs.rs`, `src/server/routes_vcs.rs`, `src/db.rs`, `docs/http-api-guide.md`, `docs/plans/2026-05-01-change-requests-protected-paths.md`.

## Recent Execution / Run-Record Work

Execution Phase 1 is implemented as run records only.

What is built:

- `src/runs.rs` defines the run-record model, safe run ID validation, canonical file layout, metadata rendering, and tests.
- `POST /runs` creates durable run artifacts in a mounted workspace under `/runs/<run-id>/`.
- `GET /runs/{id}` reads the durable run record summary, including file metadata and bounded content previews.
- `GET /runs/{id}/stdout` and `GET /runs/{id}/stderr` return raw captured output content.
- Standard files are:
  - `prompt.md`
  - `command.md`
  - `stdout.md`
  - `stderr.md`
  - `result.md`
  - `metadata.md`
  - `artifacts/`
- `POST /runs` requires workspace-mounted bearer auth plus `X-Stratum-Workspace`.
- `POST /runs` accepts optional `Idempotency-Key` values. Same-key retries with the same workspace, agent UID, and normalized request body replay the original completed `201 Created` JSON response with `X-Stratum-Idempotent-Replay: true` and do not create another run directory.
- Idempotent run-create replays still validate the current workspace token's run write scope before returning the stored response.
- Run reads require workspace-mounted bearer auth plus read scope for the backing workspace `/runs/<run-id>` path.
- Plain user auth and global bearer sessions are rejected for run creation.
- Supplied run IDs are restricted to ASCII letters, digits, `_`, and `-`; omitted IDs are UUID-based.
- Duplicate run IDs are rejected with `409 Conflict` and do not overwrite existing records unless the request is a matching idempotency replay.
- Idempotency-key reuse with a different request fingerprint returns `409 Conflict` without mutation; invalid idempotency keys return `400 Bad Request` before mutation.
- Writes are scoped through the existing workspace token boundary.
- Success, replay, and error paths are projected back to workspace-relative paths and should not leak backing workspace paths.
- Oversized run file payloads are rejected before creating the run root.
- Phase 1 is explicitly not transactional across all files if a later write fails.

What is not built:

- No command execution.
- No scheduler or queue.
- No stdout/stderr streaming.
- No cancellation.
- No sandbox policy.
- No automatic commit or review workflow around a run.

Relevant commits:

- `5b920d7` - plan run records foundation
- `6e7e47b` - add run record model
- `a40e18c` - harden run record model
- `e0cc8ab` - create workspace run records over HTTP
- `20b560f` - harden run record creation
- `3f8f02c` - format server route module
- `5f14348` - plan run status API
- `08ac155` - add run status model
- `3ac58fe` - read workspace run records over HTTP

Grounding: `docs/execution-roadmap.md`, `docs/http-api-guide.md`, `docs/plans/2026-04-30-run-records.md`, `src/runs.rs`, `src/server/routes_runs.rs`.

The follow-on run status/read API slice has landed against `docs/plans/2026-04-30-run-status-api.md`.

Current status model states are `queued`, `running`, `succeeded`, `failed`, `cancelled`, and `timed_out`. New run records default to `queued` unless imported or externally managed run data provides a specific status.

## CI Foundation

The CI foundation slice adds GitHub Actions workflows for the checks that should protect normal branch and pull-request work.

What is built:

- `.github/workflows/rust-ci.yml` runs on pull requests and pushes to `main` and `v2/**`.
- The default CI gate uses least-privilege `contents: read` permissions.
- Default CI jobs run formatting, clippy with warnings denied, non-perf tests, optional `fuser` compile, and `cargo audit --deny warnings`.
- Workflow actions are pinned to commit SHAs, checkout credentials are not persisted into jobs, and `cargo-audit` is installed at a pinned version.
- `.github/workflows/rust-perf.yml` runs the release-mode perf suites only through manual dispatch and a weekly schedule.
- Rust formatting and clippy cleanup is committed separately from workflow wiring so future CI failures are easier to attribute.
- The audit gate is green without advisory ignores: the direct `bincode` dependency was replaced with a local bounded `serde-wincode`/`wincode` codec wrapper, and `aws-sdk-s3` features were narrowed away from the legacy rustls connector path.

What is intentionally not in the default PR gate:

- Release-mode perf tests, because they are longer-running signal and should not block every normal PR by default.

Grounding: `.github/workflows/rust-ci.yml`, `.github/workflows/rust-perf.yml`, `docs/plans/2026-05-01-ci-foundation.md`.

## Audit Event Scaffolding

The audit-event scaffolding slice adds a local/file-backed audit foundation for mutating HTTP operations.

What is built:

- `src/audit.rs` defines the audit event model, in-memory store, and durable local store.
- Local audit events are stored at `<STRATUM_DATA_DIR>/.vfs/audit.bin` by default, or `STRATUM_AUDIT_PATH` when set.
- Events carry server-assigned ID, sequence, timestamp, actor, optional workspace context, action, resource, outcome, and metadata-only details.
- HTTP mutation success paths emit audit events for filesystem write/mkdir/delete/copy/move, VCS commit/revert/ref create/ref update, workspace creation, workspace-token issuance, and run-record creation.
- `POST /runs` can emit partial `run_create` audit events when run side effects occur before a later failure.
- VCS commit/revert workspace-head update failures emit partial audit events after the VCS mutation succeeds.
- `GET /audit` returns a bounded recent-event list for admin-equivalent `Authorization: User ...` sessions; bearer tokens are rejected, including workspace bearer tokens.
- Audit details intentionally exclude file contents, raw tokens, request bodies, run prompt/command/stdout/stderr/result content, and commit messages.
- If audit persistence fails after a mutation has committed, route responses explicitly include `mutation_committed: true` and `audit_recorded: false`; run-create idempotency reservations are completed rather than aborted after committed side effects.
- The local audit lock is owner-aware on clean shutdown so an old store cannot remove a replacement lock file.

What is not built:

- No production Postgres/event-bus audit pipeline.
- No read/auth/policy-decision audit coverage.
- No tamper-evident hash chain, retention policy, export job, pagination cursor, or path/actor/time filtering.
- No cross-store atomic transaction between filesystem/workspace/VCS mutation state and local audit persistence.
- A crash can still leave a stale local audit lock requiring manual cleanup, matching the current local-file scaffolding nature.

Relevant commits:

- `da50bef` - plan audit event scaffolding
- `42491b3` - add audit event scaffolding
- `ad91be9` - harden audit review findings

Grounding: `src/audit.rs`, `src/server/routes_audit.rs`, `src/server/routes_fs.rs`, `src/server/routes_vcs.rs`, `src/server/routes_workspace.rs`, `src/server/routes_runs.rs`, `docs/http-api-guide.md`, `docs/plans/2026-05-01-audit-event-scaffolding.md`.

## HTTP Idempotency Coverage

The HTTP idempotency coverage slice extends the durable idempotency foundation beyond run creation.

What is built:

- `src/server/idempotency.rs` centralizes HTTP `Idempotency-Key` parsing, conflict responses, and JSON replay responses.
- `PUT /fs/{path}`, `DELETE /fs/{path}`, and `POST /fs/{path}?op=copy|move` accept optional idempotency keys.
- Filesystem fingerprints include the authenticated actor, mounted workspace boundary, resolved paths, operation/query/header semantics, and file body length plus SHA-256 digest for writes. Raw file contents are not persisted in idempotency records.
- `POST /vcs/commit`, `POST /vcs/revert`, `POST /vcs/refs`, and `PATCH /vcs/refs/{name}` accept optional idempotency keys.
- VCS fingerprints include the authenticated actor, optional workspace header, commit/revert/ref payload, ref path name, expected ref target, and expected ref version.
- `POST /workspaces` accepts optional idempotency keys and replays the original workspace creation response without creating duplicate workspace records.
- Same-key/same-fingerprint retries replay the original JSON response with `X-Stratum-Idempotent-Replay: true` and do not append another mutation audit event.
- Same-key/different-fingerprint retries return `409 Conflict` without mutation; duplicate in-progress requests also return `409 Conflict`.
- No-mutation failures abort reservations. Committed success, committed partial/failure, and post-mutation audit-failure responses complete reservations with the exact client-visible response.
- Replay paths re-authorize current credentials and current resource access before returning the stored response.
- `POST /workspaces/{id}/tokens` rejects `Idempotency-Key` for now because its success response contains a raw `workspace_token`; secret-aware replay storage is required before this can be enabled safely.
- `src/db.rs` now exposes read-only preflight checks for delete, copy, and move so keyed filesystem requests do not reserve keys before matching the real mutation authorization path.
- `PATCH /fs/{path}` now also participates in HTTP idempotency for metadata-only updates.

What is not built:

- No TTL, pruning, or quota controls for the local idempotency store.
- No encrypted/KMS-backed replay storage for responses containing raw secrets.
- No distributed idempotency coordination beyond the current local durable store.

Relevant commits:

- `574e78a` - plan HTTP idempotency coverage
- `b01a94c` - extract HTTP idempotency helpers
- `1b827ed` - extend HTTP idempotency coverage
- `4348579` - address idempotency review findings

Grounding: `src/idempotency.rs`, `src/server/idempotency.rs`, `src/server/routes_fs.rs`, `src/server/routes_vcs.rs`, `src/server/routes_workspace.rs`, `src/server/routes_runs.rs`, `src/db.rs`, `docs/http-api-guide.md`, `docs/plans/2026-05-01-http-idempotency-coverage.md`.

## File Metadata Foundation

The file metadata foundation slice closes the Phase 1 gap for MIME type, content hash, and custom attrs on the local filesystem/stat/API surfaces.

What is built:

- `Inode` stores `mime_type` and bounded string `custom_attrs` with explicit local-state migration from pre-metadata v5 state.
- `VirtualFs::stat` returns `mime_type`, `content_hash`, and `custom_attrs`; `content_hash` is computed on demand from current bytes as `sha256:<hex>` and is not cached.
- Metadata updates touch `changed`/ctime without changing file content or `modified`/mtime.
- Copy/move/link behavior follows inode semantics: copies get an independent metadata copy; moves and hard links preserve the inode metadata.
- `PUT /fs/{path}` accepts `X-Stratum-Mime-Type` and preserves existing MIME metadata when the header is absent.
- Raw `GET /fs/{path}` returns stored MIME as `Content-Type`, defaulting to `application/octet-stream`.
- `PATCH /fs/{path}` updates MIME/custom attrs on existing paths with write authorization, optional `Idempotency-Key`, and metadata-only audit details that omit attr values.
- Metadata PATCH responses include attr keys but not attr values so local idempotency replay records do not persist custom attr values.
- DB-level metadata updates follow symlinks to the same final target that content writes use.
- VCS tree objects, status, changed paths, text diff output, and revert preserve MIME/custom attrs; legacy pre-metadata tree objects decode with empty metadata.

What is not built:

- No FUSE xattr surface for `getxattr`, `setxattr`, `listxattr`, or `removexattr`.
- No automatic MIME sniffing or extension inference.
- No cloud/Postgres metadata backend yet.

Relevant commits:

- `4921ad6` - plan file metadata foundation
- `c3d59bc` - add file metadata foundation
- `d19b5d5` - address file metadata review findings

Grounding: `src/fs/inode.rs`, `src/fs/mod.rs`, `src/db.rs`, `src/server/routes_fs.rs`, `src/store/tree.rs`, `src/vcs/`, `src/persist.rs`, `docs/http-api-guide.md`, `docs/plans/2026-05-01-file-metadata.md`.

## Verification Status

Verified on 2026-04-30 from this worktree:

```bash
cargo test --locked
```

Result: passed.

Observed coverage in that run included:

- `src/lib.rs` unit tests: 120 passed.
- `stratum_mcp` unit tests: 8 passed.
- `stratumctl` unit tests: 1 passed.
- integration tests: 131 passed.
- perf tests: 37 passed.
- perf comparison test: 1 passed.
- permission tests: 72 passed.
- doc tests: 0 run, passed.

Additional focused verification after adding the run status model:

```bash
cargo test --locked runs::tests -- --nocapture
```

Result: passed, 17 tests.

Additional focused verification after adding run create idempotency:

```bash
cargo test --locked server::routes_runs::tests -- --nocapture
git diff --check -- src/server/routes_runs.rs docs/http-api-guide.md docs/project-status.md
rustfmt --edition 2024 --check src/server/routes_runs.rs
```

Result: passed, 26 route tests; diff check and rustfmt check passed.

CI foundation verification uses the same command set as the default workflow plus the optional FUSE compile and security audit:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --lib --bins
cargo test --locked --test integration --test permissions
cargo test --locked --doc
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-01: passed from this worktree. Observed coverage included 153 lib tests, 8 MCP unit tests, 1 `stratumctl` unit test, 131 integration tests, 72 permission tests, 0 doc tests, optional `stratum-mount` FUSE compile, and `cargo audit --deny warnings` scanning 387 dependencies with no denied findings.

Focused VCS/session semantics verification during implementation:

```bash
cargo test --locked server::routes_vcs::tests -- --nocapture
cargo test --locked workspace::tests -- --nocapture
```

Result on 2026-05-01: passed from this worktree. Observed coverage included VCS ref HTTP create/list/update tests, duplicate and stale CAS conflict behavior, scoped workspace bearer rejection for global ref management, workspace ref ownership defaults, v1/v2 workspace metadata migration, and session-ref namespace validation.

Full current-HEAD verification for the VCS/session semantics slice:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-01: passed from this worktree. Observed coverage included 164 lib tests, 8 MCP unit tests, 1 `stratumctl` unit test, 131 integration tests, 37 perf tests, 1 perf comparison test, 72 permission tests, 0 doc tests, optional `stratum-mount` FUSE compile, `cargo audit --deny warnings` scanning 387 dependencies with no denied findings, and `git diff --check`.

Focused audit-event scaffolding verification during implementation and review fixes:

```bash
cargo test --locked audit -- --nocapture
cargo test --locked --lib routes_runs -- --nocapture
cargo test --locked --lib routes_vcs -- --nocapture
cargo test --locked --lib
cargo clippy --locked --all-targets -- -D warnings
```

Result on 2026-05-01: passed from this worktree. Observed coverage included the local audit store reload/corrupt/persist-failure/lock-owner tests, admin-only audit route tests, filesystem/VCS/workspace/run audit emission tests, run partial-audit/idempotency replay tests, VCS partial workspace-head update tests, and 179 total library tests.

Full current-HEAD verification for the audit-event scaffolding slice:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-01: passed from this worktree. Observed coverage included 179 lib tests, 8 MCP unit tests, 1 `stratumctl` unit test, 131 integration tests, 37 perf tests, 1 perf comparison test, 72 permission tests, 0 doc tests, optional `stratum-mount` FUSE compile, `cargo audit --deny warnings` scanning 387 dependencies with no denied findings, and `git diff --check`.

Focused HTTP idempotency coverage verification during implementation:

```bash
cargo test --locked --lib routes_runs -- --nocapture
cargo test --locked --lib routes_fs -- --nocapture
cargo test --locked --lib routes_vcs -- --nocapture
cargo test --locked --lib routes_workspace -- --nocapture
```

Result on 2026-05-01: passed from this worktree. Observed coverage included the shared HTTP idempotency helper on run creation, filesystem write/delete/copy/move replay and conflict behavior, non-writable copy/move destination replay cases, VCS commit/revert/ref replay and conflict behavior, workspace creation replay and audit-failure replay, and explicit workspace-token idempotency rejection.

Focused file metadata foundation verification during implementation:

```bash
cargo test --locked metadata:: -- --nocapture
cargo test --locked persist:: -- --nocapture
cargo test --locked routes_fs -- --nocapture
cargo test --locked vcs::test_vcs_tracks_and_restores_file_metadata -- --nocapture
cargo test --locked test_diff_reports_metadata_only_file_changes -- --nocapture
cargo test --locked db::tests::set_metadata_as_updates_symlink_target -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
git diff --check
```

Result on 2026-05-01: passed from this worktree. Observed coverage included fresh stat content hashes across write/truncate/handle writes, MIME/custom attr stat output, copy/move/hard-link metadata semantics, v5 local-state and legacy tree-object metadata migration, VCS metadata status/revert/diff, HTTP MIME header/raw content-type behavior, idempotent audited metadata PATCH with attr values omitted from replay responses, explicit `mime_type: null` clearing behavior, and symlink-target metadata updates through `StratumDb`.

Full current-HEAD verification for the file metadata foundation slice:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-01: passed from this worktree. Observed coverage included 201 lib tests, 8 MCP unit tests, 1 `stratumctl` unit test, 136 integration tests, 37 perf tests, 1 perf comparison test, 72 permission tests, 0 doc tests, optional `stratum-mount` FUSE compile, `cargo audit --deny warnings` scanning 387 dependencies with no denied findings, clippy with warnings denied, formatting check, and whitespace diff check.

Focused protected-change foundation verification during implementation and review fixes:

```bash
cargo test --locked review::tests -- --nocapture
cargo test --locked server::routes_review::tests -- --nocapture
cargo test --locked server::routes_vcs::tests::protected_ref_rules_block_direct_vcs_mutations -- --nocapture
cargo test --locked server::routes_fs::tests::protected_path_rules_block_direct_http_writes -- --nocapture
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
git diff --check
```

Result on 2026-05-01: passed from this worktree. Observed coverage included review-store validation and local locking, protected-rule and change-request HTTP authorization/idempotency, fast-forward merge/reject status transitions, protected ref enforcement for direct VCS mutations, protected HTTP path enforcement for writes/metadata/deletes/copy/move, symlink-target protection for file writes/metadata patches, ancestor delete/move blocking, formatting, clippy with warnings denied, and whitespace diff check.

Full protected-change foundation verification:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-01: passed from this worktree. Observed coverage included 218 lib tests, 8 MCP unit tests, 1 `stratumctl` unit test, 136 integration tests, 37 perf tests, 1 perf comparison test, 72 permission tests, 0 doc tests, optional `stratum-mount` FUSE compile, `cargo audit --deny warnings` scanning 387 dependencies with no denied findings, clippy with warnings denied, formatting check, and whitespace diff check.

## Known Residual Risks

- Local durability is still file-backed metadata/state, not the CTO-plan target of Postgres metadata plus S3/R2 object storage.
- Scoped ACL enforcement has broad tests now, but the long-term policy service, action capabilities, policy decision logging, and tenant isolation model are not built.
- Refs/status/diff and protected-change semantics are foundation-level; full approval workflows, merge queues, distributed policy decisions, and protected-change enforcement outside HTTP routes are not complete.
- Run records are useful audit artifacts, but they do not prove safe execution because no runner or sandbox exists yet.
- Run-record creation is not fully atomic across all files.
- Search remains a filesystem/search surface, not the full-text plus semantic derived index described in the v2 plan.
- Audit events are local/file-backed scaffolding only; there is no production audit pipeline for auth/read/policy/approval decisions or durable event-bus/Postgres ingestion.
- Workspace-token issuance intentionally rejects idempotency keys until secret-aware replay storage exists.
- File metadata is available through stat/HTTP/VCS/local persistence, but FUSE xattr mapping and automatic MIME inference are not built.
- Cloud deployment scaffolding exists, but production multi-tenant backend, observability, idempotency retention/quota controls, KMS/secrets posture, and private-beta hardening remain future work.

## Not Built Yet

From the CTO plan and current repo docs, these are the major missing v2 pieces:

- Durable cloud backend: Postgres metadata, S3/R2 object store, idempotent object upload, atomic ref updates.
- Repo/session domain model beyond the current workspace/ref ownership foundation.
- Approval records, reviewer identity, comments, protected-change review UI, merge queues, and protected-change enforcement beyond HTTP route-level gates.
- Full audit event pipeline beyond the local mutating-operation scaffold.
- TypeScript SDK and Python SDK.
- Full POSIX/FUSE metadata compatibility, including xattr mapping for MIME/custom attrs and remote sparse mount cache correctness guarantees.
- Full-text extraction workers and ACL-aware semantic search.
- Web console for browsing, diffs, approvals, audit, and access management.
- Execution Phase 2+: job runner, lifecycle status transitions, output streaming, cancellation, timeouts, sandbox policy, and artifact limits.

## Recommended Next Slices

Recommended order, keeping risk and the CTO plan in mind:

1. Tighten POSIX compatibility around metadata/xattrs and FUSE behavior now that inode/stat metadata exists.
2. Extend protected-change semantics into approval records and policy decisions only after the review identity model is clear.
3. Expand audit coverage to auth/read/policy decisions and move audit persistence toward the future Postgres/event-bus pipeline.
4. Start cloud storage abstraction work behind the existing local backend rather than rewriting the Rust core.
5. Add secret-aware workspace-token idempotency only after the replay storage and KMS/secrets posture are explicit.
6. Continue execution phase 2 only after idempotency, protected-change contracts, and audit semantics are clearer.

## Branch And Release Status

- Branch: `v2/foundation`.
- Remote tracking branch: `origin/v2/foundation`.
- Before the current protected-change slice, `main` and `v2/foundation` were synced and pushed at merge commit `3b61961` after the file metadata foundation slice.
- `v2/foundation` now contains the VCS/session semantics, audit-event scaffolding, HTTP idempotency coverage, CI foundation, file metadata foundation, and protected-change foundation slices after that merge.
- This branch appears to be foundation work, not a release branch.
- No release tag or packaged v2 artifact was identified during this status pass.

## Updating This File

Update this file whenever a meaningful slice lands or a major assumption changes.

Suggested update rules:

- Change `Last updated`, branch, and HEAD when updating status.
- Add new completed slices with file and commit references.
- Move items from "Not Built Yet" into completed sections only after code and tests land.
- Keep verification factual: include exact command, date, and pass/fail result.
- Keep roadmap statements tied to repo docs, the CTO plan, or committed code.
- Do not use this file for speculative product claims or external market claims.
- Keep it concise; link to deeper docs instead of duplicating them.
