# Stratum Project Status

- Last updated: 2026-05-04
- Branch: `v2/foundation`
- Backend work branch: `v2/foundation`
- Baseline on `v2/foundation` before the latest backend slice: `7f54467` (`docs: record durable startup verification`)
- Latest completed backend slice: Durable runtime control-plane cutover (`STRATUM_BACKEND=durable` with Postgres-backed workspace/idempotency/audit/review stores)
- Latest completed SDK slice: TypeScript in-process mount in `@stratum/sdk` with `@stratum/bash` on shared mount primitives; opt-in live smoke harness for TS mount, `@stratum/bash`, and Python (`docs/plans/2026-05-03-sdk-live-smoke-harness.md`)
- Planned next SDK slice: semantic-search parity, published package releases, optional async SDK

This is a living engineering status file. Keep it factual, repo-grounded, and short enough that a teammate can use it as a starting point before reading the deeper docs.

## Completed TypeScript SDK Foundation Slice

The SDK/DX lane landed the reusable `@stratum/sdk` package and refactored `@stratum/bash` to consume it instead of carrying duplicate HTTP client code.

Completed scope:

- Add `sdk/typescript` as `@stratum/sdk`, a TypeScript-first client for the current Stratum HTTP API.
- Cover filesystem, search, VCS, review/change-request, run-record, and workspace-token workflows without changing Rust server behavior.
- Refactor `sdk/bash` so its virtual shell uses `@stratum/sdk` instead of owning duplicate HTTP route/auth/error code.
- Keep semantic search explicit as unsupported until the backend has the derived full-text/vector index described in `docs/semantic-index.md`.

Grounding:

- `docs/plans/2026-05-02-typescript-sdk-foundation.md`
- `docs/http-api-guide.md`
- `sdk/bash`
- SMFS reference repo at `/Users/rajattiwari/virtualfilesystem/smfs`

Current SDK foundation progress:

- `sdk/typescript` now contains the `@stratum/sdk` package with TypeScript, Bun, Vitest, ESM output, and source maps.
- `sdk/package.json` now defines a private Bun workspace for the SDK packages, with a shared `sdk/bun.lock`.
- `StratumClient` exposes `fs`, `search`, `vcs`, `reviews`, `runs`, and `workspaces` resource clients for the currently implemented HTTP API.
- The SDK supports user, bearer, and workspace-bearer auth; safe filesystem/tree/ref route construction; required ref compare-and-swap fields; typed HTTP errors; generated or caller-supplied idempotency keys; and an explicit unsupported semantic-search boundary.
- `sdk/bash` now depends on `@stratum/sdk` for HTTP auth, route construction, response typing, idempotency, path indexing, session caching, and the `StratumVolume` in-process mount while retaining its bash-specific `StratumFs`, command, error-translation, and `just-bash` layers.
- `createBash` preserves bash-originated idempotency keys with the `stratum-bash` prefix.
- Package release dry-runs build only expected `dist`, README, and package metadata through `prepack`; the root `sdk` Bun workspace controls local install, typecheck, test, and build order.
- Remaining SDK work is semantic search once the backend derived index lands, broader integration examples, published package releases (`stratum-sdk` on PyPI), and an AsyncStratumClient once the synchronous API stabilizes.

## Completed TypeScript In-Process Mount Slice

Delivered from `docs/plans/2026-05-02-typescript-in-process-mount.md`; Rust server behavior untouched.

Completed scope:

- Promote cwd-aware mount path normalization, workspace-relative client-path conversion, `PathIndex`, `SessionCache`, and `StratumVolume` from `@stratum/bash` into `@stratum/sdk`.
- Add `StratumClient.mount(options?)` as the ergonomic TypeScript entry point for tools that want a process-local mounted workspace without FUSE or a shell.
- Preserve binary-safe read/write caching with defensive `Uint8Array` copies, root stat synthesis, normalized cwd handling, parent-list invalidation, and filesystem/search/VCS helper delegation.
- Refactor `@stratum/bash` local mount modules into compatibility shims that re-export the shared SDK primitives while bash continues to own `StratumFs`, commands, and `just-bash` wiring.

Verification (local worktree):

```bash
cd sdk
bun install --frozen-lockfile
bun run typecheck
bun run test:run
bun run build
cd typescript && npm pack --dry-run
cd ../bash && npm pack --dry-run
cd ../..
cargo test --locked --no-run
git diff --check
```

Result on 2026-05-02: passed after review fixes.

Grounding:

- `docs/plans/2026-05-02-typescript-in-process-mount.md`
- `sdk/typescript/src/mount.ts`
- `sdk/typescript/src/mount-cache.ts`
- `sdk/typescript/src/mount-paths.ts`
- `sdk/bash/src/volume.ts`

## Completed SDK Live Smoke Harness Slice

Delivered from `docs/plans/2026-05-03-sdk-live-smoke-harness.md`; Rust server behavior untouched.

Completed scope:

- Share `sdk/typescript/tests/live-helpers.ts` (`liveConfigOrSkip`, `createLiveWorkspace`) for Vitest live tests; bash reuses it via `tsconfig.test.json` includes.
- `sdk/typescript/tests/live-smoke.test.ts` plus `bun run test:live` — workspace create + token issue via admin user and env agent token; workspace-bearer filesystem, grep/find/tree, run records, admin-gated VCS status/diff, and `UnsupportedFeatureError` on `search.semantic()`.
- `sdk/bash/tests/live-smoke.test.ts` plus `test:live` — `StratumVolume` / `client.mount()` cache observation (GET `/fs/` counting), volume and shell admin-boundary-aware `status` / `diff`, virtual `pwd` / `cat` / `grep`, and `sgrep` unsupported boundary.
- `sdk/python/tests/test_live_smoke.py` — same contract with `UserAuth` / `WorkspaceAuth` and pytest skips when live env is absent.
- Runnable examples: `sdk/typescript/examples/live-workspace.ts`, `sdk/bash/examples/live-bash.ts`, `sdk/python/examples/live_workspace.py`; SDK READMEs and `docs/getting-started.md` document `STRATUM_SDK_LIVE*` and that workspace tokens must not be logged.

Verification (local worktree):

```bash
cd sdk
bun install --frozen-lockfile
bun run typecheck
bun run test:run
bun run build
cd python
python -m pytest
python -m mypy src/stratum_sdk
python -m ruff check src tests
python -m ruff format --check src tests
cd ../..
cargo test --locked --no-run
git diff --check
```

Opt-in live runs (when `stratum-server` and agent token are available) use the `test:live` scripts in `sdk/typescript` and `sdk/bash`, and `pytest tests/test_live_smoke.py` under `sdk/python`, with `STRATUM_SDK_LIVE=1` and the documented `STRATUM_SDK_LIVE_*` variables.

Grounding:

- `docs/plans/2026-05-03-sdk-live-smoke-harness.md`
- `sdk/typescript/tests/live-smoke.test.ts`
- `sdk/bash/tests/live-smoke.test.ts`
- `sdk/python/tests/test_live_smoke.py`

## Completed Python SDK Foundation Slice

Delivered coverage from `docs/plans/2026-05-02-python-sdk-foundation.md`; Rust server behavior untouched.

Completed scope:

- Add `sdk/python` as publication-name `stratum-sdk` (`import stratum_sdk`), Hatchling/pyproject metadata, synchronous `StratumHttpClient`, and pragmatic `TypedDict` JSON shapes aligned with `@stratum/sdk`.
- Mirror `fs`, `search`, `vcs`, `reviews`, `runs`, and `workspaces` clients plus top-level ergonomics compatible with `@stratum/sdk`.
- Preserve user/bearer/workspace auth headers; safe filesystem/tree normalization; dot-segment escaping for `/vcs/refs/` updates; SDK-generated visible-ASCII bounded idempotency keys; no `Idempotency-Key` on workspace-token issuance.
- Cover behavior with pytest + httpx `MockTransport` (no spawned `stratum-server` in-repo tests).

Verification (local worktree):

```bash
cd sdk/python
python -m pytest
python -m mypy src/stratum_sdk
python -m ruff check src tests
python -m ruff format --check src tests
python -m build
tmpdir="$(mktemp -d)"
python -m venv "$tmpdir/venv"
"$tmpdir/venv/bin/python" -m pip install dist/stratum_sdk-0.0.0-py3-none-any.whl
"$tmpdir/venv/bin/python" - <<'PY'
from stratum_sdk import StratumClient, __version__

assert __version__ == "0.0.0"
assert StratumClient is not None
PY
rm -rf "$tmpdir"
cd ../..
git diff --check
```

Result on 2026-05-02: passed (29 pytest tests; isolated wheel smoke import OK; git diff whitespace check clean).

Grounding:

- `docs/plans/2026-05-02-python-sdk-foundation.md`
- `sdk/python`
- `sdk/typescript`
- `docs/http-api-guide.md`

## Completed Bash SDK Slice

The TypeScript virtual bash SDK lane landed from `sdk/typescript-virtual-bash` and adds a standalone package at `sdk/bash`.

Current intent:

- Add a standalone `sdk/bash` package for `@stratum/bash`.
- Adapt the SMFS virtual bash SDK shape to Stratum's existing HTTP workspace bearer API.
- Keep the SDK independent of the Rust runtime/backend cutover work.
- Use workspace bearer headers, Stratum unrestricted path behavior, existing filesystem/search/tree endpoints, VCS helper commands that surface the server's admin boundary, and idempotency keys for writes.
- Reserve semantic `sgrep` behavior until Stratum has the derived semantic-search/indexing layer.

Grounding:

- `docs/plans/2026-05-02-typescript-virtual-bash-sdk.md`
- SMFS reference repo at `/Users/rajattiwari/virtualfilesystem/smfs`

Current SDK progress:

- `sdk/bash` package scaffold exists with Bun, TypeScript, Vitest, and `just-bash`.
- `StratumClient` covers workspace bearer auth, filesystem read/write/list/stat, raw byte reads/writes, copy/move/delete, grep/find/tree, and VCS status/diff/commit calls that may return the current server's admin-gated permission errors.
- Client route construction normalizes dot segments before URL construction so filesystem paths cannot escape `/fs` or `/tree`.
- `PathIndex`, `SessionCache`, and `StratumVolume` provide cwd-aware path normalization, TTL/LRU read/stat/list caching, root stat synthesis, byte-safe read caching, and cache invalidation for mutations.
- `StratumFs` implements the `just-bash` filesystem interface over `StratumVolume`, including file reads/writes/appends, mkdir/rm/cp/mv, directory reads, POSIX-like errors, and clear unsupported-link/metadata errors.
- `createBash` wires `StratumClient`, `StratumVolume`, `StratumFs`, and `just-bash` with custom Stratum `status`, `diff`, `commit`, `grep`, and unsupported `sgrep` commands.
- Final review fixes cover binary byte safety, non-recursive mkdir parent checks, package build hooks, and defensive binary cache copies.

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
- HTTP API covers filesystem read/write/list/stat, search/find/tree, VCS, workspace metadata, workspace tokens, run-record creation/reads, local audit-event reads, protected-change control-plane records, approval records, reviewer assignments, review comments, and approval dismissal.
- Most mutating HTTP endpoints now support optional `Idempotency-Key` retries with scoped request fingerprints and replay authorization.
- File stat now exposes MIME type, computed content hash, and bounded custom attrs; HTTP supports metadata updates.
- POSIX/FUSE exposes Stratum MIME/custom metadata through Stratum metadata-backed user xattrs.
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

## Change Requests, Protected Changes, Approvals, And Feedback

The change-request/protected-change foundation now includes approval records, approval dismissal, durable review comments, reviewer assignments, and the first approval-policy contract before full review workflows.

What is built:

- `src/review.rs` defines protected ref rules, protected path-prefix rules, change-request records, approval records, reviewer assignments, review comments, approval dismissal, computed approval policy decisions, open/merged/rejected state transitions, and in-memory plus local file-backed stores.
- Local review state is stored at `<STRATUM_DATA_DIR>/.vfs/review.bin` by default, or `STRATUM_REVIEW_PATH` when set.
- The local review store uses a single-writer lock, matching the existing local audit/workspace metadata store pattern, and migrates v1/v2/v3 review stores to v4 with empty reviewer assignments where needed.
- HTTP exposes admin-gated endpoints for `GET/POST /protected/refs`, `GET/POST /protected/paths`, `GET/POST /change-requests`, `GET /change-requests/{id}`, `GET/POST /change-requests/{id}/approvals`, `GET/POST /change-requests/{id}/reviewers`, `GET/POST /change-requests/{id}/comments`, `POST /change-requests/{id}/approvals/{approval_id}/dismiss`, `POST /change-requests/{id}/reject`, and `POST /change-requests/{id}/merge`.
- Mutating review endpoints accept optional `Idempotency-Key` values and only replay non-secret JSON responses after current admin authorization succeeds.
- Change-request creation snapshots source and target ref heads as `head_commit` and `base_commit`.
- Change-request read/list/create/reject/merge responses include computed `approval_state`.
- Approval records are bound to a change request and captured `head_commit`; duplicate active approval by the same approver for the same head returns the existing record with `created: false`, and new approvals are limited to open change requests.
- Reviewer assignments are durable active records keyed by change request and reviewer UID, can be required or optional, are limited to open change requests, require admin-equivalent users for new assignments and upgrades to required while still allowing existing assignments to be downgraded if a reviewer loses approval rights, reject assignment of the change-request author, and update the existing assignment plus version when the required flag changes.
- Approval dismissal is limited to open change requests, marks an active approval inactive, records `dismissed_by` plus an optional stored reason, increments the approval version, returns `dismissed: true`, and immediately removes that approval from computed approval counts. Re-dismissing an inactive approval returns the same inactive record with `dismissed: false`.
- Review comments are durable records with `general` or `changes_requested` kind, author UID, optional normalized path, trimmed bounded body text, active flag, and version. New review comments are limited to open change requests.
- Approval policy decisions are computed from active protected ref rules matching the target ref, active protected path rules matching changed paths between the recorded base/head commits, and active required reviewer assignments.
- Effective required approvals is the maximum required count across matching rules, only active approvals for the current recorded head count, and required reviewer assignments must be satisfied by approvals from those exact reviewer UIDs.
- Change-request merge is a fast-forward contract: source and target refs must still match the recorded head/base commits, the recorded head must descend from the recorded base, approval state must be approved, then the target ref is compare-and-swap updated to the recorded head while source freshness is rechecked under the same local DB write lock.
- Direct protected ref mutations are blocked for `POST /vcs/commit`, `POST /vcs/revert`, and `PATCH /vcs/refs/{name}` when an active matching rule applies.
- Direct protected path mutations are blocked for HTTP file writes, directory creates, metadata patches, deletes, copy destinations, move source/destination paths, and HTTP VCS reverts that would touch protected paths on `main` when an active matching path-prefix rule applies.
- File writes and metadata patches check both the requested path and the final symlink target they would mutate.
- Deletes and move sources also block ancestor paths that contain protected descendants.
- Protected rule creation, approval creation, reviewer assignment, review-comment creation, approval dismissal, and change-request create/reject/merge mutations emit local audit events without persisting request descriptions, approval comments, review-comment bodies, dismissal reasons, or file content.
- Review-route approval/comment/dismiss/reviewer-assignment/merge/reject mutations use conservative terminal-state checks and idempotency replay ordering so matching retries can replay after merge/reject while new terminal mutations are rejected.
- Review-route merge/reject transitions use a process-local transition lock to avoid same-process terminal-state races in this local foundation.

What is not built:

- No reviewer groups, threaded replies, comment resolution, or review UI.
- No protected-path-aware content merge/rebase; change-request merge is fast-forward only.
- No distributed policy engine or database transaction boundary for multi-node deployments.
- No web review console, notifications, or merge queue.
- No protected-change enforcement through MCP, CLI, POSIX/FUSE, or direct embedded `StratumDb` callers yet.
- No durable production audit pipeline for policy decisions beyond the local mutation audit events.

Relevant commits:

- `bfe1eed` - plan change request protected paths
- `1804c90` - add review control-plane store
- `6698e74` - add change request http contract
- `72ed1c7` - enforce protected change rules
- `a66a069` - address protected change review findings
- `2ecf3e3` - plan approval policy foundation
- `0de8a41` - add approval records to review store
- `8f5ac10` - expose changed paths between commits
- `4d11195` - enforce change request approvals
- `a07f543` - plan review feedback foundation
- `66e13a9` - add review comments and approval dismissal
- `1674eb3` - expose review feedback endpoints
- `ddd1b60` - plan reviewer assignment foundation
- `82d462e` - add reviewer assignment store foundation
- `f3cd827` - expose reviewer assignment endpoints

Grounding: `src/review.rs`, `src/server/routes_review.rs`, `src/server/routes_fs.rs`, `src/server/routes_vcs.rs`, `src/db.rs`, `src/vcs/mod.rs`, `docs/http-api-guide.md`, `docs/plans/2026-05-01-change-requests-protected-paths.md`, `docs/plans/2026-05-01-approval-policy-foundation.md`, `docs/plans/2026-05-01-review-feedback-foundation.md`, `docs/plans/2026-05-01-reviewer-assignment-foundation.md`.

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
- POSIX exposes metadata xattrs through `PosixFs::{listxattr,getxattr,setxattr,removexattr}` with read permission required for list/get and write permission required for set/remove.
- Optional FUSE maps those POSIX xattrs through `getxattr`, `setxattr`, `listxattr`, and `removexattr` inside the mounted filesystem snapshot.
- Stable xattr names are `user.stratum.mime_type` and `user.stratum.custom.<key>`.
- FUSE honors normal xattr sizing semantics, `XATTR_CREATE`/`XATTR_REPLACE` flags, `NO_XATTR` for missing backed attrs, `ENOTSUP` for unsupported xattr names, and `ERANGE` for undersized get/list buffers.

What is not built:

- No automatic MIME sniffing or extension inference.
- No arbitrary binary xattrs or native platform xattr persistence beyond Stratum's string metadata fields.
- `stratum-mount` still mounts `db.snapshot_fs()`, so FUSE writes and xattr mutations are not yet persisted back into `StratumDb` or represented in HTTP audit events.
- No remote sparse FUSE cache correctness guarantees.
- No runtime cloud/Postgres metadata backend cutover yet.

Relevant commits:

- `4921ad6` - plan file metadata foundation
- `c3d59bc` - add file metadata foundation
- `d19b5d5` - address file metadata review findings
- `c9b4408` - plan posix fuse xattrs
- `7c4c311` - add posix metadata xattrs
- `9f6fbb7` - expose metadata xattrs over fuse

Grounding: `src/fs/inode.rs`, `src/fs/mod.rs`, `src/posix.rs`, `src/fuse_mount.rs`, `src/db.rs`, `src/server/routes_fs.rs`, `src/store/tree.rs`, `src/vcs/`, `src/persist.rs`, `docs/http-api-guide.md`, `docs/plans/2026-05-01-file-metadata.md`, `docs/plans/2026-05-01-posix-fuse-xattrs.md`.

## Durable Backend Foundation

The durable backend foundation starts Milestone 2 conservatively by defining the storage contracts before cutting the runtime over to cloud services.

What is built:

- `src/backend/mod.rs` defines object, commit, and ref store contracts with local in-memory conformance adapters.
- `RepoId::local()` represents the current single-repo local runtime while leaving room for future multi-repo metadata.
- Object storage contracts keep `ObjectId = sha256(raw_bytes)`, require kind checks, and make same-object writes idempotent.
- Commit metadata contracts store commit IDs, root tree IDs, parent commit IDs, author/message/timestamp, and changed paths separately from object bytes.
- Ref contracts use explicit `RefExpectation` values and `RefVersion` counters for compare-and-swap updates.
- Source-checked ref updates are modeled as a single store operation so future Postgres implementations can preserve change-request merge freshness checks transactionally.
- `StratumStores::local_memory()` composes the new object/commit/ref stores with the existing workspace metadata, review, idempotency, and audit store traits.
- `migrations/postgres/0001_durable_backend_foundation.sql` records the first Postgres schema plan for repos, objects, object cleanup claims, commits, refs, idempotency records, audit events, workspace metadata, workspace tokens, protected rules, change requests, approvals, review comments, and reviewer assignments.
- `migrations/postgres/0002_review_local_commit_ids.sql` removes the change-request-to-Postgres-commit foreign keys so durable review rows can capture local core VCS commit IDs until the core filesystem/VCS runtime is cut over.
- The optional `postgres` feature now exposes a Postgres metadata adapter that proves object metadata, object cleanup claim, commit metadata, and ref compare-and-swap contracts against the migration schema.

What is not built:

- No core filesystem/VCS runtime cutover to the Postgres object, commit, or ref adapters; only the current durable control-plane server path uses Postgres stores.
- No runtime S3/R2 object-store cutover behind the new object contract.
- No Redis/distributed lock layer.
- No cross-store transaction spanning filesystem state, refs, idempotency, review, workspace metadata, and audit.
- No normalized POSIX inode/path table or sparse remote FUSE cache.
- The default HTTP API behavior is still local; `STRATUM_BACKEND=durable` with the `postgres` feature now uses Postgres for workspace metadata, idempotency, audit, and review state only.

Grounding: `src/backend/mod.rs`, `migrations/postgres/0001_durable_backend_foundation.sql`, `docs/plans/2026-05-01-durable-backend-foundation.md`, `markdownfs_v2_cto_architecture_plan.md`.

## Backend Adapter Scaffolding

The backend adapter scaffolding slice starts connecting the contract layer to the existing remote byte-store abstraction without changing server behavior.

What is built:

- A typed byte-backed object adapter now maps `ObjectStore` operations onto the existing `RemoteBlobStore` abstraction using repo-scoped, kind-scoped, content-addressed object keys.
- The adapter keeps object metadata separate from object bytes, modeling the future Postgres `objects` table while using an in-memory metadata implementation for local conformance tests.
- The byte-backed object adapter preserves the backend object contract: `ObjectId = sha256(raw_bytes)`, same-object writes are idempotent, kind mismatches are corruption, missing metadata is `Ok(None)`, and missing/corrupt remote bytes behind existing metadata are corruption.
- Remote byte stores now expose conditional create-if-absent writes, delete, and prefix listing so durable object writes can avoid accidental final-key overwrites and can clean old upload staging keys.
- The byte-backed object adapter stages object bytes under repo-scoped upload keys, converges final immutable object keys with conditional writes, reconciles matching existing final bytes, and leaves final content-addressed bytes in place if metadata insertion fails so retries can repair metadata.
- The object adapter exposes cleanup helpers for old staged uploads, dry-run detection for old final object keys that are missing metadata records, and a claim-backed helper that repairs missing object metadata from verified final bytes.
- Final object delete mode still fails closed. Cleanup claims coordinate repair workers, but deletion still needs a stronger metadata-writer fencing contract before it is safe.
- `LocalBlobStore` has focused coverage for nested durable object keys.
- `scripts/check-r2-object-store.sh` and `remote::blob::tests::r2_blob_store_live_integration` provide an opt-in live S3/R2-compatible object-store gate for byte round trips, missing-key mapping, and `BlobObjectStore` composition.
- `.github/workflows/rust-ci.yml` includes a default no-secret `r2-object-store` job that checks the gate script skip path without requiring bucket credentials.
- `migrations/postgres/0001_durable_backend_foundation.sql` now records stricter contract constraints for repo IDs, object hash identity, commit timestamps, ref version bounds, global audit sequence uniqueness, active approval uniqueness, and explicit `updated_at` ownership.

What is not built:

- No server runtime cutover for object bytes, object metadata, commit metadata, or ref compare-and-swap; only Postgres control-plane runtime wiring is in place.
- No server connection pool or hosted database operations posture; current runtime store construction uses the existing per-operation Postgres adapter connections.
- No S3/R2 runtime cutover.
- No background cleanup worker, lifecycle policy automation, multipart/chunked uploads, signed URLs, final-object deletion, distributed locking, or cross-store transaction boundary.
- No HTTP filesystem/VCS object-byte behavior change; the local `StratumDb` snapshot remains the core request path.

Grounding: `src/backend/blob_object.rs`, `src/backend/object_cleanup.rs`, `src/backend/mod.rs`, `src/remote/blob.rs`, `scripts/check-r2-object-store.sh`, `.github/workflows/rust-ci.yml`, `migrations/postgres/0001_durable_backend_foundation.sql`, `docs/plans/2026-05-01-backend-adapter-scaffolding.md`, `docs/plans/2026-05-02-r2-object-store-integration.md`, `docs/plans/2026-05-02-object-upload-staging.md`, `docs/plans/2026-05-02-durable-cleanup-claims.md`.

## Postgres Migration Harness

The Postgres migration harness makes the first durable metadata migration executable without changing runtime behavior.

What is built:

- `tests/postgres/0001_durable_backend_foundation_smoke.sql` applies the ordered Postgres migration catalog inside a rollback-only transaction and asserts key schema contracts.
- The smoke harness covers repo ID envelopes, object ID/hash identity, object cleanup claim constraints, repo-scoped commit/ref FKs, change-request commit ID shape checks, ref version bounds, idempotency state shape, audit sequence uniqueness, active approval uniqueness, and sequential ref compare-and-swap predicates using the documented source-row-locking SQL shape.
- `scripts/check-postgres-migrations.sh` runs the smoke harness with `psql -v ON_ERROR_STOP=1`, skips cleanly when `STRATUM_POSTGRES_TEST_URL` is unset outside required mode, rejects password-bearing connection URLs including query/keyword password forms, and fails clearly if `psql` is missing.
- `.github/workflows/rust-ci.yml` includes a separate `postgres-migrations` job using a `postgres:16` service container, `pg_isready` health check, explicit PostgreSQL client install on the runner, and required harness mode so CI cannot green-skip if the URL is removed.

What is not built:

- No runtime connection pool or hosted Postgres operations posture; database URL handling is limited to durable startup preflight and the current control-plane store construction.
- No broad Postgres transaction stress test yet; the smoke harness checks schema contracts and the adapter adds focused CAS/source-lock coverage.
- No live S3/R2 runtime cutover, distributed locking, production cleanup workers, or cross-store transactions.
- No HTTP filesystem/VCS object-byte behavior change; `StratumDb` core state remains local.

Grounding: `tests/postgres/0001_durable_backend_foundation_smoke.sql`, `scripts/check-postgres-migrations.sh`, `.github/workflows/rust-ci.yml`, `migrations/postgres/0001_durable_backend_foundation.sql`, `migrations/postgres/0002_review_local_commit_ids.sql`, `docs/plans/2026-05-02-postgres-migration-harness.md`.

## Postgres Migration Runner Foundation

The Postgres migration runner foundation adds production-style migration state tracking for durable startup preparation before the server opens any durable control-plane stores.

What is built:

- `src/backend/postgres_migrations.rs` defines a feature-gated Rust migration runner over the existing `tokio-postgres` adapter stack.
- The runner owns a `stratum_schema_migrations` control table with ordered migration version, name, SHA-256 checksum, `started`/`applied`/`failed` state, timestamps, and failure message fields.
- The static migration catalog currently wraps `migrations/postgres/0001_durable_backend_foundation.sql` and `migrations/postgres/0002_review_local_commit_ids.sql`.
- `status()` reports known migrations as pending or applied and surfaces dirty, checksum/name mismatch, and unknown applied-version state.
- `apply_pending()` creates the control table when needed, takes a schema-scoped `pg_try_advisory_lock`, refuses dirty/mismatched/unknown state before applying, records `started`, runs each migration in a transaction, and records `applied` or `failed`.
- Runner `Debug` output includes only non-secret schema/catalog information and does not include Postgres connection strings.
- Durable `stratum-server` startup calls the runner in status or apply mode when the binary is built with the `postgres` feature, then the durable runtime control-plane cutover opens Postgres workspace/idempotency/audit/review stores if preflight succeeds.

What is not built:

- No runtime connection pool, hosted TLS/KMS/secrets posture, or production database configuration.
- No migration CLI/admin endpoint, rollback/down migrations, or adoption flow for schemas that were manually migrated before the control table existed.
- No core filesystem/VCS runtime cutover to Postgres object/commit/ref metadata or S3/R2 object bytes.

Grounding: `src/backend/postgres_migrations.rs`, `src/backend/postgres.rs`, `migrations/postgres/0001_durable_backend_foundation.sql`, `migrations/postgres/0002_review_local_commit_ids.sql`, `docs/plans/2026-05-02-postgres-migration-runner-foundation.md`.

## Postgres Metadata Adapter

The Postgres metadata adapter makes the durable backend schema executable from Rust. The default server runtime stays local, while the durable runtime control-plane cutover uses the adapter for workspace metadata, idempotency, audit, and review state.

What is built:

- `Cargo.toml` defines an optional `postgres` feature using `tokio-postgres`; default builds and runtime behavior are unchanged.
- `src/backend/postgres.rs` defines `PostgresMetadataStore`, which connects per operation, drives the Postgres connection task, pins default connections to `public`, supports a validated schema override for tests, and avoids logging connection strings or passwords.
- `PostgresMetadataStore` implements `ObjectMetadataStore` over the `objects` table while leaving bytes in the object-store layer.
- `PostgresMetadataStore` implements `ObjectCleanupClaimStore` over `object_cleanup_claims`, including expiring leases, retry attempts, stale-token completion/failure rejection, and completion state.
- `PostgresMetadataStore` implements `CommitStore` over `commits` and ordered `commit_parents`, including idempotent duplicate insert handling and conflict detection.
- `PostgresMetadataStore` implements `RefStore` over `refs`, including `MustNotExist`, matching compare-and-swap updates, version increments, source-checked updates in one transaction, and row locking with `SELECT ... FOR UPDATE` for existing source/target refs.
- `PostgresMetadataStore` implements `IdempotencyStore` over `idempotency_records` with `BEGIN`/`COMPLETE`/`ABORT` semantics aligned to `src/idempotency.rs` (replay, fingerprint conflict, in-progress concurrent begins, stale-reservation fencing, hashed keys only).
- `PostgresMetadataStore` implements `AuditStore` over global `audit_events` rows (`repo_id IS NULL`), using JSONB for structured fields and a transaction advisory lock for monotonic sequence allocation.
- `PostgresMetadataStore` implements `WorkspaceMetadataStore` over global `workspaces` rows (`repo_id IS NULL`) and `workspace_tokens`, preserving base/session refs, head-version updates, scoped-prefix normalization, and hash-only workspace-token validation.
- Adapter tests create a unique schema, apply the ordered Postgres migration catalog, exercise object metadata, cleanup claims, byte-store composition, commit metadata, idempotency store contracts, audit store contracts, workspace metadata contracts, review store contracts, blocked conflicting idempotency inserts, concurrent duplicate idempotency, ref CAS, source-checked CAS, cross-repo FK behavior, max-version overflow semantics, and a focused concurrent CAS race, then drop the schema.
- `.github/workflows/rust-ci.yml` includes a separate `postgres-backend` job using a `postgres:16` service container, warnings-denied clippy with the `postgres` feature, and required Postgres adapter tests.

What is not built:

- No `StratumDb`, HTTP filesystem/VCS, MCP, CLI, or FUSE core runtime cutover to Postgres object, commit, or ref metadata.
- No connection pool, TLS/KMS/secrets posture, or production database configuration beyond the current `PGPASSWORD` startup preflight path.
- Durable control-plane routing is now wired for HTTP `stratum-server` only; MCP, CLI, FUSE, and embedded callers still use existing local/default control-plane stores unless separately adapted.
- No S3/R2 object-byte runtime cutover or cross-store transaction spanning object bytes plus metadata.
- Source-checked `MustNotExist` is intentionally unsupported in the adapter because there is no source row to lock under the current schema.

Grounding: `src/backend/postgres.rs`, `src/backend/blob_object.rs`, `src/backend/object_cleanup.rs`, `src/backend/mod.rs`, `.github/workflows/rust-ci.yml`, `migrations/postgres/0001_durable_backend_foundation.sql`, `migrations/postgres/0002_review_local_commit_ids.sql`, `docs/plans/2026-05-02-postgres-metadata-adapter.md`, `docs/plans/2026-05-02-durable-cleanup-claims.md`.

## Postgres Idempotency Adapter Foundation

The Postgres idempotency foundation proves the durable `idempotency_records` schema satisfies the existing Rust `IdempotencyStore` contract. The default server runtime stays local, and the durable runtime control-plane cutover uses this adapter for HTTP idempotency in `STRATUM_BACKEND=durable` with the `postgres` feature.

What is built:

- Feature-gated `impl IdempotencyStore for PostgresMetadataStore` in `src/backend/postgres.rs`, persisting only `IdempotencyKey::key_hash()` (never raw headers) alongside SHA-256-shaped `request_fingerprint`, status, and JSON replay bodies for completed responses.
- The migration now constrains `idempotency_records.key_hash` and `request_fingerprint` to lowercase 64-hex digest shape, with rollback-only smoke assertions for malformed values.
- `run_idempotency_contracts`, invoked from existing `run_backend_contracts`, exercises execute → complete → replay, fingerprint conflict, pending / in-progress semantics, targeted abort freeing a reservation, stale `complete` and `abort` rejection after same-key retry, blocked conflicting insert commit/rollback behavior, concurrent same-fingerprint winners (one `Execute`, one `InProgress`), and a SQL check that stored `key_hash` matches `key.key_hash()`.
- `IdempotencyReservation::for_store` plus minimal `pub(crate)` accessors in `src/idempotency.rs` so adapters compose without leaking raw keys; reservations now carry an opaque store-owned token so stale reservations cannot complete or abort a later retry with the same idempotency key and fingerprint.

What is not built:

- No HTTP behavior change for default/local builds.
- No retention TTL, stale-pending takeover, or sweep worker (`idempotency_records` retains rows indefinitely until a future runtime plan adds expiration/recovery).
- No idempotent workspace-token issuance; secret-bearing replay remains explicitly outside this slice.
- No connection pooling or hosted idempotency operations posture.

Residual risk:

- Hosted hardening remains blocked on retention, stale-pending recovery, secrets posture for replay bodies, operational pooling, and multi-node concurrency testing.

Focused review verification on 2026-05-02 with Postgres at `postgres://127.0.0.1/postgres`:

```bash
cd /Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation
cargo fmt --all -- --check
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-02 from this `v2/foundation` worktree: formatting check passed; Postgres migration rollback smoke exited with `ROLLBACK`; `backend::postgres` lib tests observed **8** passed (migration runner helpers plus `postgres_metadata_store_round_trips_backend_contracts`) with required Postgres URL; both clippy configurations passed with `-D warnings`; **full `cargo test --locked` passed** including the stale-aborted-reservation regression; `stratum-mount` gated compile succeeded; **`cargo audit --deny warnings`** scanned **408** crate dependencies without denied vulnerabilities; **`git diff --check`** whitespace scan was clean.

Grounding: `src/idempotency.rs`, `src/backend/postgres.rs`, `migrations/postgres/0001_durable_backend_foundation.sql`, `docs/plans/2026-05-02-postgres-idempotency-adapter-foundation.md`.

## Postgres Audit Adapter Foundation

The Postgres audit adapter foundation proves the durable `audit_events` table can satisfy the existing Rust `AuditStore` append/list contract. The default server runtime stays local, and the durable runtime control-plane cutover uses this adapter for current mutation audit events in `STRATUM_BACKEND=durable` with the `postgres` feature.

What is built:

- Feature-gated `impl AuditStore for PostgresMetadataStore`, storing sanitized actor/workspace/resource/details JSONB and action/outcome text using the existing serde snake_case enum shape.
- Global audit events use `repo_id IS NULL`, database-owned timestamps, and a transaction-scoped Postgres advisory lock for sequence allocation.
- Live adapter tests cover append, partial outcome and workspace JSON round trip, `list_recent` ordering/limit behavior, and concurrent append sequence uniqueness.

What is not built:

- No HTTP behavior change for default/local builds.
- No event bus, streaming sink, partitioning, export, retention, or hosted audit operations.
- No read/auth/policy-decision audit expansion.
- No repo-scoped audit sequence model.

Residual risk:

- Production audit remains a foundation until retention/export, policy-decision coverage, hosted operations, and event-bus ingestion are designed.

Review verification on 2026-05-03 from the `v2/foundation` worktree: formatting check passed; Postgres migration rollback smoke exited with `ROLLBACK`; required live Postgres backend tests observed **8** passed including the audit adapter contracts; both clippy configurations passed with `-D warnings`; **full `cargo test --locked` passed**; optional `stratum-mount` FUSE compile succeeded; **`cargo audit --deny warnings`** scanned **408** crate dependencies without denied vulnerabilities; **`git diff --check`** whitespace scan was clean.

Grounding: `src/backend/postgres.rs`, `migrations/postgres/0001_durable_backend_foundation.sql`, `docs/plans/2026-05-03-postgres-audit-adapter-foundation.md`.

## Postgres Workspace Metadata Adapter Foundation

The Postgres workspace metadata adapter foundation proves the durable `workspaces` and `workspace_tokens` tables can satisfy the existing Rust `WorkspaceMetadataStore` contract. The default server runtime stays local, and the durable runtime control-plane cutover uses this adapter for hosted workspace endpoints in `STRATUM_BACKEND=durable` with the `postgres` feature.

What is built:

- Feature-gated `impl WorkspaceMetadataStore for PostgresMetadataStore`, storing global workspaces with `repo_id IS NULL`.
- Workspace create/list/get, base/session ref ownership, head commit updates, and monotonic version increments over the durable `workspaces` table.
- Workspace-token issuance stores only SHA-256 secret hashes and normalized read/write prefix arrays in JSONB; validation returns the existing workspace/token shape without exposing raw secrets.
- Live adapter tests cover workspace ordering, ref fields, head update versioning, scoped token normalization, wrong-secret rejection, wrong-workspace rejection, and raw SQL assertions that token secrets are not stored.

What is not built:

- No HTTP behavior change for default/local builds.
- No idempotent workspace-token issuance or secret-bearing replay persistence.
- No workspace-token expiry, revocation, rotation, KMS/secret-manager integration, or hosted operations.
- No repo-scoped workspace domain model.

Residual risk:

- Production workspace metadata still needs secret posture, token lifecycle operations, hosted deployment behavior, and repo-aware domain work.

Review verification on 2026-05-03 from the `v2/foundation` worktree: formatting check passed; Postgres migration rollback smoke exited with `ROLLBACK`; required live Postgres backend tests observed **8** passed including workspace metadata contracts and corrupt ref rejection; both clippy configurations passed with `-D warnings`; **full `cargo test --locked` passed**; optional `stratum-mount` FUSE compile succeeded; **`cargo audit --deny warnings`** scanned **408** crate dependencies without denied vulnerabilities; **`git diff --check`** whitespace scan was clean.

Grounding: `src/workspace/mod.rs`, `src/backend/postgres.rs`, `migrations/postgres/0001_durable_backend_foundation.sql`, `docs/plans/2026-05-03-postgres-workspace-metadata-adapter-foundation.md`.

## Postgres Review Adapter Foundation

The Postgres review adapter foundation proves the durable protected-change and review tables can satisfy the existing Rust `ReviewStore` contract while the core VCS runtime still owns commit identity locally. The default server runtime stays local, and the durable runtime control-plane cutover uses this adapter for review/protected-change endpoints in `STRATUM_BACKEND=durable` with the `postgres` feature.

What is built:

- Feature-gated `impl ReviewStore for PostgresMetadataStore`, storing review rows under `RepoId::local()` until the review domain becomes repo-aware.
- Protected ref/path rule create/list/get, change-request create/list/get/transition, approval create/list/dismissal, reviewer assignment create/update/list, review comment create/list, and approval-policy decision computation over Postgres rows.
- Duplicate active approvals return the existing approval, dismissed approvals stop counting, required reviewer assignments participate in approval decisions, and terminal change requests reject new review mutations.
- Live adapter tests cover rule storage, change-request commit ID shape without requiring durable Postgres commit rows, duplicate approvals, dismissal/re-approval, reviewer assignment updates, comment normalization, terminal-state rejection, approval-policy computation, and corrupt-row rejection.

What is not built:

- No HTTP behavior change for default/local builds.
- No repo-aware review trait or hosted multi-repo review routing.
- No reviewer groups, threaded/resolved comments, merge queue, web review UI, distributed policy engine, or cross-store transaction boundary.

Residual risk:

- Production review state still needs a repo-aware domain, hosted multi-repo routing, distributed policy design, and review UX.
- Reviewer/user IDs plus required-approval counts are bounded by the current Postgres `INTEGER` schema.

Review verification on 2026-05-03 from the `v2/foundation` worktree: `cargo fmt --all -- --check` passed; `cargo check --locked --features postgres` passed; `STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh` exited `ROLLBACK` for migration smoke; `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **8** passed; `cargo clippy --locked --features postgres --all-targets -- -D warnings` passed; `cargo clippy --locked --all-targets -- -D warnings` passed; **full `cargo test --locked` passed**; `cargo check --locked --features fuser --bin stratum-mount` passed; **`cargo audit --deny warnings`** scanned **408** crate dependencies without denied vulnerabilities; **`git diff --check`** whitespace scan was clean.

Grounding: `src/review.rs`, `src/backend/postgres.rs`, `migrations/postgres/0001_durable_backend_foundation.sql`, `migrations/postgres/0002_review_local_commit_ids.sql`, `docs/plans/2026-05-03-postgres-review-adapter-foundation.md`.

## Durable Runtime Control-Plane Cutover

The durable runtime control-plane cutover lets `STRATUM_BACKEND=durable` start `stratum-server` with Postgres-backed workspace metadata, idempotency, audit, and review stores after migration preflight. This is intentionally not the full durable filesystem/VCS/object-byte cutover.

What is built:

- `src/server/mod.rs` now has a `ServerStores` bundle for workspace metadata, idempotency, audit, and review stores.
- `server::build_router(db)` remains the local/default compatibility path and still opens local control-plane stores.
- `server::open_server_stores_for_runtime()` opens local stores for `STRATUM_BACKEND=local` and one shared `PostgresMetadataStore` for workspace/idempotency/audit/review in `STRATUM_BACKEND=durable` when built with the `postgres` feature.
- Durable server startup remains fail-closed without the `postgres` feature.
- Durable server startup with the `postgres` feature still runs migration status/apply preflight before opening any durable control-plane stores.
- Durable startup now fails closed for remote or TLS-required Postgres URLs until TLS support is wired into the runtime connector; explicit localhost, loopback hostaddr, and Unix-socket targets are allowed.
- After migration preflight, durable startup verifies the expected repository/control-plane tables and columns are present before serving so drifted schemas do not pass a catalog-only status check.
- `stratum-server` logs only non-secret backend mode and control-plane store labels, and exits cleanly on runtime store or database-open errors.
- Process tests cover default local health startup, durable fail-closed startup without `postgres`, remote-NoTLS rejection, pending/dirty/drifted migration blocking, missing `repos` blocking, durable apply-mode health startup, cleanup of isolated test resources, and a live Postgres request path that verifies workspace, idempotency, audit, protected-ref, and change-request rows land in Postgres without creating local control-plane `.bin` files.

What is not built:

- No `StratumDb` core runtime cutover: filesystem state, VCS object bytes, commit vectors, and refs still use the local `.vfs/state.bin` snapshot.
- No S3/R2 object-byte request routing, despite durable config still requiring R2 env vars for the future hosted runtime.
- No Postgres connection pool, hosted TLS/KMS/secrets manager posture, distributed locks, or cross-store transaction boundary.
- No MCP, CLI, FUSE, or embedded caller cutover to durable control-plane stores.
- No idempotency retention/sweep worker, audit event bus, workspace token lifecycle operations, repo-aware review routing, or durable background cleanup worker.

Residual risk:

- Durable control-plane state can now live in Postgres while core filesystem/VCS state remains local, so hosted deployments still need an explicit operational boundary and backup story for both stores.
- Multi-node correctness is not proven; the current adapter opens per-operation Postgres connections and no distributed runtime lock exists.
- Durable runtime Postgres connectivity is intentionally local/Unix-socket only until a TLS-capable connector and hosted secret posture are designed.
- R2 credentials are validated by durable config but unused by request handling until the object-byte runtime cutover lands.

Focused implementation verification on 2026-05-04 and 2026-05-05 from the `v2/foundation` worktree: `cargo fmt --all -- --check` passed; `cargo test --locked backend::runtime --lib -- --nocapture` observed **21** passed; `cargo test --locked --features postgres backend::runtime --lib -- --nocapture` observed **30** passed; `cargo test --locked --test server_startup -- --nocapture` observed **4** passed before review-fix expansion; `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture` observed **14** passed after review fixes; `cargo check --locked --features postgres` passed; `git diff --check` was clean. Focused review-fix verification also passed `STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh`, required live `backend::postgres_migrations`, required live `backend::postgres`, `PGPASSWORD=postgres cargo test --locked --test server_startup durable_backend_startup_fails_closed_without_creating_local_store_when_env_is_complete -- --exact --nocapture`, and `cargo clippy --locked --features postgres --test server_startup -- -D warnings`.

Full review-fix verification on 2026-05-05 from the `v2/foundation` worktree passed: `cargo fmt --all -- --check`; `cargo check --locked --features postgres`; `STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh`; required live Postgres `backend::postgres`; required live Postgres `--test server_startup`; `cargo clippy --locked --features postgres --all-targets -- -D warnings`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo test --locked` (**359** lib tests, **142** integration tests, **37** perf tests, **1** perf comparison test, **72** permission tests, and **7** default startup process tests observed); `cargo check --locked --features fuser --bin stratum-mount`; `cargo audit --deny warnings` (scanned **408** dependencies); and `git diff --check`.

Grounding: `src/bin/stratum_server.rs`, `src/server/mod.rs`, `src/backend/runtime.rs`, `src/backend/postgres.rs`, `src/backend/postgres_migrations.rs`, `migrations/postgres/0002_review_local_commit_ids.sql`, `tests/server_startup.rs`, `docs/plans/2026-05-04-durable-runtime-control-plane-cutover.md`.

## Durable Startup Migration Runner Wiring

The durable startup migration wiring lets operators prepare and verify Postgres schema state through `stratum-server` startup before durable control-plane stores are opened.

What is built:

- `BackendRuntimeConfig` now carries durable migration startup settings in addition to the existing durable Postgres/R2 prerequisite checks.
- `STRATUM_DURABLE_MIGRATION_MODE` accepts `status` and `apply`, defaults to `status`, and is only used for `STRATUM_BACKEND=durable`.
- `STRATUM_POSTGRES_SCHEMA` optionally selects the migration schema and defaults to `public`.
- In `status` mode, durable startup built with the `postgres` feature connects to Postgres, checks the migration control table/catalog, and rejects pending, dirty, checksum-mismatched, or unknown migration state before opening local state.
- In `apply` mode, durable startup applies pending migrations through the existing schema-scoped advisory lock, validates final state, and then the durable runtime control-plane cutover can open Postgres workspace/idempotency/audit/review stores.
- `STRATUM_POSTGRES_URL` continues to reject embedded passwords; startup applies `PGPASSWORD` to the migration connection when present without storing or logging it.
- Startup and migration-runner corruption errors avoid echoing database-controlled migration names or states.

What is not built:

- No `StratumDb`, HTTP, MCP, CLI, or FUSE cutover to Postgres metadata or S3/R2 object bytes.
- No server connection pool, hosted TLS/KMS/secrets posture, migration CLI/admin endpoint, rollback/down migrations, or manual-adoption workflow.
- No cross-store transaction boundary or distributed runtime lock for metadata plus object bytes.

Residual risk:

- This is still a startup preflight and schema preparation path, not the production durable cloud runtime.
- Operators must opt into `STRATUM_DURABLE_MIGRATION_MODE=apply`; the default `status` mode reports pending migrations without changing schema state.

Review verification on 2026-05-03 from the `v2/foundation` worktree: `cargo fmt --all -- --check` passed; `cargo check --locked --features postgres` passed; `STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh` exited `ROLLBACK` for migration smoke; `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture` observed **8** passed; `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture` observed **5** passed including status/apply/dirty durable startup cases; `cargo clippy --locked --features postgres --all-targets -- -D warnings` passed; `cargo clippy --locked --all-targets -- -D warnings` passed; **full `cargo test --locked` passed**; `cargo check --locked --features fuser --bin stratum-mount` passed; **`cargo audit --deny warnings`** scanned **408** crate dependencies without denied vulnerabilities; **`git diff --check`** whitespace scan was clean.

Grounding: `src/backend/runtime.rs`, `src/backend/postgres_migrations.rs`, `src/bin/stratum_server.rs`, `tests/server_startup.rs`, `docs/plans/2026-05-03-durable-startup-migration-runner-wiring.md`.

## Backend Runtime Selection Foundation

The backend runtime selection foundation defines the startup contract for local versus durable server modes.

What is built:

- `src/backend/runtime.rs` parses `STRATUM_BACKEND`, defaulting to `local` and accepting only `local` or `durable`.
- `STRATUM_BACKEND=durable` validates that the planned durable prerequisites are present: `STRATUM_POSTGRES_URL`, `STRATUM_R2_BUCKET`, `STRATUM_R2_ENDPOINT`, `STRATUM_R2_ACCESS_KEY_ID`, and `STRATUM_R2_SECRET_ACCESS_KEY`.
- Runtime Postgres URLs that embed passwords in URI userinfo, query `password=`, or keyword/value `password = ...` forms are rejected before server startup. The startup preflight can consume `PGPASSWORD` when present, but does not store or log it.
- `STRATUM_DURABLE_MIGRATION_MODE=status|apply` and optional `STRATUM_POSTGRES_SCHEMA` now control the durable startup migration preflight when built with the `postgres` feature.
- Runtime R2 endpoints that embed userinfo or secret-bearing query parameters are rejected before server startup.
- The runtime selector stores only non-secret object-store fields plus booleans for configured credential variables, and its `Debug` output does not include raw R2 credentials or the Postgres URL.
- `stratum-server` logs the selected backend mode, checks/applies migrations for durable `postgres` builds, fails closed for `STRATUM_BACKEND=durable` without the `postgres` feature, and otherwise opens Postgres control-plane stores through the durable runtime control-plane cutover.
- `R2BlobStoreConfig` now has a manual redacted `Debug` implementation so future diagnostics do not print access keys or secret keys.

What is not built:

- No server connection pool; durable control-plane stores currently use the existing per-operation Postgres adapter connections.
- No HTTP filesystem/VCS, MCP, CLI, FUSE, or `StratumDb` core cutover to Postgres object/commit/ref metadata or S3/R2 object bytes.
- No production secret manager/KMS integration, background cleanup worker, distributed locking, or cross-store transaction boundary.

Grounding: `src/backend/runtime.rs`, `src/bin/stratum_server.rs`, `src/remote/blob.rs`, `docs/http-api-guide.md`, `docs/plans/2026-05-02-backend-runtime-selection.md`.

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

Focused POSIX/FUSE xattr verification during implementation:

```bash
cargo test --locked --test integration posix_xattr -- --nocapture
cargo test --locked --features fuser fuse_mount::tests::xattr -- --nocapture
cargo check --locked --features fuser --bin stratum-mount
cargo fmt --all -- --check
git diff --check -- src/posix.rs tests/integration/posix.rs src/fuse_mount.rs
```

Result on 2026-05-01: passed from this worktree. Observed coverage included POSIX MIME/custom xattr round trips, list/remove behavior, create-only and replace-only flags, unsupported-name behavior, symlink inode metadata semantics, permission enforcement, stat metadata/ctime updates, FUSE list payload encoding, get/list buffer sizing, xattr flag conversion, and optional `stratum-mount` FUSE compile.

Full current-HEAD verification for the POSIX/FUSE xattr slice:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-01: passed from this worktree. Observed coverage included 218 lib tests, 8 MCP unit tests, 1 `stratumctl` unit test, 142 integration tests, 37 perf tests, 1 perf comparison test, 72 permission tests, 0 doc tests, optional `stratum-mount` FUSE compile, `cargo audit --deny warnings` scanning 387 dependencies with no denied findings, clippy with warnings denied, formatting check, and whitespace diff check.

Focused durable backend foundation verification during implementation and review fixes:

```bash
cargo fmt --all -- --check
cargo test --locked backend:: -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
git diff --check
```

Result on 2026-05-01: passed from this worktree. Observed coverage included the local object-store idempotency and kind-mismatch contract, commit-store insert/list/get behavior, stale ref CAS rejection, source-checked ref CAS atomicity, and composed idempotency replay/conflict semantics through `StratumStores::local_memory()`.

Full durable backend foundation verification:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-01: passed from this worktree. Observed coverage included 301 lib tests, 8 MCP unit tests, 1 `stratumctl` unit test, 142 integration tests, 37 perf tests, 1 perf comparison test, 72 permission tests, 0 doc tests, optional `stratum-mount` FUSE compile, `cargo audit --deny warnings` scanning 387 dependencies with no denied findings, clippy with warnings denied, formatting check, and whitespace diff check.

Focused backend adapter scaffolding verification during implementation and review fixes:

```bash
cargo fmt --all -- --check
cargo test --locked backend::blob_object -- --nocapture
cargo test --locked remote::blob -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
git diff --check
```

Result on 2026-05-01: passed from this worktree. Observed coverage included 9 byte-backed object adapter tests, the `LocalBlobStore` nested-key test, R2 `NoSuchKey` compile coverage through clippy, formatting, clippy with warnings denied, and whitespace diff check.

Full backend adapter scaffolding verification:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-01: passed from this worktree. Observed coverage included 311 lib tests, 8 MCP unit tests, 1 `stratumctl` unit test, 142 integration tests, 37 perf tests, 1 perf comparison test, 72 permission tests, 0 doc tests, optional `stratum-mount` FUSE compile, `cargo audit --deny warnings` scanning 387 dependencies with no denied findings, clippy with warnings denied, formatting check, and whitespace diff check.

Focused Postgres migration harness verification:

```bash
bash -n scripts/check-postgres-migrations.sh
./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_MIGRATIONS_REQUIRED=1 ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_URL=postgresql://user:secret@localhost/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres ./scripts/check-postgres-migrations.sh
```

Result on 2026-05-02: passed from this worktree. The unset local run skipped cleanly, required mode rejected a missing URL, password-bearing URLs were rejected before invoking `psql`, and the local Postgres smoke run applied the migration plus SQLSTATE/constraint-aware assertions inside a rollback-only transaction.

Full Postgres migration harness verification:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-02: passed from this worktree. Observed coverage included 311 lib tests, 8 MCP unit tests, 1 `stratumctl` unit test, 142 integration tests, 37 perf tests, 1 perf comparison test, 72 permission tests, 0 doc tests, optional `stratum-mount` FUSE compile, `cargo audit --deny warnings` scanning 387 dependencies with no denied findings, clippy with warnings denied, formatting check, and whitespace diff check.

Focused Postgres metadata adapter verification:

```bash
cargo check --features postgres
env -u STRATUM_POSTGRES_TEST_URL -u STRATUM_POSTGRES_TEST_PASSWORD -u PGPASSWORD cargo test --locked --features postgres backend::postgres -- --nocapture
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres cargo test --locked --features postgres backend::postgres -- --nocapture
```

Result on 2026-05-02: passed from this worktree. The unset local run skipped cleanly, the local Postgres run created an isolated schema, applied the durable backend migration, exercised object/commit/ref adapter contracts, and dropped the schema afterward.

Full Postgres metadata adapter verification:

```bash
bash -n scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_URL='postgres://localhost/postgres?password=secret' ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_URL='host=localhost password=secret dbname=postgres' ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres ./scripts/check-postgres-migrations.sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres cargo test --locked --features postgres backend::postgres -- --nocapture
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-02: passed from this worktree. Observed coverage included 311 lib tests, 8 MCP unit tests, 1 `stratumctl` unit test, 142 integration tests, 37 perf tests, 1 perf comparison test, 72 permission tests, 0 doc tests, real Postgres adapter tests, optional `stratum-mount` FUSE compile, `cargo audit --deny warnings` scanning 408 dependencies with no denied findings, clippy with warnings denied for default and `postgres` feature builds, formatting check, and whitespace diff check.

R2 object-store integration gate verification:

```bash
bash -n scripts/check-r2-object-store.sh
./scripts/check-r2-object-store.sh
STRATUM_R2_TEST_REQUIRED=1 env -u STRATUM_R2_BUCKET ./scripts/check-r2-object-store.sh
env -u STRATUM_R2_TEST_ENABLED -u STRATUM_R2_TEST_REQUIRED cargo test --locked remote::blob::tests::r2_blob_store_live_integration -- --nocapture
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-02: passed from this worktree. The no-secret script run skipped cleanly, required mode failed before Cargo when required R2 env vars were missing, the focused Rust test skipped cleanly by default, and the full suite covered 312 lib tests, 8 MCP unit tests, 1 `stratumctl` unit test, 142 integration tests, 37 perf tests, 1 perf comparison test, 72 permission tests, and 0 doc tests.

Backend runtime selection foundation verification:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-02: passed from this worktree. Observed coverage included 327 lib tests, 8 MCP unit tests, 1 `stratumctl` unit test, 142 integration tests, 37 perf tests, 1 perf comparison test, 72 permission tests, 3 `stratum-server` durable startup process tests, 0 doc tests, optional `stratum-mount` FUSE compile, `cargo audit --deny warnings` scanning 408 dependencies with no denied findings, clippy with warnings denied for default and `postgres` feature builds, formatting check, and whitespace diff check.

Durable cleanup claims and orphan repair verification:

```bash
cargo fmt --all -- --check
cargo test --locked backend::object_cleanup -- --nocapture
cargo test --locked backend::blob_object -- --nocapture
STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres cargo test --locked --features postgres backend::postgres -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-02: passed from this worktree. Observed coverage included 6 focused cleanup-claim tests, 22 focused byte-backed object adapter tests, a live local Postgres adapter contract test, the Postgres migration smoke harness, 349 lib tests, 8 MCP unit tests, 1 `stratumctl` unit test, 142 integration tests, 37 perf tests, 1 perf comparison test, 72 permission tests, 3 `stratum-server` durable startup process tests, 0 doc tests, optional `stratum-mount` FUSE compile, `cargo audit --deny warnings` scanning 408 dependencies with no denied findings, clippy with warnings denied for default and `postgres` feature builds, formatting check, and whitespace diff check.

Postgres migration runner foundation verification:

```bash
cargo fmt --all -- --check
STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres ./scripts/check-postgres-migrations.sh
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres cargo test --locked --features postgres backend::postgres_migrations -- --nocapture
STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://localhost/postgres cargo test --locked --features postgres backend::postgres -- --nocapture
cargo clippy --locked --features postgres --all-targets -- -D warnings
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

Result on 2026-05-02: passed from this worktree. Observed coverage included 7 live Postgres migration runner tests, the Postgres migration smoke harness, 8 live Postgres backend feature tests including the existing metadata adapter contract, 349 default lib tests, 8 MCP unit tests, 1 `stratumctl` unit test, 142 integration tests, 37 perf tests, 1 perf comparison test, 72 permission tests, 3 `stratum-server` durable startup process tests, 0 doc tests, optional `stratum-mount` FUSE compile, `cargo audit --deny warnings` scanning 408 dependencies with no denied findings, clippy with warnings denied for default and `postgres` feature builds, formatting check, and whitespace diff check.

## Known Residual Risks

- Default local runtime durability is still file-backed metadata/state, and durable server mode only cuts over workspace/idempotency/audit/review control-plane stores to Postgres, not core filesystem/VCS state or object bytes.
- Scoped ACL enforcement has broad tests now, but the long-term policy service, action capabilities, policy decision logging, and tenant isolation model are not built.
- Refs/status/diff and protected-change semantics are foundation-level; approval records, review comments, approval dismissal, reviewer assignments, and approval counts exist, but merge queues, distributed policy decisions, and protected-change enforcement outside HTTP routes are not complete.
- Run records are useful audit artifacts, but they do not prove safe execution because no runner or sandbox exists yet.
- Run-record creation is not fully atomic across all files.
- Search remains a filesystem/search surface, not the full-text plus semantic derived index described in the v2 plan.
- Audit events are a mutation-only scaffold; durable server mode can persist them in Postgres, but there is no production audit pipeline for auth/read/policy/approval decisions or durable event-bus ingestion.
- Workspace-token issuance intentionally rejects idempotency keys until secret-aware replay storage exists.
- File metadata is available through stat/HTTP/VCS/local persistence and Stratum metadata-backed POSIX/FUSE xattrs, but automatic MIME inference, arbitrary binary/native xattrs, durable FUSE mutation persistence, and remote sparse FUSE cache correctness are not built.
- Cloud deployment scaffolding, backend contracts, a byte-backed object adapter scaffold, a guarded S3/R2-compatible object-store integration gate, a cleanup-claim/metadata-repair foundation, a Postgres migration smoke harness, a feature-gated Postgres migration runner, durable startup migration preflight, optional Postgres metadata adapters, a fail-closed backend runtime selector, and durable Postgres control-plane runtime wiring exist, but production multi-tenant backend, core runtime Postgres/R2 cutover, observability, idempotency retention/quota controls, KMS/secrets posture, and private-beta hardening remain future work.

## Not Built Yet

From the CTO plan and current repo docs, these are the major missing v2 pieces:

- Durable cloud runtime: core filesystem/VCS Postgres object/commit/ref runtime wiring, live S3/R2 object-store wiring in hosted runtime, background cleanup/repair workers, final-object deletion fencing, distributed locking, and cross-store transactional semantics.
- Repo/session domain model beyond the current workspace/ref ownership foundation.
- Reviewer identity beyond users/admins, reviewer groups/code owners, threaded/resolved comments, protected-change review UI, merge queues, and protected-change enforcement beyond HTTP route-level gates.
- Full audit event pipeline beyond the local mutating-operation scaffold.
- Published PyPI distribution for Python SDK (`stratum-sdk`).
- Full POSIX/FUSE metadata compatibility beyond Stratum metadata-backed MIME/custom xattrs, including arbitrary binary/native xattrs, durable mount mutation persistence, and remote sparse mount cache correctness guarantees.
- Full-text extraction workers and ACL-aware semantic search.
- Web console for browsing, diffs, approvals, audit, and access management.
- Execution Phase 2+: job runner, lifecycle status transitions, output streaming, cancellation, timeouts, sandbox policy, and artifact limits.

## Recommended Next Slices

Recommended order, keeping risk and the CTO plan in mind:

1. Design the core durable filesystem/VCS runtime boundary: `StratumDb` persistence model, Postgres object/commit/ref routing, R2 object-byte routing, connection pooling, hosted secret posture, and explicit cross-store failure semantics.
2. Add secret-aware workspace-token idempotency only after replay storage and KMS/secrets posture are explicit.
3. Expand audit coverage to auth/read/policy decisions and move audit persistence toward the future Postgres/event-bus pipeline.
4. Continue object backend work with background repair workers and final-object deletion fencing only after metadata writers consult durable cleanup state.
5. Continue execution phase 2 only after idempotency, protected-change contracts, and audit semantics are clearer.
6. Continue POSIX/FUSE hardening around sparse remote cache correctness, mount daemon lifecycle/status/sync/unmount UX, and native xattr compatibility when the mount story becomes the active product surface.
7. Extend review semantics into reviewer groups/code owners, threaded/resolved comments, and review UI after the product review model is clear.

## Branch And Release Status

- Branch: `v2/foundation`.
- Remote tracking branch: `origin/v2/foundation`.
- Before the backend runtime selection foundation slice, `main` and `v2/foundation` were synced and pushed at merge commit `866794e` after the R2 object-store integration gate slice.
- `v2/foundation` now contains the VCS/session semantics, audit-event scaffolding, HTTP idempotency coverage, CI foundation, file metadata foundation, protected-change foundation, POSIX/FUSE metadata xattr, review feedback, reviewer assignment, approval workflow hardening, durable backend foundation, backend adapter scaffolding, Postgres migration harness, Postgres metadata adapter, R2 object-store integration gate, backend runtime selection foundation, durable cleanup claims/orphan repair foundation, production migration runner, Postgres idempotency/audit/workspace/review adapters, durable startup migration wiring, and durable runtime control-plane cutover slices after the approval-workflow merge.
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
