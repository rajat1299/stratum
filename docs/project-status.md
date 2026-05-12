# Stratum Project Status

- Last updated: 2026-05-12
- Branch: `v2/foundation`
- Backend work branch: `v2/foundation`
- Baseline on `v2/foundation` before the latest backend slice: `949dd2c` (`docs: add optional SMFS/Mirage references to durable cutover plan`)
- Latest completed backend slice: Policy Enforcement Below Route Layer
- Current backend slice in review: none
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
- Use workspace bearer headers, Stratum unrestricted path behavior, existing filesystem/search/tree/VCS endpoints, and idempotency keys for writes.
- Reserve semantic `sgrep` behavior until Stratum has the derived semantic-search/indexing layer.

Grounding:

- `docs/plans/2026-05-02-typescript-virtual-bash-sdk.md`
- SMFS reference repo at `/Users/rajattiwari/virtualfilesystem/smfs`

Current SDK progress:

- `sdk/bash` package scaffold exists with Bun, TypeScript, Vitest, and `just-bash`.
- `StratumClient` covers workspace bearer auth, filesystem read/write/list/stat, raw byte reads/writes, copy/move/delete, grep/find/tree, and VCS status/diff/commit calls.
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
- Under the guarded durable commit route, change-request creation and merge can use durable source/target refs and durable commit metadata without requiring the local `.vfs/state.bin` VCS state to contain those refs or commits. Local review reads still fall back directly to local commit ancestry when durable refs are absent.
- Change-request read/list/create/reject/merge responses include computed `approval_state`.
- Approval records are bound to a change request and captured `head_commit`; duplicate active approval by the same approver for the same head returns the existing record with `created: false`, and new approvals are limited to open change requests.
- Reviewer assignments are durable active records keyed by change request and reviewer UID, can be required or optional, are limited to open change requests, require admin-equivalent users for new assignments and upgrades to required while still allowing existing assignments to be downgraded if a reviewer loses approval rights, reject assignment of the change-request author, and update the existing assignment plus version when the required flag changes.
- Approval dismissal is limited to open change requests, marks an active approval inactive, records `dismissed_by` plus an optional stored reason, increments the approval version, returns `dismissed: true`, and immediately removes that approval from computed approval counts. Re-dismissing an inactive approval returns the same inactive record with `dismissed: false`.
- Review comments are durable records with `general` or `changes_requested` kind, author UID, optional normalized path, trimmed bounded body text, active flag, and version. New review comments are limited to open change requests.
- Approval policy decisions are computed from active protected ref rules matching the target ref, active protected path rules matching changed paths between the recorded base/head commits, and active required reviewer assignments.
- Durable review merge computes protected-path approval inputs by walking durable commit parent metadata from the recorded head back to the recorded base and collecting recorded changed-path names. Merge then advances the durable target ref with source-checked durable ref CAS.
- Effective required approvals is the maximum required count across matching rules, only active approvals for the current recorded head count, and required reviewer assignments must be satisfied by approvals from those exact reviewer UIDs.
- Change-request merge is a fast-forward contract: source and target refs must still match the recorded head/base commits, the recorded head must descend from the recorded base, approval state must be approved, then the target ref is compare-and-swap updated to the recorded head while source freshness is rechecked under the same local DB write lock.
- Direct protected ref mutations are blocked for `POST /vcs/commit`, `POST /vcs/revert`, and `PATCH /vcs/refs/{name}` when an active matching rule applies.
- Direct protected path mutations are blocked for HTTP file writes, directory creates, metadata patches, deletes, copy destinations, move source/destination paths, and HTTP VCS reverts that would touch protected paths on `main` when an active matching path-prefix rule applies.
- File writes and metadata patches check both the requested path and the final symlink target they would mutate.
- Deletes and move sources also block ancestor paths that contain protected descendants.
- Mutating FS, VCS, and review routes now evaluate protected ref/path decisions through a shared route policy seam and emit policy allow/deny audit events with bounded, redacted details.
- Shared policy decisions now mint bounded allow/review-approved decision tokens that are required by guarded durable protected-aware FS mutation, ref-update, commit, revert, and review-merge execution paths before lower durable writes can occur.
- Guarded durable filesystem mutation execution fails before session ref materialization, object writes, commit metadata insert, or session ref CAS unless it receives a repo/action/ref-matching allow token.
- Guarded durable VCS ref update, commit promotion, and revert execution require policy tokens before durable ref/object/commit mutation, with durable revert changed paths computed before mutation.
- Guarded durable review merge requires an approved review-merge policy token before durable target ref CAS, with protected-path approval inputs computed from durable commit metadata before mutation.
- Guarded durable filesystem mutation audit events now include content-free recovery identity: operation id, target ref, previous commit, new commit, and changed-path count, allowing recovery to deduplicate against the normal route audit event after idempotency completion failures.
- Protected rule creation, approval creation, reviewer assignment, review-comment creation, approval dismissal, and change-request create/reject/merge mutations emit local audit events without persisting request descriptions, approval comments, review-comment bodies, dismissal reasons, or file content.
- Review-route approval/comment/dismiss/reviewer-assignment/merge/reject mutations use conservative terminal-state checks and idempotency replay ordering so matching retries can replay after merge/reject while new terminal mutations are rejected.
- Review-route merge/reject transitions use a process-local transition lock to avoid same-process terminal-state races in this local foundation.

What is not built:

- No reviewer groups, threaded replies, comment resolution, or review UI.
- No protected-path-aware content merge/rebase; change-request merge is fast-forward only.
- No distributed policy engine or database transaction boundary for multi-node deployments.
- No web review console, notifications, or merge queue.
- No protected-change enforcement parity for MCP direct tools or POSIX/FUSE mutation paths yet; `stratumctl` inherits HTTP route behavior when pointed at a protected server, and direct embedded local `StratumDb` callers remain outside this durable policy-token seam.
- No external event-bus audit pipeline or distributed policy service.

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
- `3c7878a` - add route policy decision seam
- `b785506` - audit route policy decisions
- `4cd4047` - bind durable FS audit identity
- `fccf874` - align review mutation audit
- `f302583` - align durable review merge parity
- `6d0efdb` - harden route failure redaction

Focused verification during the Policy/Review/Audit Parity implementation on 2026-05-09 from the `v2/foundation` worktree: `cargo test --locked server::policy --lib -- --nocapture` passed; protected FS/VCS/review policy route tests passed during the seam and policy-audit commits; `cargo test --locked audit --lib -- --nocapture` passed; durable FS audit identity/recovery tests passed through `cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture`; and `cargo test --locked backend::core_transaction::tests::durable_fs_mutation_recovery --lib -- --nocapture` passed. Spec/correctness review found post-mutation audit/idempotency redaction and partial durable-ref approval-state fallback issues; code-quality/security review found stale policy-audit count assertions and missing post-mutation state markers. Main-session fixes made post-mutation audit/idempotency failure bodies redacted and explicit, adjusted route tests to assert policy/mutation audit events by action, and added a partial-durable-ref fallback regression for local approval-state reads.

Full verification on 2026-05-09 from the `v2/foundation` worktree: `cargo fmt --all -- --check` passed; `git diff --check` passed; `cargo clippy --locked --all-targets -- -D warnings` passed; `cargo clippy --locked --all-targets --features postgres -- -D warnings` passed; focused route tests passed for `server::routes_fs::tests` (**36** tests), `server::routes_vcs::tests` (**58** tests), and `server::routes_review::tests` (**47** tests); `cargo test --locked --lib --tests` passed, including **610** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, and **9** server-startup tests; `cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed with live Postgres portions skipped because `STRATUM_POSTGRES_TEST_URL` was unset; `STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh` skipped cleanly; and `cargo audit --deny warnings` passed after scanning **408** crate dependencies. Warm release perf via `sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture` passed **37** tests; the test harness finished in **10.69s**, and the full timed command observed **41.60s real**, **137.04s user**, **4.68s sys**, **1,732,542,464 bytes max RSS**, and **99,336,720 bytes peak memory footprint**.

Grounding: `src/review.rs`, `src/audit.rs`, `src/server/policy.rs`, `src/server/routes_review.rs`, `src/server/routes_fs.rs`, `src/server/routes_vcs.rs`, `src/backend/core_transaction.rs`, `src/backend/postgres.rs`, `src/db.rs`, `src/vcs/mod.rs`, `docs/http-api-guide.md`, `docs/plans/2026-05-09-policy-review-audit-parity.md`, `docs/plans/2026-05-01-change-requests-protected-paths.md`, `docs/plans/2026-05-01-approval-policy-foundation.md`, `docs/plans/2026-05-01-review-feedback-foundation.md`, `docs/plans/2026-05-01-reviewer-assignment-foundation.md`.

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
- The object adapter exposes cleanup helpers for old staged uploads, dry-run detection for old final object keys that are missing metadata records, and a claim-backed helper that repairs missing object metadata from verified final bytes. The repair helper now has both in-memory coverage and live Postgres-backed conformance coverage over durable metadata and cleanup-claim leases with local byte-store bytes.
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
- Adapter tests create a unique schema, apply the ordered Postgres migration catalog, exercise object metadata, cleanup claims, byte-store composition, Postgres-backed final-object metadata repair, commit metadata, idempotency store contracts, audit store contracts, workspace metadata contracts, review store contracts, blocked conflicting idempotency inserts, concurrent duplicate idempotency, ref CAS, source-checked CAS, cross-repo FK behavior, max-version overflow semantics, and a focused concurrent CAS race, then drop the schema.
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

## Durable Core Runtime Boundary Seam

The durable core runtime boundary seam makes the filesystem/VCS runtime selection explicit before any live Postgres/R2 core request routing is attempted.

What is built:

- `STRATUM_CORE_RUNTIME` now defaults to `local-state` and is parsed independently from `STRATUM_BACKEND`.
- `local`, `local-state`, `state-file`, and `snapshot` select the current local `StratumDb` core backed by `.vfs/state.bin`.
- `durable`, `durable-cloud`, and `postgres-r2` are recognized as the future durable core runtime, but `stratum-server` rejects them fail-closed before durable backend prerequisite validation, before migration preflight, before local state is opened, and before serving.
- Unknown core runtime values are rejected without echoing the raw value.
- `server::open_core_db_for_runtime()` is the server-side core open seam; today it enforces the server runtime support gate and opens `StratumDb` only for `local-state`.
- Startup logs include the supported core runtime store label for serving configurations.
- Process coverage verifies unsupported durable core mode does not create `.vfs/state.bin` or local control-plane files, including the mixed `STRATUM_BACKEND=durable` plus durable-core case.

What is not built:

- No live durable route executor yet; the route-facing serving path is still backed by local `StratumDb`.
- No Postgres object/commit/ref or R2 object-byte routing for live HTTP filesystem/VCS requests.
- No cross-store transaction boundary, distributed lock, object-writer fencing, connection pool, or hosted TLS/secrets posture.

Focused verification on 2026-05-05 from the `v2/foundation` worktree: `cargo test --locked --release --test perf -- --test-threads=1 --nocapture` passed after each code/docs diff in the slice, including the fail-ordering and helper-hardening review fixes; `cargo fmt --all -- --check` passed; `cargo test --locked backend::runtime --lib -- --nocapture` observed **29** passed; `cargo test --locked server::tests::open_ --lib -- --nocapture` observed **3** passed; `cargo test --locked --test server_startup durable_core_runtime -- --nocapture` observed **2** passed; `cargo clippy --locked --all-targets -- -D warnings` passed; `cargo clippy --locked --features postgres --all-targets -- -D warnings` passed; `cargo test --locked` passed, including the debug perf and perf-comparison targets; `cargo check --locked --features postgres` passed; `cargo check --locked --features fuser --bin stratum-mount` passed; `STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh` passed; `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **9** passed; `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture` observed **16** passed; `cargo audit --deny warnings` passed; `cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture` passed; `git diff --check` passed.

Grounding: `src/backend/runtime.rs`, `src/server/mod.rs`, `src/bin/stratum_server.rs`, `tests/server_startup.rs`, `docs/plans/2026-05-05-durable-core-runtime-boundary.md`.

## Durable Core Transaction Semantics

The durable core transaction semantics slice makes the future Postgres/R2 filesystem and VCS write path explicit before any live durable `CoreDb` routing is enabled.

What is built:

- `src/backend/core_transaction.rs` defines the ordered durable core write path: idempotency reservation, auth/policy/protected-rule preflight, staged object-byte upload, final object-byte promotion, object metadata insert, commit metadata insert, ref compare-and-swap, workspace-head update, audit append, and idempotency completion.
- The contract distinguishes checkpoint semantics from timed failure semantics so ref compare-and-swap is the visibility point, while failures before/during CAS remain uncommitted.
- Failure policy now covers staged-upload cleanup, final-object promotion without metadata, object-metadata-insert boundaries, unreachable commit metadata before ref CAS, post-ref committed partial failures, workspace-head/audit/idempotency replay behavior, and metadata-fenced final-object deletion.
- Destructive final-object cleanup requires an explicit `FinalObjectMetadataFence` token in the contract API.
- Focused tests exercise the transaction ordering, recovery classifications, cleanup fencing, idempotency replay behavior, and existing local memory ref CAS invariants.

What is not built:

- No live Postgres/R2-backed `CoreDb` implementation.
- No HTTP filesystem/VCS route cutover to durable object bytes, object metadata, commit metadata, or ref CAS.
- No production transaction executor that applies this contract across Postgres, R2, idempotency, audit, and workspace metadata.
- No distributed lock, background repair worker, final-object deletion worker, connection pool, hosted TLS/KMS/secrets posture, or multi-node failure recovery.

Focused verification on 2026-05-05 from the `v2/foundation` worktree: `cargo test --locked --release --test perf -- --test-threads=1 --nocapture` passed after every code/docs diff so far in the slice; subagent spec review and code-quality review passed after fixes; focused implementation gates passed: `cargo fmt --all -- --check`, `cargo test --locked backend::core_transaction --lib -- --nocapture`, `cargo test --locked backend::tests::ref_cas_rejects_stale_target_or_version_without_mutation --lib -- --nocapture`, `cargo test --locked backend::tests::source_checked_ref_cas_models_change_request_merge --lib -- --nocapture`, and `cargo clippy --locked --lib --tests -- -D warnings`.

Full verification on 2026-05-05 from the `v2/foundation` worktree passed: `cargo fmt --all -- --check`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo clippy --locked --features postgres --all-targets -- -D warnings`; `cargo test --locked`; `cargo check --locked --features postgres`; `cargo check --locked --features fuser --bin stratum-mount`; `STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh`; required live Postgres `backend::postgres`; required live Postgres `--test server_startup`; `cargo audit --deny warnings`; `cargo test --locked --release --test perf -- --test-threads=1 --nocapture`; `cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture`; and `git diff --check`.

Grounding: `src/backend/core_transaction.rs`, `src/backend/mod.rs`, `docs/plans/2026-05-05-durable-core-transaction-semantics.md`.

## Route-Facing Core Runtime Seam

The route-facing core runtime seam moves HTTP filesystem/search/tree and VCS request handling behind a server-local `CoreDb` boundary while preserving the existing local `StratumDb` behavior.

What is built:

- `src/server/core.rs` defines a crate-local `CoreDb` trait and `LocalCoreRuntime` wrapper over `StratumDb`.
- `ServerState` now carries both `core` and the existing `db`; the direct `db` field remains for routes outside this slice, including runs, reviews, workspace management, and audit helpers.
- Filesystem, search, tree, and VCS route handlers plus their idempotency/preflight helpers now call `state.core.*` instead of `state.db.*`.
- Session resolution for these routes also authenticates through the same local core seam.
- Revert protected-path enforcement uses an object-safe predicate at the core boundary instead of exposing the old generic helper shape to route handlers.
- Added focused route tests proving local-state filesystem and VCS requests work through the core seam.

What is not built:

- No live Postgres/R2-backed `CoreDb` route executor.
- No live durable object-byte, object metadata, commit metadata, or ref compare-and-swap routing for HTTP filesystem/VCS requests.
- No durable commit transaction spanning object bytes, object metadata, commit metadata, ref CAS, idempotency completion, audit append, and workspace-head update.
- No MCP, CLI, FUSE, run-record, review, or workspace-management cutover to the route-facing core seam.

Focused verification on 2026-05-05 from the `v2/foundation` worktree: `cargo test --locked --release --test perf -- --test-threads=1 --nocapture` passed after every code/docs diff so far in the slice; route-seam red tests initially failed before `server::core` and `ServerState.core` existed; subsequent focused route tests passed under local subagent review: `cargo test --locked server::routes_fs::tests::put_fs_routes_through_local_core_runtime --lib -- --nocapture`, `cargo test --locked server::routes_vcs::tests::vcs_routes_use_local_core_runtime --lib -- --nocapture`, `cargo test --locked server::routes_fs::tests --lib -- --nocapture`, and `cargo test --locked server::routes_vcs::tests --lib -- --nocapture`.

Grounding: `src/server/core.rs`, `src/server/mod.rs`, `src/server/middleware.rs`, `src/server/routes_fs.rs`, `src/server/routes_vcs.rs`, `docs/plans/2026-05-05-route-facing-core-runtime-seam.md`.

## Durable Commit Transaction Executor Skeleton

The durable commit transaction executor skeleton creates the first internal commit-execution contract behind the durable `CoreDb` runtime while broad durable HTTP serving remains fail-closed.

What is built:

- `src/backend/core_transaction.rs` now has a stateless `DurableCoreCommitExecutorSkeleton` that reuses `DurableCoreStepSemantics::ordered_write_path()` as the single transaction-order source.
- The skeleton exposes live durable commit execution as disabled, exposes the unresolved prerequisites for live execution as a static slice, and returns a generic redacted unsupported-execution error for preflight.
- The unresolved prerequisites are explicit: durable object byte writes, live tree construction, source filesystem snapshotting, workspace-head coupling, audit/idempotency completion, commit locking/fencing, and repair worker coverage.
- `DurableCoreRuntime::commit_transaction_skeleton` exposes the internal runtime seam for tests and future executor work.
- `DurableCoreRuntime::commit_as` references the skeleton but still returns the existing route-level redacted `NotSupported` error and does not mutate durable object, commit, ref, workspace, audit, or idempotency stores.
- Durable startup remains fail-closed for `STRATUM_CORE_RUNTIME=durable-cloud` before local state, durable backend validation, migration preflight, or serving.

What is not built:

- No live durable `POST /vcs/commit` execution.
- No durable object-byte writes, tree construction, commit metadata insert, ref CAS, workspace-head update, audit append, idempotency completion, or object repair path from this skeleton.
- No durable auth/session path, no distributed lock/fencing implementation, no hosted R2 routing, and no background repair worker.
- No local-route behavior change; live HTTP filesystem/search/tree/VCS routes continue through `LocalCoreRuntime` and local `StratumDb`.

Focused verification on 2026-05-06 from the `v2/foundation` worktree: subagent spec review found no architecture/scope issues; subagent code-quality review found a brittle pointer-identity assertion in the skeleton ordering test, and main removed it. `cargo fmt --check` passed; `cargo test --locked backend::core_transaction --lib -- --nocapture` observed **16** passed; `cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture` observed **16** passed; `cargo test --locked server::tests::open_ --lib -- --nocapture` observed **3** passed; `cargo test --locked --test server_startup durable_core_runtime -- --nocapture` observed **2** passed; `cargo clippy --locked --all-targets -- -D warnings` passed; `cargo clippy --locked --features postgres --all-targets -- -D warnings` passed; `cargo test --locked` passed, including **404** lib tests, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, **9** default startup process tests, and doc tests; `cargo check --locked --features postgres` passed; `cargo check --locked --features fuser --bin stratum-mount` passed; `STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh` exited `ROLLBACK`; required live Postgres `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed; required live Postgres `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture` observed **16** passed; `cargo audit --deny warnings` passed; `cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture` passed; and `git diff --check` passed. Measured release perf after meaningful diffs used `sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture`; the warm post-review-fix run passed **37** tests in **11.65s real**, **10.83s user**, **0.43s sys**, with **118,898,688 bytes max RSS** and **98,828,840 bytes peak memory footprint**. A cold compiler-inclusive run also passed in **43.58s real** with **1,346,600,960 bytes max RSS**; warm runtime numbers are the durable-core footprint signal for this static skeleton slice. GPU efficiency is not applicable.

Grounding: `src/backend/core_transaction.rs`, `src/server/core.rs`, `docs/plans/2026-05-06-durable-commit-transaction-executor-skeleton.md`.

## Durable Commit Transaction Metadata Preflight

The durable commit transaction metadata preflight adds the first internal read-only commit executor step behind the durable `CoreDb` runtime while broad durable HTTP serving remains fail-closed.

What is built:

- `src/backend/core_transaction.rs` now exposes `DurableCoreCommitParentState` and `DurableCoreCommitMetadataPreflight` as the metadata-only parent snapshot for future durable commit execution.
- The preflight snapshot targets `main`, reports either unborn parent state or an existing parent commit target/version, and reuses the durable commit skeleton write-order and unresolved-prerequisite contract.
- `DurableCoreRuntime::commit_metadata_preflight` reads durable ref metadata for `main` and checks that an existing parent target has durable commit metadata.
- Missing parent commit metadata returns a generic redacted `CorruptStore` error without including ref names, commit IDs, commit messages, sessions, tokens, workspace secrets, backend mode values, or raw request data.
- If `main` changes while the preflight is checking missing parent metadata, the runtime re-reads the ref and returns the sanitized compare-and-swap mismatch instead of misreporting the old parent as corrupt.
- `DurableCoreRuntime::commit_as` remains route-level fail-closed with the existing redacted `NotSupported` response and does not call the metadata preflight from live HTTP routes.
- Durable startup remains fail-closed for `STRATUM_CORE_RUNTIME=durable-cloud` before local state, durable backend validation, migration preflight, or serving.

What is not built:

- No live durable `POST /vcs/commit` execution.
- No object-byte writes, tree construction, object metadata insert, commit metadata insert, ref CAS mutation, workspace-head update, audit append, idempotency completion, or repair-worker scheduling.
- No durable auth/session path, durable filesystem/search/tree route execution, durable list/log/status/diff/revert execution, distributed lock/fencing, hosted R2 routing, connection pool, or hosted TLS/KMS/secrets posture.
- No local-route behavior change; live HTTP filesystem/search/tree/VCS routes continue through `LocalCoreRuntime` and local `StratumDb`.

Focused verification on 2026-05-06 from the `v2/foundation` worktree: the preflight contract tests passed; `cargo test --locked backend::core_transaction::tests::durable_core_commit_metadata_preflight --lib -- --nocapture` observed **3** passed; `cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture` observed **20** passed; `cargo test --locked server::tests::open_ --lib -- --nocapture` observed **3** passed; and `cargo test --locked --test server_startup durable_core_runtime -- --nocapture` observed **2** passed. Subagent spec review and code-quality/security review both reported no findings; residual risks are that the durable runtime remains unrouted, the preflight snapshot is intentionally non-transactional and must be paired with later CAS/fencing, and future `CommitStore::contains` implementations must remain side-effect-free/existence-oriented for this preflight to stay lightweight.

Full verification on 2026-05-06 from the `v2/foundation` worktree passed: `cargo fmt --check`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo clippy --locked --features postgres --all-targets -- -D warnings`; `cargo test --locked` including **411** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, **9** default startup process tests, and doc tests; `cargo check --locked --features postgres`; `cargo check --locked --features fuser --bin stratum-mount`; `STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh` exited `ROLLBACK`; required live Postgres `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed; required live Postgres `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture` observed **16** passed; `cargo audit --deny warnings` passed; `cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture` passed; and `git diff --check` passed. Measured release perf after the code diff used `sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture`; the final warm implementation run passed **37** tests in **11.40s real**, **10.69s user**, **0.32s sys**, with **119,226,368 bytes max RSS** and **99,172,928 bytes peak memory footprint**. GPU efficiency is not applicable to this metadata-only backend path.

Grounding: `src/backend/core_transaction.rs`, `src/server/core.rs`, `docs/plans/2026-05-06-durable-commit-transaction-metadata-preflight.md`.

## Guarded Live Durable VCS Commit Routing

The guarded durable commit route makes exactly one durable core write path executable from HTTP: `POST /vcs/commit` can run through the durable object, commit, ref, workspace, audit, and idempotency stores when explicitly gated on. Broad durable core runtime serving remains fail-closed.

What is built:

- `STRATUM_DURABLE_COMMIT_ROUTE=1` enables the guarded route only with `STRATUM_BACKEND=durable` and the `postgres` feature. Startup still rejects `STRATUM_CORE_RUNTIME=durable-cloud` before local state, durable backend validation, migration preflight, or serving.
- Durable server store construction now composes Postgres metadata stores with the R2-backed object adapter for the guarded commit route, while the route-facing core remains `LocalCoreRuntime` for all other filesystem/search/tree/VCS/auth methods.
- `POST /vcs/commit` preserves the existing route preflight order: session, workspace header, mutation authorization, protected `main` check, and idempotency reservation happen before durable mutation.
- The guarded route composes the internal durable sequence: commit metadata preflight, durable parent source snapshot, local `StratumDb` filesystem snapshot, blocking write-plan build, object convergence, commit metadata insert, ref CAS visibility, and post-CAS completion.
- Successful commits persist planned blob/tree objects, commit metadata, `main` ref visibility, workspace head, audit event, and idempotency replay response without calling the local VCS commit path.
- Idempotency replay after confirmed durable visibility returns the original response without a second commit or audit event. Stale `main` CAS races return sanitized conflict and abort idempotency.
- Metadata/ref acknowledgement-loss paths recover by re-reading durable stores before proceeding. Confirmed pre-visibility failures do not record a committed replay; unconfirmed pre-visibility recovery errors return a redacted recovery-required response without completing idempotency.
- Post-CAS workspace/audit/idempotency failures return the existing redacted `202 Accepted` partial response only after ref visibility is confirmed.
- Existing-parent commits reconstruct the durable parent tree from `CommitStore` and `ObjectStore`; `BlobObjectStore::object_len` avoids downloading unchanged parent blob bytes just to compute path-record sizes.

What is not built:

- No durable auth/session path, durable filesystem/search/tree serving, durable VCS status/diff/revert serving, or broad `STRATUM_CORE_RUNTIME=durable-cloud` route cutover. Guarded durable log/ref metadata route consistency was added in the later guarded metadata slice.
- The guarded route still uses the local `StratumDb` filesystem snapshot as the commit source until durable FS mutation/source routing is designed.
- No persisted post-CAS recovery-claim adapter, background repair worker, worker wakeup loop, durable recovery/status endpoint, distributed lock/fencing layer, final-object deletion, hosted TLS/KMS/secrets posture, or idempotency retention model.
- No automatic replay after process restart for route-local pre-visibility uncertainty; the next recovery slice must persist and drain those states deliberately.

Verification on 2026-05-07 from the `v2/foundation` worktree passed: `cargo fmt --all -- --check`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo clippy --locked --all-targets --features postgres -- -D warnings`; `cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture` observed **8** passed; `cargo test --locked --lib --tests` observed **478** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, and **9** default startup process tests passed; `cargo test --locked --features postgres backend::postgres::tests::postgres_metadata_store_round_trips_backend_contracts --lib -- --nocapture` passed with live Postgres skipped because `STRATUM_POSTGRES_TEST_URL` was unset; `cargo test --locked --features postgres --test server_startup durable_env -- --nocapture` passed with live Postgres skipped for the same reason; `cargo audit --deny warnings` passed; and `git diff --check` passed. The final warm docs/status perf run used `sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture` and passed **37** tests in **12.41s real**, **11.03s user**, **0.59s sys**, with **118,505,472 bytes max RSS** and **98,402,832 bytes peak memory footprint**. GPU efficiency is not applicable to this backend route slice.

Grounding: `src/server/routes_vcs.rs`, `src/server/mod.rs`, `src/server/core.rs`, `src/backend/runtime.rs`, `src/backend/core_transaction.rs`, `src/backend/blob_object.rs`, `docs/plans/2026-05-07-guarded-live-durable-vcs-commit-routing.md`.

## Durable Commit Post-CAS Completion And Recovery Envelope

The durable commit post-CAS completion/recovery envelope adds the internal completion path after a durable commit is visible through `main`, while durable `POST /vcs/commit` routing remains fail-closed.

What is built:

- `DurableCoreCommitObjectTreeWritePlan::post_cas_envelope` binds a committed response, audit event, optional workspace ID, and optional idempotency reservation to the same write plan, commit metadata insert, and ref CAS visibility summary.
- The envelope validates the visible repo/ref/commit/version, rejects unbound audit events before side effects, and keeps debug output redacted for commit messages, authors, paths, response bodies, audit details, and idempotency tokens.
- Post-CAS completion runs optional workspace-head update, audit append, and optional idempotency completion in order.
- Workspace-head completion uses a new `WorkspaceMetadataStore::update_head_commit_if_current` compare-and-swap method, including Postgres support through `head_commit IS NOT DISTINCT FROM`, so recovery cannot roll a workspace head back after a newer visible commit advances it.
- Workspace-head or audit failure returns an explicit partial outcome and attempts idempotency completion with a fixed redacted committed-partial response.
- Idempotency completion failure returns an explicit partial outcome after workspace/audit completion; recovery can run the idempotency step alone without appending duplicate audit events.
- `DurableCorePostCasRecoveryClaimStore` defines the future durable recovery state machine for post-CAS steps, with in-memory test coverage for active claims, duplicate-worker blocking, failure backoff, retry attempts, stale-token rejection, terminal completion, terminal poison, redacted diagnostics, and bounded lease/backoff durations.
- The SMFS extraction remains pattern-only: claim/finalize/backoff/poison lifecycle ideas informed the state machine, but no latest-wins queue behavior, SQLite inode/chunk cache, or SMFS tree/object model was imported.

What is not built:

- No live durable `POST /vcs/commit` route execution, durable auth/session path, durable filesystem/search/tree serving, hosted R2 routing cutover, or Postgres recovery-claim adapter.
- No background repair worker, bounded worker pool, `Notify`/`JoinSet` wakeup loop, recovery queue persistence, final-object deletion, or distributed lock/fencing layer.
- No route-level idempotency reservation reconstruction after process restart; restart-safe replay still needs the future durable worker/envelope persistence slice.
- No local-route behavior change; live HTTP filesystem/search/tree/VCS routes continue through `LocalCoreRuntime` and local `StratumDb`.

Focused verification on 2026-05-06 from the `v2/foundation` worktree: `cargo fmt --all -- --check` passed; `cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_recovery --lib -- --nocapture` observed **9** passed; `cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_completion --lib -- --nocapture` observed **10** passed; `cargo test --locked workspace::tests --lib -- --nocapture` observed **37** passed; `cargo test --locked backend::core_transaction::tests::durable_core_commit_ref_cas_visibility --lib -- --nocapture` observed **12** passed; `cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture` observed **20** passed; `cargo test --locked server::tests::open_ --lib -- --nocapture` observed **3** passed; `cargo test --locked --test server_startup durable_core_runtime -- --nocapture` observed **2** passed; `cargo check --locked --features postgres` passed; and `git diff --check` passed. Subagent code-quality re-review reported no findings after main-session fixes for workspace-head fencing, step-specific retry, audit binding, and bounded recovery timing. Measured release perf after the final implementation diff used `sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture`; the warm implementation run passed **37** tests in **8.44s real**, **7.65s user**, **0.29s sys**, with **120,242,176 bytes max RSS** and **100,155,920 bytes peak memory footprint**. A compiler-inclusive run also passed in **31.71s real**, **95.75s user**, **4.07s sys**, with **1,308,901,376 bytes max RSS** and **101,188,136 bytes peak memory footprint**. GPU efficiency is not applicable to this metadata/control-plane envelope.

Grounding: `src/backend/core_transaction.rs`, `src/workspace/mod.rs`, `src/backend/postgres.rs`, `docs/plans/2026-05-06-post-cas-completion-recovery-envelope.md`.

## Durable Commit Ref CAS Visibility

The durable commit ref CAS visibility slice adds the internal step that makes a previously inserted durable commit reachable through `main`, while durable `POST /vcs/commit` routing remains fail-closed.

What is built:

- `DurableCoreCommitObjectTreeWritePlan::apply_ref_cas_visibility` accepts a matching `DurableCoreCommitMetadataInsert`, validates that the metadata summary belongs to the same write plan, and applies only the durable `RefStore::update` visibility mutation.
- Metadata summaries are privately bound to the source write plan with a deterministic plan fingerprint before any ref mutation; mismatched root tree, parent state, changed-path shape, or unbound metadata is rejected without touching refs.
- The executor derives `RefExpectation::MustNotExist` for unborn `main` and `RefExpectation::Matches { target, version }` for existing `main`, using the parent target/version from the source snapshot.
- Successful visibility validates the returned ref record field-by-field and returns a redacted `DurableCoreCommitRefCasVisibility` summary with repo ID, ref name, commit ID, and version.
- Stale target/version races return a fixed sanitized compare-and-swap mismatch, and non-CAS ref-store failures or mismatched returned records are wrapped in fixed redacted durable errors.
- Focused tests cover unborn `main` creation, existing `main` update, stale unborn/existing/version races, metadata mismatch and unbound metadata rejection, CAS and non-CAS error redaction, mismatched returned records, and debug redaction.

What is not built:

- No workspace-head update, audit append, idempotency completion/replay, repair scheduling, distributed lock/fencing, or cleanup policy around a visible commit.
- No live durable `POST /vcs/commit` route execution, durable auth/session path, durable filesystem/search/tree serving, hosted R2 routing cutover, or Postgres-specific route change.
- No local-route behavior change; live HTTP filesystem/search/tree/VCS routes continue through `LocalCoreRuntime` and local `StratumDb`.

Focused verification on 2026-05-06 from the `v2/foundation` worktree: the RED step failed before implementation because `apply_ref_cas_visibility` did not exist. After implementation and main-session review fixes, `cargo fmt --all -- --check` passed; `cargo test --locked backend::core_transaction::tests::durable_core_commit_ref_cas_visibility --lib -- --nocapture` observed **12** passed; adjacent metadata-insert, object-convergence, write-plan, durable runtime, open-guard, and startup fail-closed tests passed during focused verification; and `git diff --check` passed. Subagent spec review found no findings. Subagent code-quality review found that metadata summaries needed tighter plan binding and that CAS error classification was too broad; main-session fixes added the private plan fingerprint, exact CAS mismatch classification, stale-version coverage, unbound-metadata coverage, and prefixed-CAS error redaction coverage. Measured release perf after the final implementation diff used `sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture`; the warm implementation run passed **37** tests in **7.97s real**, **7.44s user**, **0.25s sys**, with **120,291,328 bytes max RSS** and **100,172,304 bytes peak memory footprint**. GPU efficiency is not applicable to this metadata/ref path.

Grounding: `src/backend/core_transaction.rs`, `docs/plans/2026-05-06-durable-ref-cas-visibility.md`.

## Durable Commit Metadata Insert Executor

The durable commit metadata insert executor adds the first internal commit metadata write step after object convergence while durable `POST /vcs/commit` routing remains fail-closed.

What is built:

- `DurableCoreCommitObjectTreeWritePlan::insert_commit_metadata` accepts a matching `DurableCoreObjectConvergence`, validates that the converged object summary still matches the write plan, and inserts an unreachable `CommitRecord` through `CommitStore`.
- Parent commit IDs are derived from the source snapshot parent state; existing-parent commits are checked through `CommitStore::contains` before insert, while unborn commits insert with no parents.
- Commit IDs are derived with the same local VCS identity rule by hashing a serialized `CommitObject` with the placeholder zero ID, final root tree, parent, timestamp, author, message, and changed paths.
- The executor validates the returned store record field-by-field, including repo ID, commit ID, root tree, parents, timestamp, author, message, and changed paths.
- Downstream parent-check, missing-parent, convergence-mismatch, insert, and mismatched-return failures are wrapped in fixed redacted `CorruptStore` messages so commit messages, authors, paths, bytes, tokens, and raw store errors do not leak.
- `DurableCoreCommitMetadataInsert` exposes only repo ID, commit ID, root tree ID, parent IDs, changed-path count, and timestamp through accessors and custom redacted `Debug`.
- Focused tests cover unborn insert without ref visibility, existing-parent insert, missing parent without mutation, leaky parent-check failure redaction, mismatched convergence, idempotent duplicate insert, conflicting duplicate redaction, root-tree FK/store failure redaction, mismatched returned records, and debug redaction.

What is not built:

- No ref CAS visibility; inserted commits remain unreachable by refs in this slice.
- No workspace-head update, audit append, idempotency completion/replay, repair scheduling, distributed lock/fencing, or cleanup policy.
- No live durable `POST /vcs/commit` route execution, durable auth/session path, durable filesystem/search/tree serving, hosted R2 routing cutover, or Postgres-specific route change.
- No local-route behavior change; live HTTP filesystem/search/tree/VCS routes continue through `LocalCoreRuntime` and local `StratumDb`.

Focused verification on 2026-05-06 from the `v2/foundation` worktree: the RED step failed before implementation because `insert_commit_metadata` and the helper record builder did not exist. After implementation and review fixes, `cargo fmt --all -- --check` passed; `cargo test --locked backend::core_transaction::tests::durable_core_commit_metadata_insert --lib -- --nocapture` observed **10** passed; adjacent object-convergence, write-plan, durable runtime, open-guard, and startup fail-closed tests passed during focused verification; and `git diff --check` passed. Subagent spec review found no findings. Subagent code-quality review found avoidable duplicate clones and requested branch coverage for parent-check and convergence-mismatch errors; main-session fixes added coverage and moved commit metadata strings/changed paths into the final `CommitRecord` after hashing. Measured release perf after the final implementation diff used `sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture`; the warm implementation run passed **37** tests in **8.38s real**, **7.53s user**, **0.30s sys**, with **118,652,928 bytes max RSS** and **98,583,080 bytes peak memory footprint**. GPU efficiency is not applicable to this metadata-only backend path.

Grounding: `src/backend/core_transaction.rs`, `docs/plans/2026-05-06-durable-commit-metadata-insert-executor.md`.

## Durable Planned Object Convergence Executor

The durable planned object convergence executor adds the first internal object-write step for future durable commit execution while durable `POST /vcs/commit` routing remains fail-closed.

What is built:

- `DurableCoreCommitObjectTreeWritePlan::converge_objects` writes the plan's already-validated blob/tree objects through `ObjectStore::put` in deterministic child-before-parent order.
- The executor validates every returned `StoredObject` against the expected repo ID, object ID, kind, and bytes before adding it to the convergence summary.
- Downstream `ObjectStore::put` and root verification failures are wrapped into fixed redacted `CorruptStore` messages at this seam so future adapters cannot leak paths, object bytes, tokens, or raw request data through convergence errors.
- After all planned objects are written, convergence verifies that `root_tree_id` exists as `ObjectKind::Tree`, establishing the object-store precondition for the later `commits.root_tree_id` foreign key.
- `DurableCoreObjectConvergence` and `DurableCoreConvergedObject` expose only repo/root/object IDs, object kind, object count, and byte lengths through accessors and custom redacted `Debug`.
- Focused tests cover successful convergence, idempotent replay against matching existing objects, mismatched store responses, missing root after all puts, wrapped leaky downstream put/root-check errors, and debug/error redaction.

What is not built:

- No `CommitStore::insert`.
- No ref CAS visibility, workspace-head update, audit append, idempotency completion/replay, repair scheduling, distributed lock/fencing, or cleanup policy.
- No live durable `POST /vcs/commit` route execution, durable auth/session path, durable filesystem/search/tree serving, hosted R2 routing cutover, or Postgres-specific object routing change.
- No local-route behavior change; live HTTP filesystem/search/tree/VCS routes continue through `LocalCoreRuntime` and local `StratumDb`.

Focused verification on 2026-05-06 from the `v2/foundation` worktree: the RED step failed before implementation because `DurableCoreCommitObjectTreeWritePlan::converge_objects` did not exist. After implementation and main-session hardening, `cargo fmt --all -- --check` passed; `cargo test --locked backend::core_transaction::tests::durable_core_commit_object_convergence --lib -- --nocapture` observed **7** passed; `cargo test --locked backend::core_transaction::tests::durable_core_commit_write_plan --lib -- --nocapture` observed **8** passed; `cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture` observed **20** passed; `cargo test --locked server::tests::open_ --lib -- --nocapture` observed **3** passed; `cargo test --locked --test server_startup durable_core_runtime -- --nocapture` observed **2** passed; and `git diff --check` passed. Subagent spec and code-quality reviews found no blocking findings. Main-session hardening then wrapped downstream object-store errors with fixed redacted messages and added order/redaction regressions; incremental reviews again found no findings. Measured release perf after the implementation/review-fix diff used `sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture`; the warm implementation run passed **37** tests in **7.91s real**, **7.45s user**, **0.23s sys**, with **118,521,856 bytes max RSS** and **98,435,624 bytes peak memory footprint**. GPU efficiency is not applicable to this object metadata path.

Full verification on 2026-05-06 from the `v2/foundation` worktree passed: `cargo fmt --all -- --check`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo clippy --locked --features postgres --all-targets -- -D warnings`; `cargo test --locked` including **426** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, **9** default startup process tests, and doc tests; `cargo check --locked --features postgres`; `cargo check --locked --features fuser --bin stratum-mount`; required live Postgres `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh` exited `ROLLBACK`; required live Postgres `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed; required live Postgres `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture` observed **16** passed; `cargo audit --deny warnings` scanned **408** crate dependencies; `cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture` passed; and `git diff --check` passed. The final warm release perf run passed **37** tests in **8.14s real**, **7.47s user**, **0.27s sys**, with **120,258,560 bytes max RSS** and **100,155,944 bytes peak memory footprint**.

Grounding: `src/backend/core_transaction.rs`, `docs/plans/2026-05-06-durable-planned-object-convergence-executor.md`.

## Durable Commit Object/Tree Write-Plan Preflight

The durable commit object/tree write-plan preflight adds the first read-only object identity plan for future durable commit execution while durable `POST /vcs/commit` routing remains fail-closed.

What is built:

- `src/backend/core_transaction.rs` now exposes `DurableCoreCommitSourceSnapshot`, `DurableCorePlannedObject`, and `DurableCoreCommitObjectTreeWritePlan` as internal durable transaction planning types.
- The source snapshot contract carries explicit parent/ref state plus base path records; unborn sources reject non-empty base records so future source freshness remains tied to parent ref-version state, not timestamps.
- The planner traverses a source `VirtualFs` snapshot, computes blob and symlink blob IDs, serializes directory `TreeObject` values with the same local VCS object identity rules, and records the final `root_tree_id`.
- Planned objects are deduplicated by raw `ObjectId` when kind and bytes match, cross-kind or cross-byte object ID collisions return a redacted planning error, and accepted objects are emitted in deterministic child-before-parent order with the root tree last.
- The plan computes normalized `ChangedPath` output from explicit base records and current worktree records, including create, delete, modified, metadata-only, and rename-equivalent create/delete cases.
- The plan can map planned objects into existing `ObjectWrite` values for a repo, but this is only data construction.

What is not built:

- No `ObjectStore::put`, R2 byte-store write, or Postgres object metadata write.
- No commit metadata insert, ref CAS mutation, workspace-head update, audit append, idempotency completion/replay, repair scheduling, or cleanup policy.
- No live durable `POST /vcs/commit` route execution, durable auth/session path, durable filesystem/search/tree serving, distributed lock/fencing, or hosted R2 routing.
- No local-route behavior change; live HTTP filesystem/search/tree/VCS routes continue through `LocalCoreRuntime` and local `StratumDb`.

Focused verification on 2026-05-06 from the `v2/foundation` worktree: the TDD red step failed before implementation because `DurableCoreCommitSourceSnapshot`, `DurableCorePlannedObject`, and `DurableCoreCommitObjectTreeWritePlan` did not exist. After implementation, `cargo fmt --all -- --check` passed; `cargo test --locked backend::core_transaction::tests::durable_core_commit_write_plan --lib -- --nocapture` observed **6** passed; `cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture` observed **20** passed; `cargo test --locked server::tests::open_ --lib -- --nocapture` observed **3** passed; `cargo test --locked --test server_startup durable_core_runtime -- --nocapture` observed **2** passed; and `git diff --check` passed. Subagent spec and code-quality reviews found that `(ObjectKind, ObjectId)` dedupe could produce a write set that cannot converge against stores keyed by raw `ObjectId`, and that derived `Debug` could render planned bytes and paths. The review fix added raw-ID collision rejection, an indexed dedupe path, custom redacted `Debug`, and focused regressions; `cargo test --locked backend::core_transaction::tests::durable_core_commit_write_plan --lib -- --nocapture` then observed **8** passed. Measured release perf after the review-fix diff used `sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture`; the warm review-fix run passed **37** tests in **12.03s real**, **10.93s user**, **0.45s sys**, with **118,882,304 bytes max RSS** and **98,796,048 bytes peak memory footprint**. GPU efficiency is not applicable to this read-only backend planning path.

Full verification on 2026-05-06 from the `v2/foundation` worktree passed: `cargo fmt --all -- --check`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo clippy --locked --features postgres --all-targets -- -D warnings`; `cargo test --locked` including **419** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, **9** default startup process tests, and doc tests; `cargo check --locked --features postgres`; `cargo check --locked --features fuser --bin stratum-mount`; required live Postgres `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres ./scripts/check-postgres-migrations.sh` exited `ROLLBACK`; required live Postgres `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed; required live Postgres `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres --test server_startup -- --nocapture` observed **16** passed; `cargo audit --deny warnings` scanned **408** crate dependencies; `cargo test --locked --release --test perf_comparison -- --test-threads=1 --nocapture` passed; and `git diff --check` passed. The final warm release perf run passed **37** tests in **11.87s real**, **11.00s user**, **0.46s sys**, with **120,078,336 bytes max RSS** and **100,024,896 bytes peak memory footprint**.

Grounding: `src/backend/core_transaction.rs`, `docs/plans/2026-05-06-durable-commit-object-tree-write-plan-preflight.md`.

## Durable Create-Ref Executor Path

The durable create-ref executor path completes the second narrow ref-management method inside the internal durable `CoreDb` implementation while broad durable HTTP serving remains fail-closed.

What is built:

- `DurableCoreRuntime::create_ref` now validates durable ref names and target commit IDs without echoing raw request values.
- The method rejects duplicate refs before checking target commit metadata, preserving local VCS duplicate-first behavior.
- Existing target commit metadata is required before durable ref creation, using the backend commit-store existence probe rather than full commit-record hydration where adapters support it.
- Ref creation goes through durable `RefStore` compare-and-swap with `RefExpectation::MustNotExist` and returns the existing `DbVcsRef` shape with version 1.
- Duplicate refs, missing target commits, invalid inputs, and a forced race between duplicate-ref precheck and missing-target lookup leave the ref unchanged or return the sanitized duplicate conflict.
- Durable startup remains fail-closed for `STRATUM_CORE_RUNTIME=durable-cloud` before local state, durable backend validation, migration preflight, or serving.

What is not built:

- No live durable server startup, durable auth, or HTTP route serving for this path.
- No durable `POST /vcs/commit`, list refs, filesystem/search/tree, log/status/diff/revert, object-byte routing, audit/idempotency/workspace-head transaction completion, distributed lock, repair worker, connection pool, or hosted TLS/KMS/secrets posture.
- No local-route behavior change; live HTTP filesystem/search/tree/VCS routes continue through `LocalCoreRuntime` and local `StratumDb`.

Focused verification on 2026-05-06 from the `v2/foundation` worktree: subagent implementation worker confirmed the six new create-ref tests failed red against the previous `NotSupported` implementation; main-session review tightened the race test to create the duplicate ref at an existing commit and assert rendered duplicate errors stay redacted; subagent spec review passed; subagent code-quality review found a commit-hydration performance risk in target-existence checks, which was fixed by adding `CommitStore::contains` plus a Postgres `SELECT EXISTS` adapter path and switching durable create/update-ref to that probe. The review-fix red step moved the forced race onto `contains` while the executor still used `get`, and the two race tests failed until the executor was switched. `cargo fmt --all -- --check` passed; `cargo test --locked backend::tests::commit_inserts_are_idempotent_and_list_newest_first --lib -- --nocapture` observed **1** passed; `cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture` observed **15** passed; `cargo test --locked server::tests::open_ --lib -- --nocapture` observed **3** passed; `cargo test --locked --test server_startup durable_core_runtime -- --nocapture` observed **2** passed; `cargo clippy --locked --all-targets -- -D warnings` passed; `cargo clippy --locked --features postgres --all-targets -- -D warnings` passed; and `git diff --check` passed. Measured release perf after meaningful diffs used `sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture`; the warm post-review-fix run passed **37** tests in **8.27s real**, **7.70s user**, **0.28s sys**, with **119,046,144 bytes max RSS** and **98,976,296 bytes peak memory footprint**. GPU efficiency is not applicable to this metadata-only backend path.

Grounding: `src/server/core.rs`, `src/backend/mod.rs`, `docs/plans/2026-05-06-durable-create-ref-executor-parity.md`.

## Durable Update-Ref Executor Path

The durable update-ref executor path is the first narrow commit-oriented method that can execute through the internal durable `CoreDb` implementation while broad durable HTTP serving remains fail-closed.

What is built:

- `DurableCoreRuntime::update_ref` now validates durable ref names and commit IDs without echoing raw request values.
- The method checks the current ref target/version expectation before checking the new target commit, preserving local VCS CAS-first behavior.
- Existing target commit metadata is required before durable ref compare-and-swap.
- The durable ref update returns the existing `DbVcsRef` shape with name, full target commit ID, and incremented version.
- Stale expectations, missing target commits, invalid inputs, and a forced race between current-ref read and missing-target lookup all leave the ref unchanged or return the sanitized CAS mismatch.
- Durable startup remains fail-closed for `STRATUM_CORE_RUNTIME=durable-cloud` before local state, durable backend validation, migration preflight, or serving.

What is not built:

- No live durable server startup, durable auth, or HTTP route serving for this path.
- No durable `POST /vcs/commit`, `POST /vcs/refs`, list refs, filesystem/search/tree, log/status/diff/revert, object-byte routing, audit/idempotency/workspace-head transaction completion, distributed lock, repair worker, connection pool, or hosted TLS/KMS/secrets posture.
- No local-route behavior change; live HTTP filesystem/search/tree/VCS routes continue through `LocalCoreRuntime` and local `StratumDb`.

Focused verification on 2026-05-06 from the `v2/foundation` worktree: subagent spec review passed; subagent code-quality review found a race in missing-target error ordering that was fixed and re-reviewed cleanly; `cargo fmt --all -- --check` passed; `cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture` observed **9** passed; `cargo test --locked server::tests::open_ --lib -- --nocapture` observed **3** passed; `cargo test --locked --test server_startup durable_core_runtime -- --nocapture` observed **2** passed; `cargo clippy --locked --lib --tests -- -D warnings` passed; and `git diff --check` passed. Measured release perf after meaningful diffs used `sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture`; the warm post-docs run passed **37** tests in **11.63s real**, **10.71s user**, **0.41s sys**, with **119,308,288 bytes max RSS** and **99,222,080 bytes peak memory footprint**. GPU efficiency is not applicable to this metadata-only backend path.

Grounding: `src/server/core.rs`, `src/backend/mod.rs`, `docs/plans/2026-05-06-durable-update-ref-executor-path.md`.

## Durable CoreDb Implementation Path

The durable CoreDb implementation path added the future server-core runtime shape before any live Postgres/R2 filesystem or VCS route execution was enabled.

What is built:

- `src/server/core.rs` now includes an internal `DurableCoreRuntime` alongside `LocalCoreRuntime` behind the existing route-facing `CoreDb` seam.
- `DurableCoreRuntime` owns one repo id and one composed `StratumStores` bundle for object, commit, ref, workspace metadata, review, idempotency, and audit stores.
- The runtime binds its write-order contract to `DurableCoreStepSemantics::ordered_write_path()`.
- At landing time, durable route execution remained disabled: every route-facing auth, filesystem, search, tree, and VCS method failed closed with a stable redacted `NotSupported` error.
- Startup fail-closed behavior is preserved for `STRATUM_CORE_RUNTIME=durable-cloud` before local state, durable backend validation, migration preflight, or serving.

What is not built:

- No live Postgres/R2 filesystem or VCS route executor was enabled by this slice.
- No object-byte route handling, sparse tree/path reconstruction, distributed transaction or lock layer, repair worker, connection pool, hosted TLS/KMS/secrets posture, or durable serving cutover.
- No local-route dispatch behavior change; local HTTP filesystem/search/tree/VCS routes continue through `LocalCoreRuntime`.

Focused verification on 2026-05-05 from the `v2/foundation` worktree: subagent spec review passed; subagent code-quality review passed after review fixes; `cargo fmt --all -- --check` passed; `cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture` observed **2** passed; `cargo test --locked server::tests::open_ --lib -- --nocapture` observed **3** passed; `cargo test --locked --test server_startup durable_core_runtime -- --nocapture` observed **2** passed; `cargo clippy --locked --lib --tests -- -D warnings` passed; and `git diff --check` passed. Measured release perf after meaningful diffs used `sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture`; the warm post-review-fix run passed **37** tests in **7.78s real**, **7.34s user**, **0.18s sys**, with **118,669,312 bytes max RSS** and **98,599,464 bytes peak memory footprint**. A cold compile-plus-perf run also passed and reported compiler-inclusive max RSS, so warm runtime numbers are the durable-core runtime footprint signal for this slice.

Grounding: `src/server/core.rs`, `src/backend/core_transaction.rs`, `src/backend/mod.rs`, `docs/plans/2026-05-05-durable-core-db-implementation-path.md`.

## Durable Final-Object Repair/Fencing Conformance

The durable final-object repair/fencing conformance slice proves the backend repair path needed after final object-byte promotion succeeds but metadata insertion fails, without enabling live durable route execution or final-object deletion.

What is built:

- `src/backend/postgres.rs` now has live Postgres conformance tests that compose `BlobObjectStore` over `LocalBlobStore` bytes and `PostgresMetadataStore` for both object metadata and cleanup-claim leases.
- The tests cover recreating missing metadata for a final object orphan, skipping an active cleanup claim, and recording a hash-mismatch repair failure without deleting bytes.
- The successful repair test now proves the cleanup claim is completed through the Postgres row state, not merely hidden by an active lease.
- The failure test proves `last_error` is recorded and the failed claim can be retried after lease expiry.
- Local test byte directories use a drop guard so assertion panics do not leave temporary blob roots behind.
- `ObjectOrphanCleanupMode::FinalObjectsMissingMetadataDelete` remains fail-closed in this conformance path.

What is not built:

- No background repair worker or HTTP/MCP/CLI/FUSE repair endpoint.
- No live Postgres/R2-backed filesystem or VCS route executor.
- No final-object deletion implementation, new cleanup-claim kind, or storage-level `FinalObjectMetadataFence` wiring.
- Metadata writers still do not consult deletion fences; final-object deletion remains blocked until that contract exists.

Focused verification on 2026-05-06 from the `v2/foundation` worktree: subagent spec review passed; subagent code-quality review passed after review fixes; `cargo fmt --all -- --check` passed; `STRATUM_POSTGRES_TEST_REQUIRED=1 STRATUM_POSTGRES_TEST_URL=postgres://127.0.0.1/postgres cargo test --locked --features postgres backend::postgres::tests::postgres_blob_object_repair --lib -- --nocapture` observed **3** passed; `cargo test --locked backend::blob_object --lib -- --nocapture` observed **22** passed; `cargo test --locked backend::object_cleanup --lib -- --nocapture` observed **6** passed; `cargo test --locked backend::core_transaction --lib -- --nocapture` observed **12** passed; `cargo clippy --locked --features postgres --all-targets -- -D warnings` passed; and `git diff --check` passed. Measured release perf after meaningful diffs used `sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture`; the warm post-review-fix run passed **37** tests in **11.93s real**, **10.95s user**, **0.44s sys**, with **119,242,752 bytes max RSS** and **99,140,112 bytes peak memory footprint**. GPU efficiency is not applicable to this storage-path slice.

Grounding: `src/backend/postgres.rs`, `src/backend/blob_object.rs`, `src/backend/object_cleanup.rs`, `docs/plans/2026-05-06-durable-final-object-repair-fencing.md`.

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

## Bounded Guarded Commit Repair Worker

The guarded durable commit path can now persist route-bound post-CAS repair context and run a bounded, admin-triggered repair worker for visible commits. This keeps broad durable core routing fail-closed while making workspace-head, audit, and idempotency completion gaps repairable without guessing from commit IDs alone.

What is built:

- `migrations/postgres/0004_guarded_commit_recovery_context.sql` adds nullable `context_json` to guarded post-CAS recovery claims so old no-context rows remain inspectable but unsupported for repair.
- `DurableCorePostCasRecoveryContext` carries redacted repair inputs: optional workspace id, expected workspace head, the bound audit event, and optional idempotency reservation parts plus explicit full-vs-partial response kind.
- `DurableCorePostCasRecoveryClaimStore::enqueue_with_context` preserves route-bound context in memory and Postgres while retaining the old no-context enqueue path.
- `AuditStore::contains_vcs_commit_event` gives the worker a narrow VCS-commit audit idempotence check before append.
- `IdempotencyStore::complete_or_match` lets recovery complete a pending reservation or accept an already-completed matching replay without overwriting a different response.
- `DurableCorePostCasRepairWorker` lists bounded due rows, claims with lease fencing, repairs one step, and records completion, retry/backoff, or terminal poison with redacted diagnostics.
- Workspace repair uses `WorkspaceMetadataStore::update_head_commit_if_current`; it treats already-advanced workspace heads as non-rollback success and enqueues the audit follow-up before completing its own claim.
- Audit repair checks for an existing VCS commit audit event before append and enqueues idempotency follow-up when the persisted context requires it.
- Idempotency repair waits for the audit prerequisite, reconstructs the persisted reservation, and completes either the full commit response or redacted partial response according to the persisted response kind.
- Guarded `POST /vcs/commit` now enqueues contextual recovery rows from the post-CAS envelope. Confirmed post-CAS idempotency failures enqueue partial replay recovery when the route returned `202 Accepted`, avoiding later `200 OK` replay drift.
- Admin-only `POST /vcs/recovery/run` claims at most the requested limit, defaults to a small run, caps at 100, ignores caller-supplied lease identity, and returns a redacted summary. Admin-only `GET /vcs/recovery` remains the bounded redacted status surface.

What is not built:

- No automatic background scheduler, wakeup loop, or daemon drain UX; operators must trigger bounded runs explicitly.
- No persisted pre-visibility recovery queue. Unconfirmed visibility states still fail closed without committed idempotency replay until ref visibility is proven.
- No broad durable filesystem/VCS/auth/session route cutover, durable non-commit VCS/FS serving, distributed lock service, final-object deletion, hosted TLS/KMS/secrets posture, or idempotency retention/quota model.

Verification on 2026-05-07 from the `v2/foundation` worktree: spec review found no blockers; code-quality review found two P2 issues, fixed by matching direct idempotency-failure recovery to the returned partial response and using the guarded capability store bundle consistently for route post-CAS side effects and repair. Re-review found no blockers. `cargo fmt --all -- --check` passed; `cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture` observed **14** passed; `cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture` observed **2** passed; `cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture` observed **20** passed; `cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_repair_worker --lib -- --nocapture` observed **19** passed; `cargo clippy --locked --all-targets -- -D warnings` passed; `cargo clippy --locked --all-targets --features postgres -- -D warnings` passed; `cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed with live Postgres portions skipped because `STRATUM_POSTGRES_TEST_URL` was unset; `STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh` skipped cleanly; `cargo audit --deny warnings` passed; and `git diff --check` passed. Full `cargo test --locked` passed, including **515** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, **9** server-startup tests, and doc tests. Final warm release perf after docs/status passed **37** tests in **7.84s real**, **7.40s user**, **0.21s sys**, with **118,620,160 bytes max RSS** and **98,533,928 bytes peak memory footprint**.

Grounding: `src/backend/core_transaction.rs`, `src/backend/postgres.rs`, `src/backend/postgres_migrations.rs`, `src/audit.rs`, `src/idempotency.rs`, `src/server/routes_vcs.rs`, `migrations/postgres/0004_guarded_commit_recovery_context.sql`, `tests/postgres/0001_durable_backend_foundation_smoke.sql`, `docs/plans/2026-05-07-bounded-guarded-commit-repair-worker.md`.

## Guarded Pre-Visibility Recovery Ledger

The guarded durable commit route now persists redacted, operator-visible diagnostics when commit metadata insert or ref CAS visibility cannot be proven. These rows are deliberately separate from post-CAS repair claims: they are status/inspection records for uncertainty before proven visibility, not replay or repair envelopes.

What is built:

- `migrations/postgres/0005_guarded_commit_pre_visibility_recovery.sql` adds `durable_pre_visibility_recovery_ledger`, keyed by `(repo_id, ref_name, commit_id, stage)` without a commit FK so metadata-insert uncertainty can be recorded even before commit visibility is provable.
- `DurableCorePreVisibilityRecoveryStore` has in-memory and Postgres-backed implementations with idempotent record/upsert, bounded list, and aggregate counts.
- Pre-visibility stages are explicit: `commit_metadata_insert` and `ref_visibility_cas`.
- Ledger rows record only redacted bounded metadata: repo/ref/commit identity, root tree id, optional parent commit id, expected ref version, object count, changed-path count, idempotency-reservation presence, first/last seen times, and occurrence count.
- Guarded `POST /vcs/commit` records a pre-visibility row before returning the redacted recovery-required error for metadata-insert recovery uncertainty or ref-visibility recovery uncertainty.
- If pre-visibility ledger persistence fails, the route fails closed with a redacted pre-visibility status-unavailable error and does not return a normal recovery-required response as though the row were durable.
- Admin-only `GET /vcs/recovery` now includes `pre_visibility`, `pre_visibility_counts`, `pre_visibility_count`, `pre_visibility_page_count`, and `pre_visibility_available` while preserving the existing post-CAS recovery fields.
- If the pre-visibility store is unavailable, `GET /vcs/recovery` still serves existing post-CAS recovery rows and marks only the pre-visibility section unavailable.

What is not built:

- No automatic background recovery scheduler, distributed lock service, durable auth/session/source cutover, durable filesystem/non-commit VCS serving, hosted TLS/KMS/secrets posture, or production idempotency retention model.

Verification on 2026-05-08 from the `v2/foundation` worktree: spec review found a P1 issue where ledger write failures were best-effort; code-quality/security review found the same issue, a pre-visibility status API compatibility issue, and a clippy blocker. Main-session fixes made ledger persistence fail closed, preserved post-CAS status when only pre-visibility status is unavailable, OR-ed idempotency-reservation presence on duplicate diagnostics, and replaced the too-many-argument status constructor. `cargo fmt --all -- --check` passed; `git diff --check` passed; `cargo test --locked backend::core_transaction::tests::durable_core_pre_visibility_recovery --lib -- --nocapture` observed **2** passed; `cargo test --locked server::routes_vcs::tests::guarded_durable_commit_metadata_recovery_failure_does_not_replay_partial --lib -- --nocapture` passed; `cargo test --locked server::routes_vcs::tests::guarded_durable_commit_ref_visibility_recovery_failure_records_pre_visibility_status --lib -- --nocapture` passed; `cargo test --locked server::routes_vcs::tests::guarded_durable_commit_metadata_recovery_status_persistence_failure_is_redacted --lib -- --nocapture` passed; `cargo test --locked server::routes_vcs::tests::vcs_recovery_status_preserves_post_cas_when_pre_visibility_store_fails --lib -- --nocapture` passed; `cargo clippy --locked --all-targets -- -D warnings` passed; `cargo clippy --locked --all-targets --features postgres -- -D warnings` passed; `cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed with live Postgres portions skipped because `STRATUM_POSTGRES_TEST_URL` was unset; `STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh` skipped cleanly; `cargo test --locked` passed, including **528** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, **9** server-startup tests, and doc tests; and `cargo audit --deny warnings` passed. Warm release perf after review fixes passed **37** tests in **8.17s real**, **7.53s user**, **0.26s sys**, with **118,734,848 bytes max RSS** and **98,648,592 bytes peak memory footprint**.

Grounding: `src/backend/core_transaction.rs`, `src/backend/postgres.rs`, `src/backend/postgres_migrations.rs`, `src/server/routes_vcs.rs`, `src/server/mod.rs`, `migrations/postgres/0005_guarded_commit_pre_visibility_recovery.sql`, `tests/postgres/0001_durable_backend_foundation_smoke.sql`, `docs/plans/2026-05-08-guarded-pre-visibility-recovery-ledger.md`.

## Durable Core Read/Source Cutover

The durable backend path now uses durable stores as the source of truth for committed filesystem/VCS reads exposed through the guarded durable capability. This narrows the remaining `.vfs/state.bin` dependence for serving committed content without claiming the durable mutable workspace runtime is complete.

What is built:

- `DurableCommittedFsReader` materializes committed trees from durable `RefStore`, `CommitStore`, and `ObjectStore` records, reusing the existing tree/blob encodings.
- Durable committed reads support file content plus stat, directory listing, stat metadata, `tree`, capped name-based `find`, and capped plain regex `grep`.
- Durable reads synthesize stable stat fields from committed object metadata, preserve permission-bit checks, enforce scoped-session path visibility during traversal, validate durable tree entry names, and redact store/codec errors to fixed durable-read messages.
- `DurableCoreRuntime` now routes `cat_with_stat_as`, `ls_as`, `stat_as`, `tree_as`, `find_as`, `grep_as`, `list_refs`, and `vcs_log_as` through durable stores. Durable log walks visible `main` ancestry rather than listing orphan commit metadata.
- The guarded durable capability makes HTTP filesystem read/list/stat/tree/search routes read from the durable committed `main` tree when `STRATUM_BACKEND=durable`, `STRATUM_DURABLE_COMMIT_ROUTE=1`, and the `postgres` build are active.
- At landing time, guarded durable `vcs/status`, `vcs/diff`, and `vcs/revert` failed closed with a stable durable mutable-workspace unsupported error rather than returning local or misleading data. The later Durable Status/Diff/Revert Parity slice routes those endpoints through durable stores.
- Scoped workspace bearer tokens are rejected for global VCS mutations, including guarded durable commit.
- Existing local runtime behavior remains unchanged when the guarded durable capability is absent.

What is not built:

- No durable filesystem mutations (`write`, `mkdir`, `delete`, `move`, `copy`, or metadata updates).
- No durable mutable workspace/session working tree; guarded durable commit still captures its target snapshot from local `StratumDb` until durable mutations exist.
- No broad `STRATUM_CORE_RUNTIME=durable-cloud` server startup; it remains fail-closed before auth or routes are available.
- No durable auth/session state, sparse remote FUSE, semantic search/indexing, web console, or execution runner.

Focused verification on 2026-05-08 from the `v2/foundation` worktree: TDD red tests first failed for the missing durable committed reader. Spec/correctness review found that guarded durable revert still consulted local changed-path state, durable log listed orphan commit metadata, tree entry names were trusted, and direct symlink grep missed a read check. Code-quality/security review found the scoped bearer VCS mutation gap plus glob/cap and lint hygiene issues. Main-session fixes made guarded durable revert fail before local revert-path resolution, made durable log walk visible `main` ancestry, rejected scoped workspace bearer sessions for global VCS mutations, validated durable tree entry names, capped durable find/grep traversal/results, escaped glob metacharacters once per find, and required symlink read permission for direct grep. After fixes, focused tests passed: `cargo test --locked backend::committed_read --lib -- --nocapture` observed **7** passed; `cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture` observed **28** passed; `cargo test --locked server::routes_fs::tests::guarded_durable_fs_routes_read_committed_tree_without_local_state --lib -- --nocapture` passed; `cargo test --locked server::routes_vcs::tests::guarded_durable_status_and_diff_routes_fail_closed_without_request_leaks --lib -- --nocapture` passed; `cargo test --locked scoped_workspace_bearer_cannot_run --lib -- --nocapture` observed **2** passed; and `cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture` observed **17** passed. Full verification passed: `cargo fmt --all -- --check`; `git diff --check`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo clippy --locked --all-targets --features postgres -- -D warnings`; `cargo test --locked --lib --tests`, including **545** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, and **9** server-startup tests; `cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed with live Postgres portions skipped because `STRATUM_POSTGRES_TEST_URL` was unset; `STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh` skipped cleanly; and `cargo audit --deny warnings` passed after scanning **408** crate dependencies with no denied findings. The final warm release perf pass passed **37** tests in **8.28s real**, **7.68s user**, **0.29s sys**, with **119,537,664 bytes max RSS** and **99,451,432 bytes peak memory footprint**.

Grounding: `src/backend/committed_read.rs`, `src/server/core.rs`, `src/server/routes_fs.rs`, `src/server/routes_vcs.rs`, `docs/http-api-guide.md`, `docs/plans/2026-05-08-durable-core-read-source-cutover.md`.

## Durable Status/Diff/Revert Parity

Guarded durable VCS inspection and rollback now use durable refs, commits, trees, objects, policy, audit, idempotency, and recovery state instead of falling back to local `.vfs/state.bin`.

What is built:

- Guarded durable `GET /vcs/status` compares the durable target ref against the mounted durable session ref when present, or committed `main` when not, using durable tree/object metadata and path records rather than blob-byte scans.
- Durable status output includes source identity: target ref, optional session ref, base/head commit ids, base/head root tree ids, and changed path count.
- Guarded durable `GET /vcs/diff` renders durable changes with exact-or-descendant path filtering, grouped unified text hunks for text changes including added/deleted text files, existing byte/cell caps, and stable summaries for binary, non-UTF-8, oversized, metadata-only, non-file, and type-changed paths.
- Guarded durable `POST /vcs/revert` restores `main` to a target durable commit root by creating a new durable commit whose parent is the observed current head, then CAS-updating the durable ref through the existing durable commit/recovery primitives. It does not apply text hunks.
- Durable revert is protected-ref and protected-path gated before idempotency replay or mutation, source-checked against the observed head/version, audited as `VcsRevert`, idempotent across retries after `main` advances to the revert commit, and recovery-aware for unresolved pre-visibility, post-CAS, or durable FS mutation claims on `main`.
- Recovery conflict detection for durable revert is a store-side unresolved check for pre-visibility, post-CAS, and durable FS mutation recovery rows on the target ref, including poisoned recovery rows and rows outside bounded status pages.
- Post-CAS idempotency repair now treats visible `VcsRevert` commit-resource audit events as valid visible commit audits and reconstructs the durable revert replay body from the persisted recovery context.
- Existing local status, diff, revert, ref, policy, audit, and idempotency behavior remains unchanged when the guarded durable capability is absent.

What is not built:

- No semantic diff, docx/PDF redline generation, merge queue, three-way conflict UI, or web console.
- No broad `STRATUM_CORE_RUNTIME=durable-cloud` startup, durable auth/session service, non-guarded durable VCS/FS route cutover, or durable FUSE mutation persistence.
- No final-object deletion worker, unreachable commit/object GC, production observability pipeline, distributed lock service beyond ref CAS/source-check fencing, or production idempotency retention/quota model.
- No dedicated recovery-conflict explanation/drain UX beyond the existing redacted recovery status and run surfaces.

Focused verification on 2026-05-09 from the `v2/foundation` worktree: spec/correctness review found durable diff missing source identity plus durable revert post-CAS idempotency repair gaps; a follow-up spec/correctness review found durable revert recovery conflicts were list-bounded and skipped poisoned rows; code-quality/security review found no remaining blockers after fixes. Focused tests passed for durable status routes, durable diff routes, durable revert routes (**10** guarded durable revert tests), local revert routes (**3** tests), durable diff helpers (**4** tests), audit visible-commit predicates (**2** tests), post-CAS durable revert idempotency repair replay (**1** test), and unbounded post-CAS unresolved recovery detection (**1** test). `cargo fmt --all -- --check` passed; `git diff --check` passed; `cargo clippy --locked --lib -- -D warnings` passed; `cargo clippy --locked --features postgres --lib -- -D warnings` passed; `cargo clippy --locked --all-targets -- -D warnings` passed; `cargo clippy --locked --all-targets --features postgres -- -D warnings` passed; `cargo test --locked --lib --tests` passed with **631** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, and **9** server-startup tests; `cargo test --locked --features postgres backend::postgres --lib -- --nocapture` passed **12** tests with live Postgres portions skipped because `STRATUM_POSTGRES_TEST_URL` was unset; `STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh` skipped cleanly; and `cargo audit --deny warnings` passed after scanning **408** crate dependencies. Warm release perf after the durable revert/recovery code diff passed **37** tests in **8.17s real**, **7.65s user**, **0.27s sys**, with **119,373,824 bytes max RSS** and **99,271,184 bytes peak memory footprint**.

Grounding: `src/backend/committed_read.rs`, `src/backend/core_transaction.rs`, `src/backend/durable_mutation.rs`, `src/backend/postgres.rs`, `src/audit.rs`, `src/server/core.rs`, `src/server/routes_vcs.rs`, `src/vcs/diff.rs`, `docs/http-api-guide.md`, `docs/plans/2026-05-09-durable-status-diff-revert-parity.md`.

## Durable Visibility And Mutation Safety Repair

The guarded durable backend path now closes the launch-blocking gaps between durable visibility, recovery intent persistence, session-ref ancestry, and write-only workspace-token mutation checks. The slice keeps broad `STRATUM_CORE_RUNTIME=durable-cloud` enablement out of scope.

What is built:

- Guarded durable VCS post-CAS completion pre-enqueues and route-claims post-visible recovery rows before workspace-head, audit, or idempotency side effects begin. Full success completes those rows; partial outcomes leave the relevant row backing off with a partial idempotency replay context.
- Guarded durable FS mutations pre-enqueue and route-claim audit/idempotency recovery rows before route audit/idempotency side effects begin. Recovery envelopes preserve the audit operation identity so idempotency repair does not append duplicate audit events under the idempotency key hash.
- In-memory and Postgres recovery stores support atomic enqueue-and-claim plus active claim context/envelope replacement, with stale owner/token/lease fencing. Postgres post-CAS and FS mutation candidate listing filters due work in SQL and orders workspace, audit, then idempotency.
- Guarded durable mutable FS routes now fail closed without a durable mount and session ref instead of accepting local-only `.vfs/state.bin` writes that durable reads cannot observe.
- Session-ref promotion no longer accepts same-root internal-message commits as proof. It requires bounded parent ancestry or the explicit prior-promotion equivalence already modeled by the guarded route.
- Durable write preflight can evaluate execute/write permissions and POSIX bits through an internal durable mutation lookup without requiring public read scope. Public read/list/stat/tree APIs remain read-scoped, while write-only workspace bearer tokens can write, mkdir, delete, copy, move, and update metadata within write scope.

What is not built:

- No bounded `/tree` output limits, bounded durable grep blob scanning, durable FS audit dedupe cleanup, committed-read commit-store identity validation, final object deletion / unreachable commit GC, sparse remote mount, ACL-aware full-text/vector index, web console, or execution runner.
- No broad `STRATUM_CORE_RUNTIME=durable-cloud` route enablement beyond the guarded durable capability path.

Verification on 2026-05-09 from the `v2/foundation` worktree: spec/correctness review found blockers in route-owned recovery leasing, Postgres recovery ordering, active FS idempotency envelope replacement, FS audit operation identity, and the historical post-CAS row assertion; code-quality/security review found blockers in route-owned recovery leasing, atomic Postgres conflict claiming, and SQL-filtered post-CAS repair candidate listing. Main-session fixes landed all findings, and final re-reviews reported no blockers. `cargo fmt --all -- --check` passed; `git diff --check` passed; focused tests passed for `backend::durable_mutation` (**11** tests), `backend::core_transaction::tests::durable_fs_mutation_recovery` (**8** tests), `server::routes_fs::tests::guarded_durable` (**14** tests), `server::routes_vcs::tests::guarded_durable_commit` (**22** tests), `server::core::tests::durable_core_runtime` (**29** tests), and `server::routes_vcs::tests::vcs_recovery` (**4** tests). `cargo clippy --locked --all-targets -- -D warnings` passed; `cargo clippy --locked --all-targets --features postgres -- -D warnings` passed; `cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed with live Postgres portions skipped because `STRATUM_POSTGRES_TEST_URL` was unset; `STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh` skipped cleanly; `cargo test --locked --lib --tests` passed, including **589** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, and **9** server-startup tests; and `cargo audit --deny warnings` passed after scanning **408** crate dependencies. Final warm release perf after docs/status passed **37** tests in **11.57s real**, **10.68s user**, **0.40s sys**, with **118,571,008 bytes max RSS** and **98,501,160 bytes peak memory footprint**.

Grounding: `docs/plans/2026-05-09-durable-visibility-mutation-safety-repair.md`, `src/backend/committed_read.rs`, `src/backend/core_transaction.rs`, `src/backend/durable_mutation.rs`, `src/backend/postgres.rs`, `src/server/core.rs`, `src/server/routes_fs.rs`, `src/server/routes_vcs.rs`.

## Recovery Observability And Operator Readiness

The guarded durable recovery surface is now operator-ready while preserving the existing durable runtime semantics. This is still a guarded route/status slice, not broad `STRATUM_CORE_RUNTIME=durable-cloud` enablement.

What is built:

- `docs/plans/2026-05-10-recovery-observability-operator-readiness.md` records the reference protocol, accepted/rejected SMFS and Mirage patterns, implementation tasks, and verification plan.
- Admin-only `GET /vcs/recovery` preserves the legacy bounded `recovery`, `pre_visibility`, and `fs_mutations` fields, and adds stable `health`, `phases`, and `blockers` sections.
- The recovery health block reports durable backend mode, guarded durable availability, per-store availability, and live scheduler status when the startup scheduler handle is attached.
- Recovery scheduler status now records started time, last tick time, redacted last outcome/error markers, and per-phase attempted/completed/backing-off/poisoned/skipped counts without changing tick order, limits, lease owners, or worker semantics.
- Recovery rows now include age/readiness metadata such as `age_millis`, created/updated timestamps where available, stale-active classification, due/retryable booleans, stuck tier, and next retry time.
- Post-CAS and durable FS mutation status types now carry read-only created/updated timestamps from in-memory stores and Postgres `created_at`/`updated_at` columns so pending and active age reporting is meaningful.
- `ObjectCleanupClaimStore` now exposes bounded redacted `list` and `counts` read APIs with in-memory and Postgres implementations, making cleanup-claim age/counts visible without exposing lease tokens or raw failure text.
- `GET /vcs/recovery.phases.object_cleanup` surfaces cleanup-claim counts and bounded rows, including CAS-lost durable mutation cleanup candidates, as visibility only.
- `POST /vcs/recovery/run` now returns a redacted correlation ID in the body and `X-Stratum-Recovery-Correlation-Id`, the caller `requested_limit`, normalized phase summaries, aggregate attempted/completed/backing-off/poisoned/skipped/remaining counts, and `converged: false` when bounded work remains.
- Per-ref blockers identify unresolved, stale-active, or poisoned work blocking `main`; workspace blockers summarize durable FS mutation recovery work by workspace scope and target ref.

What is not built:

- No broad durable cloud runtime enablement, durable auth/session routing, non-guarded route cutover, distributed lock service, web console, execution runner, FUSE/sparse mount, or event-bus audit pipeline.
- No final-object deletion, unreachable commit GC, or cleanup worker. Cleanup claims are observable risk markers only.
- No unbounded recovery status output. Status rows remain bounded and raw store failures remain redacted.
- No hosted shutdown/control-plane commands; SMFS/Mirage references were used for operator shape only.

Verification on 2026-05-10 from the `v2/foundation` worktree: local spec/security review found cleanup status rows exposed canonical object keys on the HTTP surface and that expired failed cleanup claims were not visibly retryable; the route now omits `object_key`, exposes repo/object kind/object ID instead, and marks stale failed cleanup rows as due/retryable. Local quality review found clippy shape issues in status constructors and bool assertions; fixed with status input structs and idiomatic assertions. Focused tests passed for `server::routes_vcs::tests::vcs_recovery` (**9** tests), `server::routes_fs::tests::guarded_durable` (**17** tests), `server::tests::durable_recovery_scheduler` (**3** tests), `backend::object_cleanup` (**8** tests), `backend::core_transaction::tests::durable_core_commit_post_cas_recovery` (**16** tests), `backend::core_transaction::tests::durable_fs_mutation_recovery` (**8** tests), `backend::durable_mutation` (**11** tests), and `server::routes_vcs::tests::guarded_durable_commit` (**22** tests). Full gates passed: `cargo fmt --all -- --check`; `git diff --check`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo clippy --locked --all-targets --features postgres -- -D warnings`; `cargo test --locked --lib --tests`, including **639** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, and **9** server-startup tests; `cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed with live Postgres portions skipped because `STRATUM_POSTGRES_TEST_URL` was unset; `STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh` skipped cleanly; and `cargo audit --deny warnings` passed after scanning **408** crate dependencies. Final warm release perf passed **37** tests in **31.52s real**, **110.31s user**, **3.75s sys**, with **1,816,739,840 bytes max RSS** and **99,107,392 bytes peak memory footprint**.

Grounding: `docs/plans/2026-05-10-recovery-observability-operator-readiness.md`, `src/backend/core_transaction.rs`, `src/backend/object_cleanup.rs`, `src/backend/postgres.rs`, `src/server/mod.rs`, `src/server/routes_vcs.rs`, `docs/http-api-guide.md`.

## Broad Durable Runtime/Auth/Policy Cutover Planning

This docs-only planning slice defines the safe path from guarded durable capability routes to broad `STRATUM_CORE_RUNTIME=durable-cloud`. It does not enable broad durable runtime or change Rust behavior.

What is built:

- `docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md` records the current runtime/auth/policy/storage boundary, startup gates, durable auth/session model, tenant/repo routing requirements, storage/operations blockers, rollback points, and implementation sequencing.
- The plan keeps broad durable startup fail-closed until durable auth/session routing, policy below route layer, explicit tenant/repo routing, hosted storage posture, idempotency retention/quota, recovery readiness, and non-HTTP bypass handling are solved.
- Future implementation slices are broken down for durable auth/session routing, policy enforcement below the route layer, tenant/repo routing, broad durable core runtime incremental enablement, final-object deletion/GC, idempotency retention/quota and secret-safe replay, hosted storage hardening, and non-HTTP caller parity.
- Each future slice records what stays fail-closed, acceptance criteria, rollback boundaries, and verification commands.

What is not built:

- No broad `STRATUM_CORE_RUNTIME=durable-cloud` enablement.
- That planning slice itself did not build durable auth/session implementation, distributed locks, final-object deletion/GC, idempotency retention/quota, hosted TLS/KMS/secrets posture, event-bus audit pipeline, web console, FUSE sparse mount, semantic search, or execution runner. The later Durable Auth/Session Routing Foundation section records the auth/session foundation that has since landed.

Grounding: `docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md`, `src/backend/runtime.rs`, `src/backend/mod.rs`, `src/backend/postgres.rs`, `src/backend/core_transaction.rs`, `src/backend/durable_mutation.rs`, `src/backend/committed_read.rs`, `src/server/mod.rs`, `src/server/core.rs`, `src/server/middleware.rs`, `src/server/policy.rs`, `src/server/routes_auth.rs`, `src/server/routes_workspace.rs`, `src/server/routes_fs.rs`, `src/server/routes_vcs.rs`, `src/server/routes_review.rs`, `src/bin/stratum_mcp.rs`, `src/bin/stratumctl.rs`, `src/fuse_mount.rs`, `src/auth/session.rs`, `src/workspace/mod.rs`, `src/review.rs`, `migrations/postgres/*.sql`, `tests/server_startup.rs`.

## Durable Auth/Session Routing Foundation

This slice moves hosted durable auth/session validation through runtime/store seams without enabling broad `STRATUM_CORE_RUNTIME=durable-cloud`.

What is built:

- `migrations/postgres/0009_durable_auth_session_foundation.sql` adds durable principals plus workspace-token lifecycle fields for issued/updated/expiry/revocation, principal identity, repo scope, and finite timestamp checks. Existing token lifecycle timestamps backfill from `created_at`, and the Postgres smoke harness covers a pre-0009 token.
- Workspace token records now carry repo identity, principal identity, lifecycle timestamps, expiry, revocation, token version, and hash-only token secrets. Local metadata migration preserves existing behavior while normalizing lifecycle defaults.
- Postgres workspace-token validation is store-backed for hosted durable mode, enforces token hash, workspace/repo match, prefix match, expiry, revocation, active durable principal state, and maps timestamp decode failures to `CorruptStore`.
- `/auth/login` routes through the `CoreDb` seam instead of direct `state.db` access, and workspace bearer auth validates through the workspace store before session creation.
- Mounted workspace sessions now carry workspace id, repo id, base ref, session ref, principal uid, token id/version, and read/write scopes. Durable principal sessions no longer require local user metadata, while local/global-token compatibility remains local-only.
- Workspace token issuance rejects `Idempotency-Key` for secret-returning responses, authenticates backing agents through the core seam, and never persists or audits raw token secrets.
- Admin `POST /workspaces/{workspace_id}/tokens/{token_id}/revoke` revokes tokens without idempotency replay, returns only bounded token metadata, and audits token identity without raw token or hash material.
- Startup keeps broad durable core fail-closed with an explicit durable auth/session readiness message before opening local `.vfs` stores or parsing durable backend secrets.
- Staff-review hardening now rejects malformed workspace headers with static errors, validates mounted workspace identity before creating sessions, redacts raw token secrets from `IssuedWorkspaceToken` debug output, projects mounted audit workspace roots to `/`, omits backing paths from workspace create/token audit details, and keeps durable-core fail-closed ahead of invalid backend env parsing.

What is not built:

- No broad durable runtime enablement.
- No external OIDC/SAML, refresh-token flows, full tenant/org membership model, MCP/FUSE durable auth cutover, or secret-bearing idempotent replay.
- Durable auth/session routing is wired for HTTP server paths only; non-HTTP callers keep their existing local auth/session boundaries.

Verification on 2026-05-12 from the `v2/foundation` worktree: focused gates passed for `cargo test --locked auth --lib -- --nocapture` (**40** tests), `cargo test --locked workspace::tests --lib -- --nocapture` (**51** tests), `cargo test --locked server::routes_workspace::tests --lib -- --nocapture` (**21** tests), and `cargo test --locked --test server_startup durable_env -- --nocapture` (**1** test). Full gates passed: `cargo fmt --all -- --check`; `git diff --check`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo clippy --locked --all-targets --features postgres -- -D warnings`; `cargo test --locked --lib --tests`, including **664** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, and **11** server-startup tests; `cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed with live Postgres portions skipped because `STRATUM_POSTGRES_TEST_URL` was unset; `STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh` skipped cleanly; and `cargo audit --deny warnings` passed after scanning **408** crate dependencies. Final warm release perf used `sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture` and passed **37** tests in **47.77s real**, **149.17s user**, **6.09s sys**, with **1,455,865,856 bytes max RSS** and **100,762,152 bytes peak memory footprint**.

Grounding: `docs/plans/2026-05-10-durable-auth-session-routing-foundation.md`, `src/auth/session.rs`, `src/workspace/mod.rs`, `src/backend/postgres.rs`, `src/backend/runtime.rs`, `src/server/middleware.rs`, `src/server/routes_auth.rs`, `src/server/routes_workspace.rs`, `migrations/postgres/0009_durable_auth_session_foundation.sql`, `tests/postgres/0001_durable_backend_foundation_smoke.sql`, `tests/server_startup.rs`.

## Durable Mutations And Recovery Ops

The guarded durable backend path now supports mounted-session filesystem mutations against durable session refs, plus bounded recovery scheduling for visible route side-effect gaps. This is still a guarded capability path, not broad `STRATUM_CORE_RUNTIME=durable-cloud` enablement.

What is built:

- `src/backend/durable_mutation.rs` adds a durable mutable tree engine over the existing `RefStore`, `CommitStore`, and `ObjectStore` contracts. It materializes a session ref from the workspace base ref with source-checked ref creation, applies one mutation, writes durable blob/tree objects, inserts an internal durable mutation commit, and CAS-updates the session ref.
- Guarded durable mounted sessions can now route `write_file`, `mkdir_p`, `delete`, `copy`, `move`, and metadata updates through durable session refs. Local runtime behavior remains unchanged when the guarded durable capability is absent.
- Durable FS reads after mounted-session mutations use the durable session ref, survive a fresh local `StratumDb`, and keep committed `main` reads available when no session ref exists.
- Guarded durable `POST /vcs/commit` can promote the durable session-ref tree into the base ref without reading the local `.vfs/state.bin` snapshot for mounted durable workspaces. It rejects stale main/base races and accepts a session ref after a prior promotion when the session tree already contains the visible `main` root plus new internal mutation commits.
- Route idempotency and audit completion integrate with a new durable FS mutation recovery ledger. Visible partials enqueue bounded recovery work instead of returning misleading success.
- `migrations/postgres/0007_durable_fs_mutation_recovery.sql` adds the durable FS mutation recovery ledger, and the Postgres adapter implements claim, completion, backoff, poison, bounded listing, due-candidate listing, and aggregate counts with lease owner/token fencing.
- `migrations/postgres/0008_durable_mutation_cleanup_claim_kind.sql` extends the cleanup-claim kind constraint without changing the checksum of the original foundation migration.
- Automatic startup recovery scheduling starts when guarded durable stores are configured. The bounded scheduler drains pre-visibility, post-CAS, and durable FS mutation recovery queues and avoids duplicate loops for the same store set.
- Admin `GET /vcs/recovery` and `POST /vcs/recovery/run` now include durable FS mutation recovery rows/counts and summaries alongside guarded commit recovery sections.
- CAS-lost durable mutation object writes claim object cleanup candidates through the shared cleanup-claim ledger when the production cleanup claim store is available, making the orphan risk durable and inspectable for a future cleanup/GC contract.

What is not built:

- No broad `STRATUM_CORE_RUNTIME=durable-cloud` route enablement.
- No durable mutable workspace support outside guarded mounted-session routes.
- Durable status/diff/revert landed later and remains guarded rather than broad non-guarded durable runtime behavior.
- No distributed lock service beyond durable ref CAS/source-check fencing.
- No automatic final-object deletion worker or unreachable commit GC for CAS-lost durable mutation objects; cleanup claims make those candidates visible, but automatic deletion/drain remains future work.
- No durable FUSE mutation persistence, sparse remote mount, semantic index, web console, execution runner, or hosted observability beyond the current recovery status route.
- No pre-visible mutation-intent ledger before session-ref CAS; the later safety repair covers post-visible side-effect completion once the route reaches the post-visible phase.

Verification on 2026-05-09 from the `v2/foundation` worktree: implementation review found directory-copy parity, scheduler duplicate-handle lifecycle, and Debug redaction issues; main-session fixes made durable directory copy fail closed, made duplicate routers share the same recovery scheduler handle, and stopped Debug output from printing custom attr keys/removal names. Spec review found no acceptance-blocking issues after fixes; the CAS-lost object cleanup claim drain remains documented as a residual because final-object deletion and unreachable commit GC are out of scope. `cargo fmt --all -- --check` passed; `git diff --check` passed; focused tests passed for `backend::durable_mutation` (**11** tests), `backend::core_transaction::tests::durable_fs_mutation_recovery` (**5** tests), `server::routes_fs::tests::guarded_durable` (**10** tests), `server::routes_vcs::tests::guarded_durable_commit` (**20** tests), `server::routes_vcs::tests::vcs_recovery` (**4** tests), and `server::tests::durable_recovery_scheduler` (**2** tests). `cargo clippy --locked --all-targets -- -D warnings` passed; `cargo clippy --locked --all-targets --features postgres -- -D warnings` passed; `cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed with live Postgres portions skipped because `STRATUM_POSTGRES_TEST_URL` was unset; `STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh` skipped cleanly; `cargo test --locked --lib --tests` passed, including **578** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, and **9** server-startup tests; and `cargo audit --deny warnings` passed after scanning **408** crate dependencies. Warm release perf passed **37** tests in **8.11s real**, **7.57s user**, **0.22s sys**, with **118,620,160 bytes max RSS** and **98,550,312 bytes peak memory footprint**.

Grounding: `src/backend/durable_mutation.rs`, `src/backend/core_transaction.rs`, `src/backend/postgres.rs`, `src/server/core.rs`, `src/server/routes_fs.rs`, `src/server/routes_vcs.rs`, `src/server/mod.rs`, `migrations/postgres/0007_durable_fs_mutation_recovery.sql`, `migrations/postgres/0008_durable_mutation_cleanup_claim_kind.sql`, `docs/plans/2026-05-09-durable-mutations-and-recovery-ops.md`.

## Guarded Pre-Visibility Recovery Run Control

The guarded durable recovery route can now converge bounded pre-visibility rows once commit/ref visibility is provable, and it can safely release the original idempotency reservation when a commit did not become visible. This keeps the operator-triggered model and avoids fabricating audit or replay context from commit IDs alone.

What is built:

- `migrations/postgres/0006_pre_visibility_recovery_run_control.sql` extends `durable_pre_visibility_recovery_ledger` with route-bound `context_json`, lease owner/token/expiry, attempts, retry/backoff, redacted error, poison, terminal, and due-work indexing.
- `DurableCorePreVisibilityRecoveryStore` now supports claim, resolve, failure/backoff, poison, due-candidate listing, and expanded counts for pending, active, backing-off, resolved, and poisoned rows. In-memory and Postgres implementations fence transitions by lease owner/token/expiry.
- Guarded `POST /vcs/commit` attaches route-bound post-CAS recovery context to new pre-visibility rows: optional workspace id, expected workspace head, bound VCS commit audit event, and optional idempotency reservation context for full commit replay.
- `DurableCorePreVisibilityRecoveryRun` processes due rows before post-CAS repair. It verifies commit metadata against the recorded root, parent, and changed-path count; proves visibility when `main` points at the commit or reaches it through a bounded parent walk; safely applies ref CAS for metadata-insert rows when `main` is still at the recorded parent/version; enqueues contextual post-CAS repair after proof; and aborts the persisted idempotency reservation when the commit is not visible.
- Admin-only `POST /vcs/recovery/run` returns the existing post-CAS summary plus a nested redacted `pre_visibility` summary, and shares the caller limit across pre-visibility and post-CAS work.
- Admin-only `GET /vcs/recovery` includes pre-visibility run-control fields such as attempts, lease expiry, retry time, terminal time, redacted diagnosis marker, and `has_recovery_context` without exposing commit messages, idempotency keys, reservation tokens, or raw store errors.

What is not built:

- No automatic scheduler, wakeup loop, or daemon drain UX; operators still trigger bounded runs explicitly.
- No broad durable filesystem/VCS/auth/session route cutover, durable non-commit VCS/FS serving, distributed lock service, hosted TLS/KMS/secrets posture, or production idempotency retention/quota model.

Verification on 2026-05-08 from the `v2/foundation` worktree: `cargo fmt --all -- --check` passed; `git diff --check` passed; `cargo test --locked backend::core_transaction::tests::durable_core_pre_visibility_recovery --lib -- --nocapture` observed **4** passed; `cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture` observed **3** passed; `cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture` observed **17** passed; `cargo test --locked server::routes_vcs::tests::recovery_run_resolves_visible_pre_visibility_row_and_enqueues_post_cas --lib -- --nocapture` passed with the bounded parent-walk scenario; `cargo check --locked --features postgres` passed; `cargo clippy --locked --all-targets -- -D warnings` passed; `cargo clippy --locked --all-targets --features postgres -- -D warnings` passed; `cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed with live Postgres portions skipped because `STRATUM_POSTGRES_TEST_URL` was unset; `STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh` skipped cleanly; `cargo test --locked` passed, including **531** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, **9** server-startup tests, and doc tests; and `cargo audit --deny warnings` passed after scanning **408** crate dependencies with no denied findings. Warm release perf passed **37** tests in **11.37s real**, **10.64s user**, **0.34s sys**, with **118,341,632 bytes max RSS** and **98,255,424 bytes peak memory footprint**.

Grounding: `src/backend/core_transaction.rs`, `src/backend/postgres.rs`, `src/backend/postgres_migrations.rs`, `src/server/routes_vcs.rs`, `migrations/postgres/0006_pre_visibility_recovery_run_control.sql`, `tests/postgres/0001_durable_backend_foundation_smoke.sql`, `docs/plans/2026-05-08-pre-visibility-recovery-run-control.md`.

## Guarded Durable VCS Metadata Route Consistency

The guarded durable commit route now exposes its matching VCS metadata surface for log and refs. This removes the split-brain where `POST /vcs/commit` wrote durable commit/ref metadata but `GET /vcs/log`, `GET /vcs/refs`, `POST /vcs/refs`, and `PATCH /vcs/refs/{name}` still observed or mutated only the local `StratumDb` state.

What is built:

- `STRATUM_DURABLE_COMMIT_ROUTE=1` now routes admin `GET /vcs/log`, `GET /vcs/refs`, `POST /vcs/refs`, and `PATCH /vcs/refs/{name}` through the guarded durable capability when present.
- Guarded `GET /vcs/log` reads durable `CommitStore` metadata, maps durable records into the existing `CommitObject` HTTP response shape, uses the first parent for merge-shaped metadata, and keeps scoped workspace bearer sessions out through the stricter admin gate.
- Guarded `GET /vcs/refs` lists durable `RefStore` metadata, so durable commits and durable ref CAS updates are visible through the route that operators already use.
- Guarded ref create/update reuse the durable `RefStore` compare-and-swap paths while preserving existing route-level admin auth, protected-ref blocking, idempotency reservation/replay, and audit append behavior.
- Durable metadata route store failures are redacted to a fixed unavailable-store error; invalid inputs, duplicate refs, missing target commits, and CAS conflicts retain stable client-facing semantics without leaking raw backend details.
- The internal broad `DurableCoreRuntime` route methods remain fail-closed; only the explicit guarded route capability is live.

What is not built:

- No durable auth/session path, broad non-guarded durable filesystem/search/tree/VCS serving, or broad `STRATUM_CORE_RUNTIME=durable-cloud` route cutover. Guarded durable status/diff/revert landed in the later parity slice.
- No durable source snapshot cutover beyond guarded commit using the local `StratumDb` filesystem source.
- No automatic recovery scheduling, distributed lock service, hosted TLS/KMS/secrets posture, or production idempotency retention model.

Verification on 2026-05-08 from the `v2/foundation` worktree: TDD red tests first failed because guarded durable log/refs observed local metadata and durable ref create returned a local missing-commit error; review-fix red tests then failed on multi-parent log projection, raw durable metadata store errors, and scoped root bearer access to guarded durable log. Spec review found the multi-parent log projection and stale HTTP docs; code-quality/security review found the redaction and scoped-admin issues. All findings were fixed locally. `cargo fmt --all -- --check` passed; `git diff --check` passed; `cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture` observed **17** passed; `cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture` observed **25** passed; `cargo test --locked server::routes_vcs::tests::admin_can_create_list_and_update_refs_over_http --lib -- --nocapture` passed; `cargo test --locked server::routes_vcs::tests::non_admin_and_workspace_bearer_cannot_manage_refs --lib -- --nocapture` passed; `cargo clippy --locked --all-targets -- -D warnings` passed; `cargo clippy --locked --all-targets --features postgres -- -D warnings` passed; `cargo test --locked --lib --tests` passed, including **523** lib tests, **8** `stratum_mcp` tests, **1** `stratumctl` test, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, and **9** server-startup tests; `cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed with live Postgres portions skipped because `STRATUM_POSTGRES_TEST_URL` was unset; `STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh` skipped cleanly; and `cargo audit --deny warnings` passed. Warm release perf after the code-quality fixes passed **37** tests in **11.35s real**, **10.74s user**, **0.34s sys**, with **119,963,648 bytes max RSS** and **99,861,032 bytes peak memory footprint**.

Grounding: `src/server/core.rs`, `src/server/routes_vcs.rs`, `docs/http-api-guide.md`, `docs/plans/2026-05-08-guarded-durable-vcs-metadata-route-consistency.md`.

## Persisted Guarded Commit Recovery Claims

The guarded durable commit route now persists post-CAS recovery work for visible commits before it returns or replays a normal redacted partial response. This keeps the broad durable runtime fail-closed while making visible post-CAS completion gaps inspectable and safely claimable across process restarts.

What is built:

- `migrations/postgres/0003_guarded_commit_recovery_claims.sql` adds `durable_post_cas_recovery_claims`, keyed by `(repo_id, ref_name, commit_id, step)` for immutable commit/ref/step identity.
- Recovery claim state is explicit: `pending`, `active`, `backing_off`, `completed`, and `poisoned`.
- The in-memory and Postgres recovery stores support enqueue, claim, complete, failure/backoff, poison, bounded list, and aggregate counts.
- Worker claims are fenced by lease owner, lease token, and lease expiry. Stale owner/token/expired claims cannot complete, fail, or poison a retry.
- `claim` only transitions existing pending, expired-active, or due-backing-off rows; it does not invent un-enqueued work.
- Diagnostics stored for failure/backoff and poison are redacted to a fixed marker.
- Guarded durable `POST /vcs/commit` enqueues the failed post-CAS step after ref visibility is confirmed and before completing a normal partial idempotency replay.
- If partial idempotency replay completion fails after workspace/audit recovery is enqueued, the route also enqueues `idempotency_completion` recovery.
- If required recovery enqueue fails, the route fails closed with the existing redacted recovery-required error and does not leave a normal `202` replay behind.
- Admin-only `GET /vcs/recovery` returns bounded redacted rows plus aggregate state counts for the guarded durable commit capability.
- The Postgres adapter contract test harness now applies migration 0003 before live backend contracts when `STRATUM_POSTGRES_TEST_URL` is configured.

What is not built:

- No persisted repair worker loop, wakeup scheduler, or daemon drain/status UX yet.
- No automatic post-restart reconstruction of post-CAS envelopes for workspace/audit/idempotency repair.
- Pre-visibility uncertain guarded-commit responses still fail closed without committed idempotency replay; they are not yet persisted as durable recovery queue rows.
- No broad durable filesystem/VCS/auth/session route cutover and no durable non-commit VCS/FS serving.

Focused verification on 2026-05-07 from the `v2/foundation` worktree: subagent spec review found two P2 issues, fixed by redacting `/vcs/recovery` store errors and applying migration 0003 in the live Postgres test harness; subagent code-quality review found no actionable findings. `cargo fmt --all -- --check` passed; `cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas --lib -- --nocapture` observed **24** passed; `cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_recovery --lib -- --nocapture` observed **14** passed; `cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture` observed **11** passed; `cargo test --locked --features postgres backend::postgres --lib -- --nocapture` observed **12** passed with live Postgres portions skipped because `STRATUM_POSTGRES_TEST_URL` was unset; `cargo clippy --locked --all-targets -- -D warnings` passed; `cargo clippy --locked --all-targets --features postgres -- -D warnings` passed; `cargo test --locked --lib --tests` passed, including **486** lib tests, **142** integration tests, **37** debug perf tests, **1** debug perf-comparison test, **72** permission tests, and **9** server-startup tests; `STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh` skipped cleanly; `cargo audit --deny warnings` passed; `git diff --check` passed. Final warm release perf after review fixes passed **37** tests in **12.12s real**, **10.99s user**, **0.47s sys**, with **119,914,496 bytes max RSS** and **99,811,856 bytes peak memory footprint**.

Grounding: `src/backend/core_transaction.rs`, `src/backend/postgres.rs`, `src/backend/postgres_migrations.rs`, `src/server/routes_vcs.rs`, `src/server/mod.rs`, `migrations/postgres/0003_guarded_commit_recovery_claims.sql`, `tests/postgres/0001_durable_backend_foundation_smoke.sql`, `docs/plans/2026-05-07-persisted-guarded-commit-recovery-claims.md`.

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

- Default local runtime durability is still file-backed metadata/state. Durable server mode cuts over workspace/idempotency/audit/review control-plane stores to Postgres, hosted HTTP auth/session seams can validate durable principals and workspace bearer tokens, and the guarded durable capability can serve committed FS/VCS reads, mounted-session FS mutations, and durable status/diff/revert from durable stores, but broad core runtime startup and non-guarded durable FS/VCS route serving are not cut over.
- Scoped ACL enforcement has broad tests now, and mutating HTTP routes emit bounded policy allow/deny audit events, but the long-term policy service, action capabilities, and tenant isolation model are not built.
- Refs/status/diff and protected-change semantics are foundation-level; approval records, review comments, approval dismissal, reviewer assignments, and approval counts exist, but merge queues, distributed policy decisions, and protected-change enforcement outside HTTP routes are not complete.
- Run records are useful audit artifacts, but they do not prove safe execution because no runner or sandbox exists yet.
- Run-record creation is not fully atomic across all files.
- Search remains a filesystem/search surface, not the full-text plus semantic derived index described in the v2 plan.
- Audit events are still a route-level scaffold; durable server mode can persist mutating-route, policy-decision, and review-decision events in Postgres, but there is no production audit pipeline for auth/read events or durable event-bus ingestion.
- Workspace-token issuance intentionally rejects idempotency keys until secret-aware replay storage exists.
- File metadata is available through stat/HTTP/VCS/local persistence and Stratum metadata-backed POSIX/FUSE xattrs, but automatic MIME inference, arbitrary binary/native xattrs, durable FUSE mutation persistence, and remote sparse FUSE cache correctness are not built.
- Cloud deployment scaffolding, backend contracts, a byte-backed object adapter scaffold, a guarded S3/R2-compatible object-store integration gate, a cleanup-claim/metadata-repair foundation with live Postgres-backed repair conformance coverage, a Postgres migration smoke harness, a feature-gated Postgres migration runner, durable startup migration preflight, optional Postgres metadata adapters, a fail-closed backend runtime selector, durable Postgres control-plane runtime wiring, durable auth/session routing foundations, a durable core transaction semantics contract, durable committed FS read primitives, guarded committed FS/search/tree read routing, guarded live durable `POST /vcs/commit`, guarded durable VCS log/ref metadata routes, guarded durable status/diff/revert, persisted post-CAS recovery claims, a bounded operator-triggered guarded commit repair worker, persisted guarded pre-visibility recovery diagnostics, bounded pre-visibility run control, guarded durable mounted-session mutations, automatic bounded recovery scheduling, and operator-ready recovery observability exist, but production multi-tenant backend, broad core runtime Postgres/R2 startup, final-object deletion fencing, idempotency retention/quota controls, KMS/secrets posture, and private-beta hardening remain future work.

## Not Built Yet

From the CTO plan and current repo docs, these are the major missing v2 pieces:

- Durable cloud runtime: implementation of the remaining broad-runtime planning slices, policy-below-route enforcement, tenant/repo routing, MCP/FUSE durable auth parity, durable mutable workspace writes outside guarded mounted-session routes, non-guarded durable VCS/FS route serving, broad `STRATUM_CORE_RUNTIME=durable-cloud` startup, final-object deletion fencing, distributed locking, and production cross-store transaction execution beyond the guarded route path and its recovery ledgers.
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

1. Policy enforcement below the route layer so HTTP, MCP, CLI, FUSE, and embedded durable callers cannot bypass protected ref/path decisions.
2. Tenant/repo routing foundation to replace hosted `RepoId::local()` assumptions while preserving explicit local compatibility.
3. Broad durable core runtime incremental enablement only after auth/session, policy, repo routing, storage, idempotency, and recovery gates are ready.
4. Final-object deletion/GC design for CAS-lost durable mutation objects and unreachable durable commit/object cleanup.
5. Hosted storage, TLS, KMS, and secrets posture hardening before any production broad durable runtime rollout.

Deferred until guarded durable commit repair execution and pre-visibility run control are fully operational:

- Secret-aware workspace-token idempotency, which needs explicit replay storage and KMS/secrets posture first.
- Broader auth/read/policy audit coverage and the future Postgres/event-bus audit pipeline.
- Execution Phase 2 runner work.
- POSIX/FUSE sparse remote cache and native xattr hardening.
- Reviewer groups/code owners, threaded/resolved comments, and review UI.

SMFS extraction guidance: do not copy SMFS's latest-wins push queue or SQLite inode/chunk cache into the durable commit path. Use SMFS only as pattern input: persisted claim/finalize/backoff queue semantics, bounded worker/wakeup structure for object convergence or repair workers, dirty/source freshness as parent ref-version freshness rather than timestamps, atomic-save rename edge cases for write-set tests, and daemon status/drain UX for later repair/commit observability.

## Branch And Release Status

- Branch: `v2/foundation`.
- Remote tracking branch: `origin/v2/foundation`.
- Before the backend runtime selection foundation slice, `main` and `v2/foundation` were synced and pushed at merge commit `866794e` after the R2 object-store integration gate slice.
- `v2/foundation` now contains the VCS/session semantics, audit-event scaffolding, HTTP idempotency coverage, CI foundation, file metadata foundation, protected-change foundation, POSIX/FUSE metadata xattr, review feedback, reviewer assignment, approval workflow hardening, route policy decision/audit parity, durable review merge parity, durable backend foundation, backend adapter scaffolding, Postgres migration harness, Postgres metadata adapter, R2 object-store integration gate, backend runtime selection foundation, durable cleanup claims/orphan repair foundation, production migration runner, Postgres idempotency/audit/workspace/review adapters, durable startup migration wiring, durable runtime control-plane cutover, durable core runtime boundary, route-facing core seam, durable CoreDb implementation path, durable final-object repair/fencing conformance, durable update-ref executor path, durable create-ref executor path, durable commit transaction executor skeleton, durable commit transaction metadata preflight, durable commit object/tree write-plan preflight, durable planned object convergence executor, durable commit metadata insert executor, durable commit ref CAS visibility, durable commit post-CAS completion/recovery envelope, guarded live durable `POST /vcs/commit` routing, persisted guarded-commit post-CAS recovery claims/status, bounded guarded commit repair worker, guarded durable VCS metadata route consistency, guarded pre-visibility recovery ledger/run-control slices, guarded durable committed-read/source cutover, guarded durable mounted-session FS mutations, durable FS mutation recovery, durable FS audit identity/dedupe, automatic bounded recovery scheduling, fail-closed guarded durable FS mutation routing, write-scope durable preflight, stricter session-ref ancestry proof, route-owned post-visible recovery completion claims, recovery observability/operator readiness, and durable auth/session routing foundations.
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
