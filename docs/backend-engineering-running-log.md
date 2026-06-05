# Stratum Backend Engineering Running Log

Last updated: 2026-06-04

This log tracks backend review findings, scoped fixes, and borrowable ideas from sibling projects. Entries should stay factual, cite evidence, and distinguish fixed work from follow-up work.

## Bugs / Correctness Risks

- Fixed 2026-06-04: durable recovery audit append now uses `AuditStore::append_once` with explicit visible-commit or durable-FS-mutation identities instead of separate contains-then-append checks. In-memory and local stores perform the check and append under their write locks; Postgres performs the check and insert in the same advisory-locked transaction without a schema migration. Direct post-CAS audit completion uses the same visible-commit identity.
- Fixed 2026-06-04: post-CAS workspace-head repair now records explicit `repaired`, `already_desired`, and `superseded` outcomes in worker summaries, scheduler health, and manual recovery JSON. The superseded path completes recovery without overwriting the newer workspace head, making the conservative behavior visible instead of collapsing it into generic completion.
- Fixed 2026-06-04: workspace-create audit failure responses leaked backend audit error details to clients. Evidence: `src/server/routes_workspace.rs` returned `format!("audit append failed after mutation: {error}")`; regression `create_workspace_audit_failure_response_is_redacted` now asserts the public body excludes the underlying I/O message.
- Fixed 2026-06-04: run-create audit failure responses also leaked backend audit error details after the run record had been written. Evidence: `src/server/routes_runs.rs` returned `format!("mutation committed but audit recording failed: {error}")`; the run-route audit failure regression now requires the stable public message and rejects the backend failure text.

## Architecture Debt

- Several backend files are very large and carry multiple concepts: `src/backend/core_transaction.rs` (~13.8k lines), `src/backend/postgres.rs` (~13.6k), `src/server/routes_vcs.rs` (~13.5k), `src/server/routes_fs.rs` (~7.2k), `src/server/routes_review.rs` (~6.8k), and `src/backend/object_cleanup.rs` (~6.6k). Deepening opportunity: split by durable transaction phase, store adapter family, and route family test support so the interface remains smaller than the implementation.
- Partially fixed 2026-06-04: durable post-CAS workspace-head repair now has explicit outcome vocabulary for repaired, already-desired, and superseded heads. Remaining debt: poison state and partial-audit completion semantics are still inferred across stores, route recovery endpoints, worker code, and migrations.
- Fixed 2026-06-04: local HTTP `User` auth and `/auth/login` are now represented by an explicit server HTTP security policy instead of an implicit router default. The binary enables those development identity assertions only for loopback/localhost listeners unless `STRATUM_ALLOW_INSECURE_DEV_USER_AUTH=1` is set.

## Performance and Optimization Opportunities

- Fixed 2026-06-04: `VirtualFs::find` no longer recompiles the name matcher for every directory entry. Evidence: `tests/perf search::perf_find_across_tree` improved from 9.10s for 100 iterations (91.04ms/op) to 30.91ms (309.11us/op) by compiling the existing matcher semantics once per `find` call.
- Fixed 2026-06-04: hot path resolution now uses borrowed path components instead of allocating a `String` per component. Evidence: focused release perf improved shallow resolution from 41.13ms to 14.77ms for 100k reads and depth-50 resolution from 66.32ms to 31.33ms for 10k reads.
- Fixed 2026-06-04: `VirtualFs::tree` no longer allocates a visible-entry `Vec` per directory just to detect the final child. Evidence: focused release perf improved tree rendering from 8.35ms to 2.36ms for the 50-iteration large hierarchy case.
- Audit append in Postgres is globally serialized with a single advisory lock and global sequence rows. This is simple and correct for ordering, but it can bottleneck multi-repo durable workloads. Consider repo-scoped sequencing for repo-scoped events.
- Object GC recovery-root scans use global recovery lists before filtering by repo. This is safe for deletion but lets a noisy repo block cleanup proof for another repo. Prefer repo-scoped recovery scans, mirroring idempotency retention's repo-scoped counts.
- Borrow from SMFS: use queued/coalesced mutation syncing for any future remote-first `stratumctl` or mounted cloud-sync path so rapid repeated writes collapse to latest-wins work instead of best-effort immediate pushes.

## Reliability / Operational Hardening

- Fixed 2026-06-04: migration smoke coverage now includes every `migrations/postgres/*.sql` file and `scripts/check-postgres-migrations.sh` fails before optional DB execution if a catalog migration is missing from the smoke SQL. The smoke still seeds a pre-`0009` workspace token before applying `0009` so the auth-session backfill scenario remains covered.
- Fixed 2026-06-04: derived-poison object cleanup claims are no longer re-claimable or returned from the scheduler claimable scan once they reach `ObjectCleanupWorker::MAX_ATTEMPTS` with a failure. In-memory and Postgres claim paths both exclude those rows from acquisition, while list/count status APIs continue to surface them as failed and poisoned.
- Fixed 2026-06-04: server startup bind and serve errors no longer panic/expect in `src/bin/stratum_server.rs`. Bind failures log a redacted operational error and exit with status 1; serve failures log after shutdown cleanup and then exit with status 1.
- Borrow from SMFS: expose health/status counters for backend mode, pending idempotency records, recovery claims, cleanup claims, and object-GC blockers.

## Security / Redaction / Auth Risks

- Fixed 2026-06-04: agent API token generation no longer hashes the current timestamp plus a static string. `src/auth/registry.rs` now generates 32 random bytes with `OsRng`, returns lowercase hex, keeps the stored SHA-256 hash shape for compatibility, and compares token hashes with a constant-time helper.
- Fixed 2026-06-04: local HTTP `Authorization: User <username>` and `/auth/login` are disabled for non-loopback listeners by default. `STRATUM_ALLOW_INSECURE_DEV_USER_AUTH=1` is now an explicit trusted-development override; bearer and workspace bearer auth remain available.
- Fixed 2026-06-04: HTTP routers no longer use permissive CORS by default. Browser origins must be explicitly listed with `STRATUM_CORS_ALLOWED_ORIGINS`, and wildcard CORS is rejected at startup.
- Workspace token issuance correctly rejects idempotency keys because responses are secret-bearing; keep this invariant when adding future replayable secret storage.

## Test Gaps

- Fixed 2026-06-04: added regression coverage for local `User` auth fail-closed behavior when the server HTTP security policy disables development identity assertions.
- Fixed 2026-06-04: added CORS coverage for default origin rejection, explicit origin allowlisting, authorization/idempotency preflight headers, and wildcard-origin config rejection.
- Fixed 2026-06-04: added `append_once` regression coverage for VCS visible-commit and durable FS mutation audit identities, including concurrent in-memory calls and local persistence. Existing post-CAS and durable FS recovery worker duplicate-audit suites now exercise the append-once path.
- Fixed 2026-06-04: added workspace-head repair worker coverage for already-desired heads and superseded third-commit heads, plus route/scheduler assertions that the new outcome counters remain visible in operator JSON.
- Add object cleanup tests where recovery rows in repo B do not block GC proof for repo A.
- Fixed 2026-06-04: object cleanup tests now cover max-attempt failed claims moving out of the claimable scheduler path while remaining visible in status/counts.
- Fixed 2026-06-04: `scripts/check-postgres-migrations.sh` now asserts that every migration SQL file is included by the Postgres smoke file. Live SQL execution still requires `STRATUM_POSTGRES_TEST_URL`.

## Product / API Polish

- SDK workspace auth lacks an optional repo id header, so SDKs cannot directly call durable admin routes requiring `X-Stratum-Repo`. Suggested change: add optional `repoId` to TypeScript/Python workspace auth options and emit `X-Stratum-Repo`; use capabilities to explain when it is required.
- Fixed 2026-06-04: audit capabilities now document the existing route policy as user-admin-only with `requires: ["user-admin"]` and a bearer-rejection note, instead of advertising only generic admin availability.
- Borrow from Mirage: extend `/v1/capabilities` beyond route availability with filesystem, command, mount, and filetype affordances as additive metadata.
- Borrow from SMFS: when semantic search lands, preserve literal `/search/grep` semantics and expose semantic search through an explicit mode or clearly marked CLI/mount behavior to avoid surprising scripts.

## Follow-up Feature Ideas

- Add semantic-index include/exclude path scopes per workspace/repo before enabling embeddings broadly. This follows SMFS's separation between durable storage and indexed memory and reduces privacy/noise risk.
- Add truth-file style smoke fixtures for CLI/server output, inspired by Mirage's line-oriented integration checks that tolerate volatile IDs/timestamps while asserting important diagnostics.
- Add SDK client knobs for mount cache TTL/disable, eager path-index warmup, logger hooks, and a `refresh()` helper, borrowing from SMFS's virtual Bash runtime options.
- Treat future remote/blob/provider mounts as explicit Stratum workspace sources with mount metadata in capabilities, but keep durable core/recovery guarantees ahead of a broad provider matrix.

## Borrowable Ideas from SMFS and Mirage

- SMFS product framing: "read, write, and grep like any local directory" is a clearer first-use mental model. Stratum docs can lead with durable files plus commits/rollback, then introduce HTTP/MCP/FUSE.
- SMFS semantic grep pattern: familiar command vocabulary matters, but semantic behavior should be opt-in or explicitly scoped in Stratum to preserve script compatibility.
- SMFS persistent push queue and `dirty_since` patterns are useful for any future remote sync layer: coalesce repeated path writes and protect fresh local mutations from stale pulls.
- SMFS daemon/status protocol suggests operational counters that agents can inspect without logs.
- Mirage multi-backend namespace and command capability model suggests future Stratum mount/source metadata, but Stratum should avoid diluting reliability by adding many providers before durable backend semantics are solid.
- Mirage lazy provider registry and runtime package split suggest keeping optional SDK/provider dependencies out of the default SDK surface as Stratum grows.
- Mirage generated command/spec fixtures suggest moving Stratum docs/SDK contract checks toward generated or Rust-owned contract data rather than hand-maintained route drift.

## Review Notes

- 2026-06-04: Initial repo status check found existing user work in `.gitignore`, `web/src/lib/api/reviews.ts`, `web/src/lib/api/reviews.test.tsx`, `.agents/`, `docs/plans/2026-05-15-backend-roadmap.md`, `sdk/bunfig.toml`, `site/app.html`, and `skills-lock.json`. This review should avoid those paths unless a later backend fix explicitly requires them.
- 2026-06-04: Parallel read-only audit slices completed for durable backend/migrations, server/auth/API seams, and sibling-project borrowable patterns. Main-worktree fixes are being kept small and committed separately.
- 2026-06-04: Release perf baseline before the `find` optimization passed all 37 perf tests. The dominant in-memory hotspot was `find -name *.md` at 9.10s for 100 iterations; after the matcher fix, the focused perf case passed at 30.91ms.
- 2026-06-04: Whole-product performance subagent confirmed VCS full-tree work, global DB locking, persistence lock scope, path allocation, find regex recompilation, and traversal allocation as the main backend opportunities. Safe-now fixes were limited to matcher/path allocation; broader lock/persistence/VCS changes need design to avoid changing behavior.
- 2026-06-04: Final broad gates passed: `cargo test --locked --all-targets`, `cargo fmt --all -- --check`, `git diff --check`, `STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh` (coverage check passed; live SQL skipped because URL unset), and warm `cargo test --locked --release --test perf -- --test-threads=1 --nocapture`. Final release perf completed 37/37 tests in 0.75s; `find -name *.md` was 31.67ms for 100 iterations, permission-filtered find was 12.16ms for 50 iterations, and large tree rendering was 2.72ms for 50 iterations.
