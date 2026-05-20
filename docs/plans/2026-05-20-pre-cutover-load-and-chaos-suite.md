# Pre-Cutover Load And Chaos Suite Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a repeatable, bounded pre-cutover load and chaos suite that proves Stratum's durable commit, recovery, idempotency, and object-store behavior remains fail-closed, redacted, and non-duplicating before durable-cloud can become the default runtime.

**Architecture:** Keep the suite provider-free and deterministic by default, using existing in-memory durable stores, route-level durable-cloud helpers, and direct scheduler ticks instead of sleeping on long background loops. Add a small script that runs the local pre-cutover gates and optionally chains the existing live Postgres/R2 wrappers when credentials are intentionally supplied. Extend docs/status to make local, CI, and live-provider execution explicit without changing runtime defaults.

**Tech Stack:** Rust 2024, Tokio, Axum route tests, existing `StratumStores::local_memory()`, durable recovery claim stores, object cleanup harnesses, idempotency stores, Bash gate scripts, optional Postgres feature tests, and existing R2/Postgres live wrappers.

---

## Required Context

- `markdownfs_v2_cto_architecture_plan.md` defines Stratum v2 as a durable Rust filesystem/control plane backed by Postgres metadata plus S3/R2-compatible object storage.
- `docs/plans/2026-05-15-backend-roadmap.md` Slice 5.5 requires concurrent writes, gateway restart during visible and pre-visible phases, object-store failure/retry, recovery convergence, permission/search cache correctness where applicable, and a security-review checklist before the durable-cloud default flip.
- `docs/project-status.md` says durable-cloud is still explicitly gated by `STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV=1` and must not open local `.vfs/state.bin`.
- `docs/http-api-guide.md` documents durable-cloud route support, recovery scheduler controls, manual recovery, redaction requirements, live Postgres/R2 gate behavior, and unsupported durable-cloud route groups.
- `docs/plans/2026-05-19-recovery-scheduler-productionization.md` completed explicit scheduler config, bounded shutdown drain, disabled status handles, and claim/lease-based multi-worker safety.
- `docs/plans/2026-05-17-secret-bearing-idempotency-replay-via-kms.md` limits secret-bearing idempotency to workspace-token issuance through encrypted KMS-backed replay.
- `extract pieces.md` warns not to import latest-wins queue/cache semantics; Stratum recovery remains keyed by operation/ref/commit/idempotency identity.

## Baseline

- Local-state remains the default runtime.
- Durable-cloud remains dev/test gated and requires durable backend stores, repo routing, auth/session, policy, recovery, idempotency retention, and hosted storage posture env gates.
- Existing tests already cover many individual behaviors:
  - durable-cloud mounted-session FS mutations and concurrent writes in `src/server/routes_fs.rs`;
  - guarded durable commit/recovery/revert/idempotency behavior in `src/server/routes_vcs.rs`;
  - scheduler status, disabled mode, multi-worker races, and shutdown drain in `src/server/mod.rs`;
  - object cleanup reachability, fences, hold windows, and non-destructive readiness in `src/backend/object_cleanup.rs`;
  - idempotency quota, stale pending, encrypted secret replay, and redacted debug output in `src/idempotency.rs`;
  - optional provider gates in `scripts/check-postgres-migrations.sh`, `scripts/check-r2-object-store.sh`, `scripts/ci-live-postgres-gate.sh`, and `scripts/ci-live-r2-gate.sh`.
- Live Postgres/R2 gates are provider-verified green on protected main, but local credentials may be absent. Do not claim a fresh live provider run unless it is actually run locally.

## Required Skills And Agent Discipline

Implementation and review agents must use these skills as relevant:

- `superpowers:test-driven-development`
- `pragmatic-rust-guidelines`
- `rust-async-patterns`

The main session owns integration, review, verification, commits, merge to `main`, and pushes. Subagents may implement scoped tasks, but the main session must inspect diffs, fix integration issues, and rerun gates before accepting work. Keep commits small.

## Non-Goals

- Do not flip durable-cloud to the default runtime.
- Do not remove `STRATUM_DURABLE_CORE_RUNTIME_ENABLE_DEV`.
- Do not expose destructive object deletion controls by default.
- Do not add broad unreachable commit/object deletion.
- Do not add Redis, Postgres advisory locks, or another distributed lock service.
- Do not add a production KMS/secrets-manager provider.
- Do not route unsupported durable-cloud groups such as auth login, workspace management, audit serving, runs, execution, semantic search, MCP/FUSE, or recovery operator routes unless a task explicitly says to verify their stable fail-closed response.
- Do not create a sparse FUSE/cache subsystem or hydration scheduler.
- Do not weaken PR-safe live-gate skip behavior.

## Suite Shape

Add a local suite script:

```bash
./scripts/check-pre-cutover-load-chaos.sh
```

Default behavior:

- runs bounded local tests only;
- requires no Postgres or R2 live credentials;
- does not start a server against live providers;
- skips optional live wrappers unless explicitly requested.

Optional live behavior:

```bash
STRATUM_PRE_CUTOVER_LIVE=1 ./scripts/check-pre-cutover-load-chaos.sh
```

When `STRATUM_PRE_CUTOVER_LIVE=1`, the script should call the existing live wrappers:

```bash
./scripts/ci-live-postgres-gate.sh
./scripts/ci-live-r2-gate.sh
```

If `STRATUM_LIVE_GATE_REQUIRED=1` is also set, the existing wrappers fail closed on missing credentials. Otherwise they skip cleanly. Do not duplicate credential parsing or secret handling in the new script. Do not call `scripts/check-postgres-migrations.sh` directly in secretful CI unless `STRATUM_POSTGRES_REDACT_ERRORS=1` is set; use `scripts/ci-live-postgres-gate.sh` instead. Do not call raw R2 live tests directly from the new script; use `scripts/ci-live-r2-gate.sh`.

## Redaction Denylist

New tests that inspect JSON/status/error/script output must assert absence of representative sensitive values:

- database URLs and passwords;
- R2 endpoints, bucket names, access keys, and secret keys;
- canonical object keys and staged-upload keys;
- raw backend/provider errors;
- SQL text and migration SQL;
- request bodies and commit messages;
- raw `Idempotency-Key` values and key hashes where public output should not include them;
- raw workspace tokens, agent tokens, KMS key material, and plaintext secret replay bodies;
- local filesystem paths such as `.vfs/state.bin` where durable-cloud output should not expose local fallback details.

Public durable outputs may include bounded stable identifiers already documented as safe: repo ids where expected, short object ids, commit ids, phase names, counts, status markers, and redacted correlation ids.

Specific redaction regressions to cover:

- `GET /vcs/recovery` and `POST /vcs/recovery/run` should omit the denylist for every recovery class, including pre-visibility, post-CAS, durable FS mutation, and object cleanup.
- `health.scheduler.last_error` should contain only fixed phase/status codes such as `post_cas_failed`, not raw store or provider messages.
- Object cleanup status should not expose canonical final object keys, staged-upload keys, lease owners, lease tokens, raw provider failures, or internal orphan-cleanup report details.
- Internal structs that legitimately carry sensitive details can remain internal, but public route output, script output, docs examples, and `Debug` assertions in tests must not make those details observable.

## Task 1: Save The Plan

**Files:**
- Create: `docs/plans/2026-05-20-pre-cutover-load-and-chaos-suite.md`

**Step 1: Write this plan**

Create this file before implementation.

**Step 2: Verify docs-only diff**

Run:

```bash
git diff -- docs/plans/2026-05-20-pre-cutover-load-and-chaos-suite.md
git diff --check
```

Expected: only this plan doc is changed and whitespace is clean.

**Step 3: Commit**

```bash
git add docs/plans/2026-05-20-pre-cutover-load-and-chaos-suite.md
git commit -m "docs: plan pre-cutover load and chaos suite"
```

## Task 2: Add Local Suite Script

**Files:**
- Create: `scripts/check-pre-cutover-load-chaos.sh`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Write failing script presence/docs checks**

Add or extend tests/checks that prove:

- `scripts/check-pre-cutover-load-chaos.sh` exists and is executable;
- `bash -n scripts/check-pre-cutover-load-chaos.sh` succeeds;
- docs mention the script and optional live mode.

If no dedicated script-doc test pattern exists, start with a shell syntax verification and docs grep in the plan's verification commands rather than adding a brittle Rust test.

Run:

```bash
test -x scripts/check-pre-cutover-load-chaos.sh
bash -n scripts/check-pre-cutover-load-chaos.sh
rg -n "check-pre-cutover-load-chaos|STRATUM_PRE_CUTOVER_LIVE" docs/http-api-guide.md docs/project-status.md
```

Expected before implementation: file/docs checks fail.

**Step 2: Implement the script**

Create a strict Bash script with:

```bash
#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
cd "$repo_root"

cargo test --locked server::routes_fs::tests::durable_cloud_pre_cutover --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::durable_pre_cutover --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler_pre_cutover --lib -- --nocapture
cargo test --locked backend::object_cleanup::tests::pre_cutover --lib -- --nocapture
cargo test --locked idempotency::tests::pre_cutover --lib -- --nocapture

if [[ "${STRATUM_PRE_CUTOVER_LIVE:-}" == "1" ]]; then
  ./scripts/ci-live-postgres-gate.sh
  ./scripts/ci-live-r2-gate.sh
fi
```

Use exact module filters from the implemented tests. Keep command output normal for local tests. Rely on the existing live wrappers for secret masking and redacted failure output.

**Step 3: Document execution**

In `docs/http-api-guide.md`, add a compact section near live gate or durable-cloud docs explaining:

- local run command;
- optional live mode;
- that live provider checks still skip unless env credentials and required flags are present;
- redaction expectations;
- durable-cloud default remains gated.

In `docs/project-status.md`, add a current-slice status entry once implemented.

**Step 4: Verify**

Run:

```bash
bash -n scripts/check-pre-cutover-load-chaos.sh
test -x scripts/check-pre-cutover-load-chaos.sh
STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_PRE_CUTOVER_LIVE=1 STRATUM_LIVE_GATE_REQUIRED=0 STRATUM_POSTGRES_TEST_URL= STRATUM_R2_TEST_ENABLED= ./scripts/check-pre-cutover-load-chaos.sh
```

Expected: local suite passes once tasks below exist. Optional live run skips existing provider wrappers cleanly when credentials are absent and required mode is not enabled.

**Step 5: Commit**

```bash
git add scripts/check-pre-cutover-load-chaos.sh docs/http-api-guide.md docs/project-status.md
git commit -m "test: add pre-cutover load chaos suite runner"
```

## Task 3: Durable-Cloud FS/Search Load And Permission Tests

**Files:**
- Modify: `src/server/routes_fs.rs`

**Step 1: Add failing bounded pre-cutover tests**

Create a nested test module or consistently prefixed tests under the existing `routes_fs` test module, using the prefix `durable_cloud_pre_cutover`.

Add tests proving:

- a bounded concurrent mounted-session write load advances a durable session ref without duplicate audit entries and without local state fallback;
- after repeated writes, `GET /tree`, `GET /search/find`, and `GET /search/grep` observe the durable session content through the workspace projection;
- a token with limited read/write prefixes can mutate only permitted prefixes and search/tree only permitted readable paths;
- same `Idempotency-Key` retries under load replay without duplicate visible side effects;
- response/error bodies do not include raw token values, raw idempotency keys, request bodies, or backing local paths.

Use small fixed counts, for example 8 to 12 writes, and `tokio::time::timeout` around any join set.

Run:

```bash
cargo test --locked server::routes_fs::tests::durable_cloud_pre_cutover --lib -- --nocapture
```

Expected before implementation: tests fail because the new test module does not exist or assertions expose missing suite coverage.

**Step 2: Implement with existing helpers**

Reuse existing helpers such as:

- `durable_cloud_demo_router_with_token`;
- `durable_cloud_router_with_workspace`;
- `durable_workspace_state_with_scoped_token`;
- existing response JSON/body helpers;
- existing durable object/tree seeding helpers.

Do not expose new production APIs only for tests. Prefer route calls through the Axum router or existing route functions.

**Step 3: Verify**

Run:

```bash
cargo test --locked server::routes_fs::tests::durable_cloud_pre_cutover --lib -- --nocapture
cargo test --locked server::routes_fs::tests::durable_cloud --lib -- --nocapture
```

**Step 4: Commit**

```bash
git add src/server/routes_fs.rs
git commit -m "test: stress durable-cloud fs pre-cutover behavior"
```

## Task 4: Guarded Durable VCS/Recovery Chaos Tests

**Files:**
- Modify: `src/server/routes_vcs.rs`

**Step 1: Add failing pre-cutover chaos tests**

Create a nested test module or consistently prefixed tests under the existing `routes_vcs` test module, using the prefix `durable_pre_cutover`.

Add tests proving:

- repeated same-key guarded durable `POST /vcs/commit` retries create one visible commit and one audit side effect;
- pre-visibility metadata-insert ack loss and ref-visibility uncertainty remain in-progress until bounded recovery proves visibility or aborts safely;
- post-CAS audit/idempotency failures converge through bounded `POST /vcs/recovery/run` without duplicate audit/idempotency side effects;
- manual recovery respects caller limits and returns remaining work rather than hanging;
- recovery status/run output is redacted against the denylist.

Use existing failing stores and ack-loss doubles near the current guarded durable recovery tests. Keep counts small and deterministic.

Run:

```bash
cargo test --locked server::routes_vcs::tests::durable_pre_cutover --lib -- --nocapture
```

Expected before implementation: tests fail because the new module or assertions do not exist.

**Step 2: Implement using existing route helpers**

Reuse:

- `guarded_durable_commit_state*` helpers;
- `user_headers_with_idempotency`;
- ack-loss commit/ref metadata doubles;
- failing idempotency/audit stores;
- existing recovery status/run helpers and JSON extraction.

Do not add a new recovery algorithm. Tests should exercise the existing route and worker behavior.

**Step 3: Verify**

Run:

```bash
cargo test --locked server::routes_vcs::tests::durable_pre_cutover --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable_commit --lib -- --nocapture
```

**Step 4: Commit**

```bash
git add src/server/routes_vcs.rs
git commit -m "test: add durable vcs pre-cutover chaos coverage"
```

## Task 5: Scheduler Multi-Phase Bounded Chaos Tests

**Files:**
- Modify: `src/server/mod.rs`

**Step 1: Add failing scheduler pre-cutover tests**

Under the existing server test module, add tests with the prefix `durable_recovery_scheduler_pre_cutover`.

Add tests proving:

- one direct scheduler tick with a small `tick_limit` drains phases in order and stops at the budget;
- a second tick resumes remaining work without duplicating completed side effects;
- concurrent direct ticks over the same stores use claim/lease fencing and do not duplicate audit/idempotency completion;
- failing phase tests set sentinel DB URLs, R2 endpoints, SQL, object keys, commit messages, request bodies, idempotency keys, and raw tokens in backing errors but expose only fixed scheduler `last_error` values;
- shutdown drain timeout records a fixed redacted `timed_out` or partial outcome and no new background tick starts after timeout.

Prefer direct calls to `durable_recovery_scheduler_tick` where possible. Avoid long sleeps and background-loop timing unless the specific behavior is about the loop lifecycle.

Run:

```bash
cargo test --locked server::tests::durable_recovery_scheduler_pre_cutover --lib -- --nocapture
```

Expected before implementation: tests fail because they do not exist.

**Step 2: Implement with existing seed helpers**

Use existing recovery targets and envelopes in `src/server/mod.rs` tests. Seed at least two phase types in the same test so the suite proves phase interaction, not just isolated worker behavior.

Keep all values bounded:

- `tick_limit` no larger than 3 in budget tests;
- timeouts in milliseconds;
- no unbounded retry loops;
- no raw sensitive strings in status assertions.

**Step 3: Verify**

Run:

```bash
cargo test --locked server::tests::durable_recovery_scheduler_pre_cutover --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
```

**Step 4: Commit**

```bash
git add src/server/mod.rs
git commit -m "test: add scheduler pre-cutover chaos coverage"
```

## Task 6: Object Cleanup And Object-Store Failure Pressure Tests

**Files:**
- Modify: `src/backend/object_cleanup.rs`
- Modify if needed: `src/backend/blob_object.rs`
- Modify if needed: `src/remote/blob.rs`

**Step 1: Add failing object cleanup pre-cutover tests**

Under existing object cleanup tests, add tests with the prefix `pre_cutover`.

Add tests proving:

- many CAS-lost cleanup claims are processed with a bounded worker limit;
- final-object cleanup remains non-destructive by default and reports readiness/hold state, not deletion;
- metadata fences prevent cleanup races;
- cleanup failure diagnostics/status remain redacted and do not include object keys, staged-upload keys, lease owners, lease tokens, or provider details;
- internal orphan cleanup reports that contain object keys/provider messages are not surfaced through recovery status/run responses;
- object-store adapter failures surface fixed redacted errors.

Run:

```bash
cargo test --locked backend::object_cleanup::tests::pre_cutover --lib -- --nocapture
cargo test --locked remote::blob::tests::r2_error_redaction --lib -- --nocapture
```

Expected before implementation: focused tests fail or do not exist.

**Step 2: Implement with existing harnesses**

Reuse `GcHarness`, `WorkerHarness`, `InMemoryObjectMetadataStore`, and existing R2/local blob redaction tests where possible. Do not add destructive deletion as a default path. Do not make provider-backed tests run without explicit env gates.

**Step 3: Verify**

Run:

```bash
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked backend::blob_object --lib -- --nocapture
cargo test --locked remote::blob --lib -- --nocapture
```

**Step 4: Commit**

```bash
git add src/backend/object_cleanup.rs src/backend/blob_object.rs src/remote/blob.rs
git commit -m "test: stress object cleanup pre-cutover behavior"
```

Only add files that actually changed.

## Task 7: Idempotency Load, Retention, And Redaction Tests

**Files:**
- Modify: `src/idempotency.rs`
- Modify if needed: `src/server/idempotency.rs`
- Modify if needed: `src/server/routes_workspace.rs`

**Step 1: Add failing idempotency pre-cutover tests**

Under existing idempotency tests, add tests with the prefix `pre_cutover`.

Add tests proving:

- concurrent same-key begin/complete attempts produce exactly one executable reservation and then replay;
- same-key different-fingerprint attempts remain conflicts without inserting extra pending/completed records;
- stale pending takeover remains bounded and stale reservation tokens cannot complete later;
- completed secret-bearing replay remains encrypted-only and redacted in `Debug`;
- retention sweeps are bounded and preserve records blocked by unresolved recovery roots.

Run:

```bash
cargo test --locked idempotency::tests::pre_cutover --lib -- --nocapture
```

Expected before implementation: tests fail because they do not exist.

**Step 2: Implement with existing store APIs**

Use `InMemoryIdempotencyStore`, policy-enabled begin helpers, existing secret replay metadata builders, and retention request helpers. Avoid sleeps where a test-only clock or direct state setup exists.

**Step 3: Verify**

Run:

```bash
cargo test --locked idempotency --lib -- --nocapture
cargo test --locked server::idempotency --lib -- --nocapture
cargo test --locked server::routes_workspace::tests::workspace_token --lib -- --nocapture
```

**Step 4: Commit**

```bash
git add src/idempotency.rs src/server/idempotency.rs src/server/routes_workspace.rs
git commit -m "test: add idempotency pre-cutover pressure coverage"
```

Only add files that actually changed.

## Task 8: Startup And Provider Gate Redaction Checks

**Files:**
- Modify: `tests/server_startup.rs`
- Modify if needed: `scripts/check-pre-cutover-load-chaos.sh`
- Modify if needed: `.github/workflows/rust-ci.yml`

**Step 1: Add failing startup/gate tests**

Add focused tests proving:

- durable-cloud startup still fails closed before creating local state files when gates are missing;
- invalid live/provider posture errors do not echo raw env values;
- scheduler env vars are cleaned from startup tests and do not cross-contaminate runs;
- the pre-cutover script is syntax-valid and uses existing live wrappers for optional provider gates.

Before adding workflow changes, inspect `.github/workflows/rust-ci.yml`. Only add the new local suite to CI if it is bounded enough for regular PR execution. If not, document it and leave CI unchanged.

Run:

```bash
cargo test --locked --test server_startup durable -- --nocapture
bash -n scripts/check-pre-cutover-load-chaos.sh
```

Expected before implementation: new assertions fail if the script/docs are absent.

**Step 2: Implement**

Prefer server-startup assertions over shelling out from Rust tests. Keep secret denylist checks aligned with `assert_no_secret_leaks`.

**Step 3: Verify**

Run:

```bash
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
bash -n scripts/check-pre-cutover-load-chaos.sh
```

**Step 4: Commit**

```bash
git add tests/server_startup.rs scripts/check-pre-cutover-load-chaos.sh .github/workflows/rust-ci.yml
git commit -m "test: keep pre-cutover startup gates redacted"
```

Only add files that actually changed.

## Task 9: Documentation And Status Finalization

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update docs**

Document:

- local suite command;
- optional live-provider command;
- exact env gates for live mode;
- expected skip behavior when credentials are absent;
- no destructive cleanup by default;
- durable-cloud default remains gated;
- known limits: no production default flip, no distributed lock service, no production KMS provider, no broad cache/FUSE validation.

**Step 2: Update project status**

Add a completed Slice 5.5 section only after tests and reviews are complete. Include factual verification results only for commands actually run.

**Step 3: Verify docs**

Run:

```bash
rg -n "pre-cutover|check-pre-cutover-load-chaos|STRATUM_PRE_CUTOVER_LIVE|destructive" docs/http-api-guide.md docs/project-status.md
git diff --check
```

**Step 4: Commit**

```bash
git add docs/http-api-guide.md docs/project-status.md
git commit -m "docs: document pre-cutover load chaos suite"
```

## Review Plan

After implementation tasks and before final gates:

1. Run a spec/correctness review subagent on the full diff. Focus on acceptance criteria, out-of-scope traps, deterministic bounds, duplicate side effects, fail-closed durable-cloud posture, provider skip behavior, and docs accuracy.
2. Fix findings locally.
3. Run a code-quality/security review subagent on the full diff. Focus on Rust async correctness, test flakiness, redaction, secret handling, excessive test runtime, unnecessary production API exposure, and script safety.
4. Fix findings locally.
5. Rerun focused gates after each fix.

## Required Verification

Run the following before merging:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked server::routes_fs::tests::durable_cloud_pre_cutover --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::durable_pre_cutover --lib -- --nocapture
cargo test --locked server::tests::durable_recovery_scheduler_pre_cutover --lib -- --nocapture
cargo test --locked backend::object_cleanup::tests::pre_cutover --lib -- --nocapture
cargo test --locked idempotency::tests::pre_cutover --lib -- --nocapture
STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
cargo test --locked server::tests::durable_recovery_scheduler --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_recovery --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_core_commit_post_cas_recovery --lib -- --nocapture
cargo test --locked backend::core_transaction::tests::durable_fs_mutation_recovery --lib -- --nocapture
cargo test --locked backend::object_cleanup --lib -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

Run live provider gates only if credentials are present locally:

```bash
STRATUM_PRE_CUTOVER_LIVE=1 STRATUM_LIVE_GATE_REQUIRED=1 ./scripts/check-pre-cutover-load-chaos.sh
```

If credentials are absent, record that local live provider gates were not run and that protected-main CI has provider-verified green live Postgres/R2 gates from the latest protected-main run.

## Expected Final Commits

Keep commits small and reviewable:

1. `docs: plan pre-cutover load and chaos suite`
2. `test: add pre-cutover load chaos suite runner`
3. `test: stress durable-cloud fs pre-cutover behavior`
4. `test: add durable vcs pre-cutover chaos coverage`
5. `test: add scheduler pre-cutover chaos coverage`
6. `test: stress object cleanup pre-cutover behavior`
7. `test: add idempotency pre-cutover pressure coverage`
8. `test: keep pre-cutover startup gates redacted`
9. `docs: document pre-cutover load chaos suite`

Combine adjacent commits only if a task turns out to be docs-only or no-op after inspection.
