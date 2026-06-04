# Conformance Test Scaffolding Implementation Plan

**Goal:** Add shared, provider-free conformance scaffolding that proves local-state and durable-cloud route behavior, SDK parity, auth failures, idempotency replay, unsupported durable responses, and capability manifest alignment.

**Architecture:** Keep the conformance model as data first: a checked-in JSON fixture describes route expectations, auth profile requirements, idempotency behavior, SDK method coverage, and redaction constraints. Rust server tests generate and consume that fixture for local-state and durable-cloud test routers; TypeScript, Python, Bash, and Rust client tests consume the same fixture so SDK drift is caught without changing public SDK shapes.

**Tech Stack:** Rust, Axum tower services, serde/serde_json, existing Stratum server route tests, TypeScript SDK tests with Bun, Python SDK tests with pytest/mypy/ruff, Bash SDK tests, checked-in JSON contracts under `sdk/contracts`.

---

## Context Already Read

- `markdownfs_v2_cto_architecture_plan.md`
- `docs/plans/2026-05-15-backend-roadmap.md`
- `docs/project-status.md`
- `docs/http-api-guide.md`
- `docs/semantic-index.md`
- `src/server/mod.rs`
- `src/server/routes_capabilities.rs`
- `src/server/routes_fs.rs`
- `src/server/routes_vcs.rs`
- `src/server/routes_workspace.rs`
- `src/server/idempotency.rs`
- `src/client/mod.rs`
- `src/bin/stratumctl.rs`
- `sdk/contracts/capabilities.v1.json`
- `sdk/contracts/capabilities.v1.durable-cloud.json`
- `sdk/typescript/src/client.ts`
- `sdk/typescript/src/types.ts`
- `sdk/typescript/tests/client.test.ts`
- `sdk/python/src/stratum/client.py`
- `sdk/python/src/stratum/types.py`
- `sdk/python/tests/test_client.py`
- `sdk/bash/src/index.ts`
- `sdk/bash/tests/index.test.ts`
- Bounded Mirage VFP capability/method declarations and compact server route tests.

## Current Facts To Preserve

- Branch: `v2/foundation`.
- Keep unrelated work in other checkouts untouched.
- Capability manifests are already checked in at:
  - `sdk/contracts/capabilities.v1.json`
  - `sdk/contracts/capabilities.v1.durable-cloud.json`
- Current capability revision is `2026-05-17-2`.
- Durable router currently registers read-oriented durable routes plus `durable_unsupported_routes`.
- Durable unsupported responses currently return HTTP `501` with stable JSON:

```json
{"error":"stratum: operation not supported: durable-cloud route is not supported yet"}
```

- Idempotency replay header constant is `x-stratum-idempotent-replay` and replay value is `true`.
- Do not change public SDK method shapes unless a task explicitly finds and documents a required contract mismatch.

## Scope

### In Scope

- Add a local, deterministic conformance fixture model.
- Cover both `local-state` and `durable-cloud` route behavior through in-process test routers.
- Cover route shape, auth errors, idempotency replay, unsupported durable responses, and capability manifest parity.
- Add SDK parity tests for TypeScript, Python, Bash, and the Rust client/CLI surface where existing APIs expose the route.
- Keep durable live checks explicit and advisory only.
- Add docs explaining how to run and update conformance fixtures.
- Add redaction/stability checks so fixtures never contain secrets, raw backing paths, object keys, provider errors, DB URLs, or timing-sensitive values.

### Out Of Scope

- Requiring live Postgres, R2, or hosted infrastructure in default CI.
- Implementing new durable-cloud write behavior.
- Adding a public `stratumctl capabilities` command.
- Broad route rewrites or behavior changes not required by a failing conformance test.
- Provider-backed semantic generation.
- Load, stress, latency, or timing assertions.

## Fixture Contract

Create `sdk/contracts/conformance.routes.v1.json`.

The fixture must be deterministic, compact, and safe to publish. It describes expected route behavior, not raw runtime data.

Initial shape:

```json
{
  "version": 1,
  "revision": "2026-06-04-1",
  "capability_revision": "2026-05-17-2",
  "modes": ["local-state", "durable-cloud"],
  "auth_profiles": {
    "none": {"kind": "none"},
    "root-user": {"kind": "user", "label": "root"},
    "workspace-bearer": {"kind": "bearer", "label": "workspace"}
  },
  "forbidden_substrings": [
    "postgres://",
    "postgresql://",
    "Bearer ",
    "STRATUM_",
    "SQLSTATE",
    "object_key",
    "raw_secret",
    "workspace-secret",
    "agent-token",
    "/tmp/",
    "<absolute-home-path>"
  ],
  "cases": [
    {
      "id": "capabilities.local.public",
      "mode": "local-state",
      "method": "GET",
      "path": "/v1/capabilities",
      "auth": "none",
      "expect": {
        "status": 200,
        "headers": {
          "cache-control": "max-age=60, must-revalidate"
        },
        "json_paths": {
          "$.schema_version": 1,
          "$.revision": "2026-05-17-2",
          "$.routes.capabilities.self.available": true
        }
      },
      "sdk": {
        "typescript": "getCapabilities",
        "python": "get_capabilities",
        "bash": null,
        "rust_client": null,
        "cli": null
      }
    }
  ]
}
```

Rules:

- Fixture auth profiles are symbolic only. Tests resolve them to local headers; the fixture must not contain actual bearer values.
- Do not store raw response bodies for mutable filesystem operations. Store status, stable response shape, selected JSON paths, body class, and replay/conflict indicators.
- Do not assert timestamps, durations, autogenerated IDs, object keys, temp paths, absolute local paths, provider messages, or DB errors.
- Every case must declare `mode`, `method`, `path`, `auth`, `expect.status`, and `sdk`.
- Every non-`GET` mutation case must declare whether an idempotency key is required, ignored, or unsupported.
- Keep semantic search cases provider-free. Assert capability/route behavior and stable unavailable reasons, not embedding output.

## Expected Coverage Matrix

### Capability Manifest Cases

- `GET /v1/capabilities` in `local-state` returns schema version `1`, revision `2026-05-17-2`, and local route availability.
- `GET /v1/capabilities` in `durable-cloud` returns schema version `1`, revision `2026-05-17-2`, durable `state_mode`, and durable route availability.
- Local manifest marks semantic route unavailable with the existing stable reason when local semantic remains unimplemented.
- Durable manifest marks semantic route unavailable with the existing stable reason when the search index is unavailable.
- Unsupported durable route cases have matching unavailable capability entries when the manifest exposes that route family.

### Auth Error Cases

- Missing auth on local filesystem mutation fails with the current stable auth status/body. First implementation task must lock the exact current status and body in a red test before changing anything.
- Missing auth on durable filesystem route fails with the current stable auth status/body.
- Workspace bearer without the required workspace/repo context fails closed and never falls back to `RepoId::local`.
- Cross-repo or mismatched workspace bearer setup fails closed with a stable error shape.
- Capability manifest remains public and must not require auth.

### Idempotency Replay Cases

- Local-state filesystem mutation:
  - `PUT /fs/conformance/idempotent.txt` with `Authorization: User root`, same `Idempotency-Key`, same body returns the same response and `x-stratum-idempotent-replay: true` on replay.
  - Same key with a different body returns `409` and the stable idempotency conflict error.
- Durable-cloud mounted session filesystem mutation:
  - Use the existing test setup pattern for workspace bearer/session-backed durable filesystem writes.
  - Same `Idempotency-Key` and same body replays with `x-stratum-idempotent-replay: true`.
  - Same key with a different body returns `409`.
- If the durable-cloud in-process setup cannot exercise mutation without new production behavior, scaffold the test as an explicit `#[ignore]` advisory case and document the missing dependency in the fixture. Do not silently skip it in default local conformance output.

### Unsupported Durable Route Cases

- Durable `/workspaces` returns `501` with the stable unsupported JSON.
- Durable `/audit` returns `501` with the stable unsupported JSON.
- Durable `/execute` or `/runs` returns `501` with the stable unsupported JSON.
- Durable `/vcs/recovery` returns `501` with the stable unsupported JSON if currently covered by `durable_unsupported_routes`.
- These tests must verify that unsupported route bodies do not include internal implementation details.

### SDK Parity Cases

- TypeScript SDK consumes the fixture and asserts route path, method, auth behavior, query encoding, idempotency header behavior, and public method mapping for covered APIs.
- Python SDK consumes the same fixture through `importlib.resources` or a direct test fixture path and asserts the same route/method/auth behavior.
- Bash SDK consumes the same fixture from tests and asserts generated commands map to the same route metadata.
- Rust client tests cover the client methods used by `stratumctl`. Since `stratumctl` has no public `capabilities` command today, do not add one in this slice.
- CLI impact must be documented: conformance covers CLI indirectly through `stratum::client::StratumClient` route/auth tests plus existing parser/command tests.

## Implementation Tasks

### Task 1: Add The Conformance Fixture Schema

**Files:**
- Create: `src/server/conformance.rs`
- Modify: `src/server/mod.rs`
- Create: `sdk/contracts/conformance.routes.v1.json`

**Step 1: Add a failing schema validation test**

Add `#[cfg(test)] pub(crate) mod tests` in `src/server/conformance.rs` with serde structs for the fixture. Keep these structs test-only and private to the server conformance module.

The first test should load `sdk/contracts/conformance.routes.v1.json`, deserialize it, and assert:

- `version == 1`
- `revision == "2026-06-04-1"`
- both `local-state` and `durable-cloud` modes are present
- every case has a non-empty `id`
- every case has one of the allowed auth profiles
- every case has an SDK coverage object
- `forbidden_substrings` is non-empty

**Step 2: Run it red**

Run:

```bash
cargo test --locked server::conformance --lib -- --nocapture
```

Expected: FAIL because `src/server/conformance.rs` or `sdk/contracts/conformance.routes.v1.json` is missing.

**Step 3: Add minimal fixture and module registration**

In `src/server/mod.rs`, add:

```rust
#[cfg(test)]
mod conformance;
```

Create the JSON fixture with the shape in "Fixture Contract" and one capabilities case for each mode.

**Step 4: Run it green**

Run:

```bash
cargo test --locked server::conformance --lib -- --nocapture
```

Expected: PASS for schema validation only.

**Step 5: Commit**

```bash
git add src/server/mod.rs src/server/conformance.rs sdk/contracts/conformance.routes.v1.json
git commit -m "test: add conformance fixture schema"
```

### Task 2: Build Rust Fixture Redaction And Stability Checks

**Files:**
- Modify: `src/server/conformance.rs`
- Modify: `sdk/contracts/conformance.routes.v1.json`

**Step 1: Add failing redaction tests**

Add tests that serialize the entire parsed fixture and assert none of `forbidden_substrings` appear. Add explicit checks for:

- `postgres://`
- `postgresql://`
- `Bearer `
- `STRATUM_`
- `SQLSTATE`
- absolute home-directory paths
- `/tmp/`

Add a stability test that rejects fields named `duration_ms`, `elapsed`, `timestamp`, `object_key`, `db_url`, `raw_secret`, or `provider_error`.

**Step 2: Run it red if fixture currently violates rules**

Run:

```bash
cargo test --locked server::conformance::tests::fixture --lib -- --nocapture
```

Expected: PASS if the minimal fixture is already clean; otherwise FAIL with the violating substring or field name.

**Step 3: Fix fixture fields**

Remove unstable or sensitive values from the fixture. Use symbolic labels such as `workspace-bearer`, `root-user`, and `durable-mounted-session`.

**Step 4: Run it green**

Run:

```bash
cargo test --locked server::conformance --lib -- --nocapture
```

Expected: PASS.

**Step 5: Commit**

```bash
git add src/server/conformance.rs sdk/contracts/conformance.routes.v1.json
git commit -m "test: guard conformance fixture redaction"
```

### Task 3: Add Capability Manifest Parity Tests

**Files:**
- Modify: `src/server/conformance.rs`
- Read: `src/server/routes_capabilities.rs`
- Read: `sdk/contracts/capabilities.v1.json`
- Read: `sdk/contracts/capabilities.v1.durable-cloud.json`
- Modify: `sdk/contracts/conformance.routes.v1.json`

**Step 1: Add failing server parity tests**

Add conformance tests that call the existing capability route builders for local and durable modes and compare selected fields against the fixture:

- schema version
- revision
- `state_mode`
- `routes.capabilities.self`
- semantic route availability/reason
- route idempotency metadata for filesystem write/copy/move/delete routes
- unsupported durable route family availability where present in the manifest

Use selected JSON path assertions or typed serde structs. Do not compare entire manifest bodies unless the manifest is already deterministic and intentionally frozen.

**Step 2: Run it red**

Run:

```bash
cargo test --locked server::conformance::tests::capabilities --lib -- --nocapture
```

Expected: FAIL until fixture cases and expected manifest fields are complete.

**Step 3: Add fixture cases**

Add cases:

- `capabilities.local.public`
- `capabilities.durable.public`
- `capabilities.local.semantic-unavailable`
- `capabilities.durable.semantic-unavailable`
- `capabilities.manifest.fs-idempotency`

**Step 4: Run it green**

Run:

```bash
cargo test --locked server::conformance::tests::capabilities --lib -- --nocapture
```

Expected: PASS.

**Step 5: Run existing route capability tests**

Run:

```bash
cargo test --locked server::routes_capabilities --lib -- --nocapture
```

Expected: PASS.

**Step 6: Commit**

```bash
git add src/server/conformance.rs sdk/contracts/conformance.routes.v1.json
git commit -m "test: cover capability manifest conformance"
```

### Task 4: Add Auth Error Conformance Cases

**Files:**
- Modify: `src/server/conformance.rs`
- Read: `src/server/routes_fs.rs`
- Read: `src/server/routes_workspace.rs`
- Modify: `sdk/contracts/conformance.routes.v1.json`

**Step 1: Lock current auth failures with red tests**

Write tests that exercise current local and durable routers without changing behavior:

- missing auth on local filesystem mutation
- missing auth on durable filesystem route
- workspace bearer without required workspace/repo context
- mismatched workspace bearer/repo setup

The first run should print the exact current status/body. Convert those observations into stable expectations in the fixture. If a current error includes unstable internals, add the smallest route-layer redaction fix in this task and document it in the commit.

**Step 2: Run focused tests**

Run:

```bash
cargo test --locked server::conformance::tests::auth --lib -- --nocapture
```

Expected: FAIL until exact status/body expectations are captured in the fixture or route redaction is fixed.

**Step 3: Add fixture cases**

Add cases:

- `auth.local.fs-mutation.missing`
- `auth.durable.fs-route.missing`
- `auth.durable.workspace-context.missing`
- `auth.durable.workspace-context.mismatch`

Each case must assert:

- stable status
- stable error shape
- no fallback to local repo behavior
- no forbidden substring in response body

**Step 4: Run it green**

Run:

```bash
cargo test --locked server::conformance::tests::auth --lib -- --nocapture
```

Expected: PASS.

**Step 5: Run related existing tests**

Run:

```bash
cargo test --locked server::routes_fs --lib -- --nocapture
```

Expected: PASS.

**Step 6: Commit**

```bash
git add src/server/conformance.rs sdk/contracts/conformance.routes.v1.json
git commit -m "test: cover conformance auth failures"
```

### Task 5: Add Idempotency Replay Conformance Cases

**Files:**
- Modify: `src/server/conformance.rs`
- Read: `src/server/idempotency.rs`
- Read: `src/server/routes_fs.rs`
- Modify: `sdk/contracts/conformance.routes.v1.json`

**Step 1: Add local-state idempotency tests**

Exercise a local filesystem write route through the Axum service:

- request 1: `PUT /fs/conformance/idempotent.txt`
- auth: symbolic `root-user`, resolved to the existing local test auth header
- idempotency key: `conformance-local-write-1`
- body class: text/plain fixture body
- expected status: current successful write status
- replay: same key and same body returns the same stable body shape plus `x-stratum-idempotent-replay: true`
- conflict: same key and different body returns `409` and stable conflict JSON/text

Do not store raw body content in the fixture.

**Step 2: Run local idempotency tests red**

Run:

```bash
cargo test --locked server::conformance::tests::idempotency_local --lib -- --nocapture
```

Expected: FAIL until the fixture case and harness are complete.

**Step 3: Add durable-cloud idempotency tests**

Use the existing durable route test setup for mounted session filesystem behavior. Keep it in-process and provider-free. Add the same replay/conflict assertions.

If durable mutation cannot run locally without production behavior, add an ignored advisory test:

```rust
#[ignore = "requires durable mounted-session mutation support in local conformance harness"]
```

The fixture must still include the case with `"default_gate": "advisory"` and the docs must explain why it is not a default CI gate yet.

**Step 4: Run focused tests**

Run:

```bash
cargo test --locked server::conformance::tests::idempotency --lib -- --nocapture
```

Expected: PASS for default local cases; ignored durable advisory is listed as ignored only if the durable harness genuinely cannot support it.

**Step 5: Run existing filesystem tests**

Run:

```bash
cargo test --locked server::routes_fs --lib -- --nocapture
```

Expected: PASS.

**Step 6: Commit**

```bash
git add src/server/conformance.rs sdk/contracts/conformance.routes.v1.json
git commit -m "test: cover idempotency conformance"
```

### Task 6: Add Unsupported Durable Route Cases

**Files:**
- Modify: `src/server/conformance.rs`
- Read: `src/server/mod.rs`
- Modify: `sdk/contracts/conformance.routes.v1.json`

**Step 1: Add failing unsupported route tests**

Call the durable router for:

- `/workspaces`
- `/audit`
- `/execute` or `/runs`
- `/vcs/recovery`

Assert status `501` and exact stable unsupported JSON:

```json
{"error":"stratum: operation not supported: durable-cloud route is not supported yet"}
```

Also assert the response body does not contain forbidden substrings.

**Step 2: Run it red**

Run:

```bash
cargo test --locked server::conformance::tests::unsupported_durable --lib -- --nocapture
```

Expected: FAIL until all fixture cases are added or any route path mismatch is corrected.

**Step 3: Add fixture cases**

Add cases:

- `unsupported.durable.workspaces`
- `unsupported.durable.audit`
- `unsupported.durable.execute`
- `unsupported.durable.vcs-recovery`

If any path is not currently routed through `durable_unsupported_routes`, record the actual current behavior in the fixture and note the gap in docs. Do not invent route support.

**Step 4: Run it green**

Run:

```bash
cargo test --locked server::conformance::tests::unsupported_durable --lib -- --nocapture
```

Expected: PASS.

**Step 5: Commit**

```bash
git add src/server/conformance.rs sdk/contracts/conformance.routes.v1.json
git commit -m "test: cover unsupported durable routes"
```

### Task 7: Add TypeScript SDK Conformance Parity

**Files:**
- Create: `sdk/typescript/tests/conformance.test.ts`
- Read: `sdk/typescript/src/client.ts`
- Read: `sdk/typescript/src/types.ts`
- Read: `sdk/contracts/conformance.routes.v1.json`

**Step 1: Add failing fixture loader test**

Load `../contracts/conformance.routes.v1.json` from the test and assert the revision, modes, and case IDs.

Run:

```bash
bun test --cwd sdk/typescript tests/conformance.test.ts
```

Expected: FAIL until loader path/types are correct.

**Step 2: Add mocked fetch route assertions**

For fixture cases with `sdk.typescript` set, use mock fetch to assert:

- method
- path
- query encoding
- auth header behavior
- idempotency header behavior for mutation APIs that accept idempotency keys
- returned capability body typing for `getCapabilities`

Do not change public method names or return shapes unless an existing SDK method cannot represent the server route. If that happens, stop and document the mismatch before changing code.

**Step 3: Run focused test**

Run:

```bash
bun test --cwd sdk/typescript tests/conformance.test.ts
```

Expected: PASS.

**Step 4: Run SDK checks**

Run:

```bash
bun run --cwd sdk typecheck
bun run --cwd sdk test:run
```

Expected: PASS.

**Step 5: Commit**

```bash
git add sdk/typescript/tests/conformance.test.ts
git commit -m "test: add typescript sdk conformance parity"
```

### Task 8: Add Python SDK Conformance Parity

**Files:**
- Create: `sdk/python/tests/test_conformance.py`
- Read: `sdk/python/src/stratum/client.py`
- Read: `sdk/python/src/stratum/types.py`
- Read: `sdk/contracts/conformance.routes.v1.json`

**Step 1: Add failing fixture loader test**

Use `pathlib.Path(__file__).resolve()` to locate `sdk/contracts/conformance.routes.v1.json` without depending on the current working directory.

Run:

```bash
cd sdk/python && pytest tests/test_conformance.py -q
```

Expected: FAIL until the fixture path and model helper are correct.

**Step 2: Add mocked transport assertions**

Using the existing Python SDK test style, assert fixture-backed cases for:

- `get_capabilities`
- filesystem operations that map to fixture mutation cases
- semantic search route metadata when exposed by the client
- VCS route metadata when exposed by the client
- workspace token/auth behavior when exposed by the client

Do not add public SDK methods only for conformance. If the Python SDK lacks a public method for a fixture case, mark `sdk.python` as `null` and document the gap.

**Step 3: Run focused test**

Run:

```bash
cd sdk/python && pytest tests/test_conformance.py -q
```

Expected: PASS.

**Step 4: Run Python SDK checks**

Run:

```bash
cd sdk/python && pytest
cd sdk/python && mypy src
cd sdk/python && ruff check src tests
```

Expected: PASS or clearly documented unavailable tooling if dependencies are not installed in the local environment.

**Step 5: Commit**

```bash
git add sdk/python/tests/test_conformance.py
git commit -m "test: add python sdk conformance parity"
```

### Task 9: Add Bash SDK And Rust Client/CLI Impact Coverage

**Files:**
- Create: `sdk/bash/tests/conformance.test.ts`
- Modify: `src/client/mod.rs`
- Read: `sdk/bash/src/index.ts`
- Read: `src/bin/stratumctl.rs`
- Read: `sdk/contracts/conformance.routes.v1.json`

**Step 1: Add Bash SDK fixture-backed tests**

Use the existing Bash SDK test harness to assert command builders map to fixture route cases:

- filesystem write/read operations
- semantic search command behavior, including unsupported `sgrep` behavior if it remains intentionally unavailable
- VCS operations if already exposed
- auth and idempotency header behavior where the Bash SDK exposes it

Run:

```bash
bun test --cwd sdk/bash tests/conformance.test.ts
```

Expected: FAIL until the fixture loader and assertions are wired.

**Step 2: Add Rust client route tests**

In `src/client/mod.rs`, add focused tests using the existing local Axum fake server pattern. Cover client methods used by `stratumctl` and represented in the fixture:

- path/method correctness
- auth header behavior
- workspace/repo header behavior
- stable `501` unsupported durable response handling where already covered

Do not add a `stratumctl capabilities` command in this slice.

**Step 3: Run focused tests**

Run:

```bash
bun test --cwd sdk/bash tests/conformance.test.ts
cargo test --locked client::tests --lib -- --nocapture
```

Expected: PASS.

**Step 4: Run aggregate SDK checks**

Run:

```bash
bun run --cwd sdk typecheck
bun run --cwd sdk test:run
```

Expected: PASS.

**Step 5: Commit**

```bash
git add sdk/bash/tests/conformance.test.ts src/client/mod.rs
git commit -m "test: add bash and client conformance parity"
```

### Task 10: Add Fixture Update Gate And Documentation

**Files:**
- Modify: `src/server/conformance.rs`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`
- Optional Modify: `docs/semantic-index.md`

**Step 1: Add explicit fixture update command**

Add a test helper that can regenerate `sdk/contracts/conformance.routes.v1.json` only when an explicit environment variable is set:

```bash
STRATUM_UPDATE_CONFORMANCE_FIXTURES=1 cargo test --locked server::conformance::tests::update_checked_in_conformance_fixture_when_requested --lib -- --nocapture
```

Without the env var, the test must compare generated fixture output against the checked-in fixture and fail with a clear message when stale.

**Step 2: Run stale-check test**

Run:

```bash
cargo test --locked server::conformance::tests::checked_in_fixture_matches_generated_output --lib -- --nocapture
```

Expected: PASS.

**Step 3: Document conformance workflow**

Update `docs/http-api-guide.md` with:

- fixture location
- local default command
- fixture update command
- durable live/advisory policy
- SDK parity command list

Update `docs/project-status.md` with Slice 24 status and verification notes.

If semantic route expectations are materially clarified, update `docs/semantic-index.md` with only the conformance-relevant note.

**Step 4: Run docs/diff checks**

Run:

```bash
git diff --check
```

Expected: PASS.

**Step 5: Commit**

```bash
git add src/server/conformance.rs docs/http-api-guide.md docs/project-status.md docs/semantic-index.md
git commit -m "docs: describe conformance fixture workflow"
```

### Task 11: Full Verification

**Files:**
- Read all modified files.

**Step 1: Check worktree state**

Run:

```bash
git status --short --branch
```

Expected: only intended Slice 24 implementation files are modified before final commit; clean after final commit.

**Step 2: Run Rust formatting**

Run:

```bash
cargo fmt --all -- --check
```

Expected: PASS.

**Step 3: Run focused conformance tests**

Run:

```bash
cargo test --locked server::conformance --lib -- --nocapture
cargo test --locked server::routes_capabilities --lib -- --nocapture
cargo test --locked server::routes_fs --lib -- --nocapture
cargo test --locked client::tests --lib -- --nocapture
```

Expected: PASS, except any explicitly ignored durable live/advisory test is listed as ignored with a documented reason.

**Step 4: Run broad Rust checks**

Run:

```bash
cargo test --locked --lib --tests
cargo check --locked
cargo check --locked --features postgres
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
```

Expected: PASS.

**Step 5: Run SDK checks**

Run:

```bash
bun run --cwd sdk typecheck
bun run --cwd sdk test:run
cd sdk/python && pytest
cd sdk/python && mypy src
cd sdk/python && ruff check src tests
```

Expected: PASS or record exact unavailable dependency/tooling with command output.

**Step 6: Run supply-chain checks**

Run:

```bash
cargo audit --deny warnings
cargo deny check
```

Expected: PASS if tools/config are available. If unavailable, record the exact missing tool/config message.

**Step 7: Run whitespace check**

Run:

```bash
git diff --check
```

Expected: PASS.

**Step 8: Final commit if needed**

If verification required fixes:

```bash
git add <changed files>
git commit -m "test: complete conformance scaffolding"
```

## Live Durable Gate Policy

Default conformance must be deterministic and local. Do not require live durable infrastructure in default CI.

Any future live durable check must require an explicit env var such as:

```bash
STRATUM_CONFORMANCE_LIVE_DURABLE=1
```

Live durable checks must be advisory unless the repo already has stable test infrastructure, credentials, cleanup, and isolation. Live checks must never print credentials, database URLs, object keys, provider errors, backing paths, or raw tokens.

## Rollback Plan

- If the fixture consumer causes SDK instability, keep the Rust server fixture and mark the affected SDK fixture mappings as `null` until the SDK is ready.
- If durable-cloud mutation cannot be made deterministic locally, keep local-state idempotency as the default gate and mark durable mutation as explicit advisory with a documented ignored test.
- If route behavior must change to satisfy conformance, keep the behavior change small, covered by a red test, and documented in the task commit.
- If broad verification uncovers unrelated failures, do not paper over them. Record the exact command and failure, keep Slice 24 commits focused, and leave unrelated work alone.

## Security And Redaction Checklist

Before final completion, verify:

- Fixture JSON does not contain bearer tokens, session tokens, DB URLs, object keys, absolute host paths, temp paths, provider errors, SQLSTATE messages, raw secrets, or `STRATUM_` env values.
- Error response assertions use stable public strings only.
- Auth failure tests prove missing/mismatched workspace context fails closed.
- Idempotency tests do not leak request body content into fixture snapshots.
- Live durable gates are opt-in and advisory.
- SDK tests use mocked transports and do not make network calls.
- Docs do not include local absolute paths.

## Review Prompts

### Conformance Architecture Reviewer

Review the Slice 24 conformance scaffolding architecture. Focus on `src/server/conformance.rs`, `sdk/contracts/conformance.routes.v1.json`, and docs. Check whether the fixture model is deterministic, expressive enough for local-state and durable-cloud modes, not overfit to implementation internals, and easy for SDK tests to consume. Flag unnecessary abstraction, missing cases, or fixture fields that will become unstable.

### Rust Server Correctness Reviewer

Review the Rust server conformance tests. Focus on route setup, auth profiles, idempotency replay/conflict assertions, durable unsupported routes, and capability manifest parity. Confirm the tests exercise real Axum route behavior instead of duplicating route metadata by hand. Flag false positives, skipped behavior hidden as success, and any accidental route behavior changes.

### SDK Parity Reviewer

Review TypeScript, Python, Bash, and Rust client parity tests against `sdk/contracts/conformance.routes.v1.json`. Confirm tests assert method/path/query/auth/idempotency behavior without changing public SDK shapes unnecessarily. Flag fixture cases that should be `null` for an SDK because no public method exists, and flag SDK methods that drift from documented server behavior.

### Security And Redaction Reviewer

Review all Slice 24 fixture output, tests, and docs for leaks. Search for bearer tokens, workspace secrets, DB URLs, object keys, local absolute paths, temp paths, provider errors, SQLSTATE messages, raw request bodies, env var values, and unstable IDs. Confirm auth failure cases fail closed and that live durable gates cannot run accidentally in default CI.

### Slop And Maintainability Reviewer

Review the Slice 24 changes for over-engineering, duplicated logic, vague assertions, weak test names, hand-wavy docs, and unnecessary public API changes. Prefer compact helpers, explicit fixture cases, and test names that say the behavior being locked. Flag any broad refactor that is not required for conformance scaffolding.
