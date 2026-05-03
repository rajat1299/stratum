# SDK Live Smoke Harness Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add opt-in live smoke coverage and runnable examples proving the TypeScript SDK, TypeScript in-process mount, `@stratum/bash`, and Python SDK work against a real `stratum-server`.

**Architecture:** Do not change Rust server behavior in this slice. Add live tests that are skipped by default unless explicit environment variables are present; the tests target an already-running Stratum HTTP server and create their own isolated workspace path. Use admin/user auth plus an env-provided existing agent token to issue a workspace token, then exercise workspace-bearer filesystem/search/run-record flows through the SDKs. Global VCS routes remain admin-gated in the current server; live tests should either use admin/user auth for VCS status/diff or assert the workspace-token permission boundary.

**Tech Stack:** TypeScript, Bun, Vitest, `@stratum/sdk`, `@stratum/bash`, Python 3.11, pytest, `httpx`, current Stratum HTTP API.

---

## Constraints

- Do not edit Rust server behavior.
- Do not add command execution, job runner behavior, scheduler behavior, sandboxing, stdout streaming, or cancellation.
- Do not implement semantic search. Keep `search.semantic()` and bash `sgrep` explicitly unsupported.
- Do not require a live server for default unit tests or CI. Live tests must skip unless `STRATUM_SDK_LIVE=1` is set and required connection env vars are present.
- Do not persist or print raw workspace tokens beyond normal local process variables in tests/examples.
- Use unique workspace names/root paths per run so repeated smoke runs do not collide.
- Keep TypeScript package output ESM and existing package publishing shape unchanged.
- Keep Python package sync-only; no async client in this slice.

## Required Live Environment Contract

The live smoke harness should expect an already-running server:

```bash
export STRATUM_SDK_LIVE=1
export STRATUM_SDK_LIVE_BASE_URL="http://127.0.0.1:3000"
export STRATUM_SDK_LIVE_ADMIN_USER="root"
export STRATUM_SDK_LIVE_AGENT_TOKEN="<existing-agent-token>"
```

Reference docs for creating a server and agent token:

- `docs/getting-started.md`
- `docs/agent-workspace-demo.md`
- `docs/http-api-guide.md`

The harness should generate a unique workspace root such as `/sdk-smoke/<timestamp>-<random>` and should create all files under that root. It does not need to delete the workspace afterward; leaving smoke artifacts is acceptable because they are useful for inspection.

## Task 1: Add Shared TypeScript Live Smoke Helpers

**Files:**
- Create: `sdk/typescript/tests/live-helpers.ts`

**Behavior:**
- Provide `liveConfigOrSkip()` that returns:
  - `baseUrl`
  - `adminUser`
  - `agentToken`
  - `workspaceName`
  - `workspaceRoot`
- Skip with a clear message unless `STRATUM_SDK_LIVE=1` and all required env vars are present.
- Generate `workspaceName` and `workspaceRoot` with a timestamp plus random suffix.
- Provide `createLiveWorkspace(client, config)` that:
  - creates the workspace through `client.workspaces.create({ name, root_path })`
  - issues a workspace token with `client.workspaces.issueToken(workspace.id, { name, agent_token, read_prefixes: [workspaceRoot], write_prefixes: [workspaceRoot] })`
  - maps the issue-token response's `workspace_token` field to `workspaceToken`
  - returns `{ workspace, workspaceToken }`.

**Step 1: Write helper tests indirectly**

Do not write unit tests for the helper itself unless necessary. Its behavior will be covered by the live smoke test when enabled.

**Step 2: Implement helper**

Use only Node/Bun standard APIs and current SDK types. Do not introduce a dependency.

**Step 3: Commit**

```bash
git add sdk/typescript/tests/live-helpers.ts
git commit -m "test: add sdk live smoke helpers"
```

## Task 2: Add `@stratum/sdk` Live Smoke Test

**Files:**
- Create: `sdk/typescript/tests/live-smoke.test.ts`
- Modify: `sdk/typescript/package.json`

**Behavior:**
- Add script:

```json
"test:live": "vitest run tests/live-smoke.test.ts"
```

- Test must skip unless live env is enabled.
- Use admin `StratumClient` with `{ type: "user", username: adminUser }`.
- Create workspace and workspace token using the helper.
- Use workspace `StratumClient` with `{ type: "workspace", workspaceId: workspace.id, workspaceToken }`.
- Exercise through the workspace client:
  - `fs.mkdir("/docs")`
  - `fs.writeFile("/docs/README.md", "hello from live smoke")`
  - `fs.readFile("/docs/README.md")`
  - `fs.stat("/docs/README.md")`
  - `fs.listDirectory("/docs")`
  - `search.grep("live smoke", { path: "/docs", recursive: true })`
  - `search.find("README.md", { path: "/docs" })`
  - `search.tree("/")`
  - `runs.create(...)`, then `runs.get(...)`, `runs.stdout(...)`, `runs.stderr(...)`
- Exercise through the admin/user client:
  - `vcs.status()`
  - `vcs.diff("<workspace-root>/docs/README.md")`
- Assert returned paths are workspace-relative and do not leak the backing workspace root where the API promises projected paths.
- Assert `search.semantic("...")` still throws `UnsupportedFeatureError`.

**Step 1: Write the test**

Keep assertions concrete but not brittle about timestamps, commit hashes, or server-generated IDs.

**Step 2: Run default unit tests**

```bash
cd sdk/typescript
bun run test:run
```

Expected: existing tests pass; live smoke test is skipped unless env is enabled.

**Step 3: Run live test manually when a server is available**

```bash
cd sdk/typescript
STRATUM_SDK_LIVE=1 \
STRATUM_SDK_LIVE_BASE_URL=http://127.0.0.1:3000 \
STRATUM_SDK_LIVE_ADMIN_USER=root \
STRATUM_SDK_LIVE_AGENT_TOKEN="$STRATUM_TOKEN" \
bun run test:live
```

Expected: live smoke test passes against a configured server.

**Step 4: Commit**

```bash
git add sdk/typescript/package.json sdk/typescript/tests/live-smoke.test.ts
git commit -m "test: add typescript sdk live smoke"
```

## Task 3: Add TypeScript Mount And Bash Live Smoke Coverage

**Files:**
- Create: `sdk/bash/tests/live-smoke.test.ts`
- Modify: `sdk/bash/package.json`
- Modify: `sdk/typescript/tests/live-smoke.test.ts` if the mount assertions fit better there

**Behavior:**
- Add bash script:

```json
"test:live": "vitest run tests/live-smoke.test.ts"
```

- Reuse or copy the TypeScript live helper carefully. Prefer importing helper code from `sdk/typescript/tests/live-helpers.ts` only if Vitest/TypeScript resolution stays simple; otherwise create a tiny bash-local helper to avoid test resolution churn.
- For `client.mount()` coverage:
  - create a mounted volume with `const volume = client.mount({ cwd: "/" })`
  - write, read, `cd`, `ls`, `grep`, `find`, and `tree` through the volume.
  - exercise `status` and `diff` through the volume while accepting either authorized output or the current admin-gated permission boundary.
  - verify repeated reads hit the cache only where observable without depending on server internals; avoid fragile timing tests.
- For `@stratum/bash`:
  - call `createBash({ baseUrl, workspaceId, workspaceToken })`
  - `await refresh()`
  - run `pwd`
  - run `cat /docs/README.md`
  - run `grep live /docs`
  - run `status` and accept either authorized output or the current admin-gated permission boundary
  - run `diff /docs/README.md` and accept either authorized output or the current admin-gated permission boundary
  - run `sgrep anything` and assert it returns the unsupported boundary rather than silently succeeding.

**Step 1: Write live bash/mount tests**

Keep all paths under the live workspace root and avoid deleting root paths.

**Step 2: Run default bash tests**

```bash
cd sdk/bash
bun run test:run
```

Expected: existing tests pass; live smoke test skips unless env is enabled.

**Step 3: Run live bash test manually when a server is available**

```bash
cd sdk/bash
STRATUM_SDK_LIVE=1 \
STRATUM_SDK_LIVE_BASE_URL=http://127.0.0.1:3000 \
STRATUM_SDK_LIVE_ADMIN_USER=root \
STRATUM_SDK_LIVE_AGENT_TOKEN="$STRATUM_TOKEN" \
bun run test:live
```

Expected: live bash and mount smoke test passes.

**Step 4: Commit**

```bash
git add sdk/bash/package.json sdk/bash/tests/live-smoke.test.ts sdk/typescript/tests/live-smoke.test.ts
git commit -m "test: add bash and mount live smoke"
```

## Task 4: Add Python SDK Live Smoke Test

**Files:**
- Create: `sdk/python/tests/test_live_smoke.py`
- Modify: `sdk/python/pyproject.toml` only if needed for test markers

**Behavior:**
- Pytest test must skip unless `STRATUM_SDK_LIVE=1` and required env vars exist.
- Use `StratumClient(base_url=..., auth=UserAuth(username=admin_user))` for admin calls.
- Create workspace and issue workspace token using env `STRATUM_SDK_LIVE_AGENT_TOKEN`.
- Use workspace auth for file/search/run smoke:
  - write/read/list/stat
  - grep/find/tree
  - runs create/get/stdout/stderr
  - semantic unsupported boundary
- Use admin/user auth for global VCS smoke:
  - status/diff
- Use a context manager or explicit close for the HTTP client so no connections leak.

**Step 1: Write Python live test**

Follow current Python SDK style in `sdk/python/tests/test_client.py`.

**Step 2: Run default Python tests**

```bash
cd sdk/python
python -m pytest
```

Expected: existing tests pass; live smoke test skips unless env is enabled.

**Step 3: Run live Python test manually when a server is available**

```bash
cd sdk/python
STRATUM_SDK_LIVE=1 \
STRATUM_SDK_LIVE_BASE_URL=http://127.0.0.1:3000 \
STRATUM_SDK_LIVE_ADMIN_USER=root \
STRATUM_SDK_LIVE_AGENT_TOKEN="$STRATUM_TOKEN" \
python -m pytest tests/test_live_smoke.py -q
```

Expected: live Python smoke test passes.

**Step 4: Commit**

```bash
git add sdk/python/tests/test_live_smoke.py sdk/python/pyproject.toml
git commit -m "test: add python sdk live smoke"
```

## Task 5: Add Runnable SDK Examples And Docs

**Files:**
- Create: `sdk/typescript/examples/live-workspace.ts`
- Create: `sdk/bash/examples/live-bash.ts`
- Create: `sdk/python/examples/live_workspace.py`
- Modify: `sdk/typescript/README.md`
- Modify: `sdk/bash/README.md`
- Modify: `sdk/python/README.md`
- Modify: `docs/getting-started.md`
- Modify: `docs/project-status.md`

**Behavior:**
- Examples should be short, copyable, and use the same env vars as the live smoke tests.
- Examples should:
  - create an admin client
  - create workspace + workspace token
  - write/read a file
  - show mount or bash usage where relevant
  - print only non-secret IDs/paths/status summaries
- Docs should explain:
  - live tests are opt-in
  - how to start `stratum-server`
  - how to provide an existing agent token
  - how to run TS, bash, and Python live smoke commands
  - semantic search and command execution are intentionally out of scope
- `docs/project-status.md` should record this as completed only after verification passes.

**Step 1: Write examples**

Prefer direct imports from package source for repo-local examples if that is how they are meant to be run before publishing.

**Step 2: Update docs**

Do not remove backend status. Add a concise SDK live smoke section.

**Step 3: Run docs/examples basic checks**

```bash
cd sdk/typescript
bun run typecheck
cd ../bash
bun run typecheck
cd ../python
python -m mypy src/stratum_sdk
```

Expected: type checks pass.

**Step 4: Commit**

```bash
git add sdk/typescript/examples sdk/bash/examples sdk/python/examples sdk/typescript/README.md sdk/bash/README.md sdk/python/README.md docs/getting-started.md docs/project-status.md
git commit -m "docs: add sdk live smoke examples"
```

## Task 6: Final Verification

Run before handing back for review:

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

If a live server and token are available, also run:

```bash
cd sdk/typescript
STRATUM_SDK_LIVE=1 STRATUM_SDK_LIVE_BASE_URL=http://127.0.0.1:3000 STRATUM_SDK_LIVE_ADMIN_USER=root STRATUM_SDK_LIVE_AGENT_TOKEN="$STRATUM_TOKEN" bun run test:live

cd ../bash
STRATUM_SDK_LIVE=1 STRATUM_SDK_LIVE_BASE_URL=http://127.0.0.1:3000 STRATUM_SDK_LIVE_ADMIN_USER=root STRATUM_SDK_LIVE_AGENT_TOKEN="$STRATUM_TOKEN" bun run test:live

cd ../python
STRATUM_SDK_LIVE=1 STRATUM_SDK_LIVE_BASE_URL=http://127.0.0.1:3000 STRATUM_SDK_LIVE_ADMIN_USER=root STRATUM_SDK_LIVE_AGENT_TOKEN="$STRATUM_TOKEN" python -m pytest tests/test_live_smoke.py -q
```

If live verification cannot be run locally, state that explicitly in the handoff and include the default verification output.

## Manager-To-Implementer Handoff Message

You are implementing the SDK live smoke harness for Stratum. Work only from `docs/plans/2026-05-03-sdk-live-smoke-harness.md`, execute it task by task, and keep commits small. This is a verification/DX slice, not a backend feature slice: do not edit Rust server behavior, do not implement semantic search, do not add command execution, and do not make default tests require a live server.

The live tests should be opt-in behind `STRATUM_SDK_LIVE=1` and should target an already-running `stratum-server` via `STRATUM_SDK_LIVE_BASE_URL`, `STRATUM_SDK_LIVE_ADMIN_USER`, and `STRATUM_SDK_LIVE_AGENT_TOKEN`. Use the admin user to create a unique workspace, use the env-provided agent token only to issue a workspace token, then exercise TypeScript SDK, `client.mount()`, `@stratum/bash`, and Python SDK flows through workspace bearer auth. Keep raw tokens out of logs and docs output.

Use current code patterns in `sdk/typescript/tests`, `sdk/bash/tests`, and `sdk/python/tests`. Default `bun run test:run` and `python -m pytest` must pass without a server by skipping live tests. Update `docs/project-status.md` only after the slice is verified. Before handing back, run the final verification block in the plan and clearly say whether live smoke was actually run or skipped due to missing server/token.
