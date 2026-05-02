# TypeScript Virtual Bash SDK Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a first `@stratum/bash` TypeScript package that gives AI agents a persistent bash-like tool over Stratum HTTP workspace bearer sessions.

**Architecture:** Add a standalone package under `sdk/bash` so the SDK can evolve without disturbing the Rust runtime. The package adapts the proven SMFS virtual bash pattern, but uses Stratum HTTP APIs, workspace-token auth, unrestricted Stratum file names, idempotency keys for writes, and Stratum VCS commands.

**Tech Stack:** TypeScript 5, Vitest, Bun package scripts, `just-bash`, Stratum HTTP API.

---

## Context

Reference implementation lives in `/Users/rajattiwari/virtualfilesystem/smfs/bash/src`.

Useful files:

- `create-bash.ts`
- `supermemory-fs.ts`
- `volume.ts`
- `path-index.ts`
- `session-cache.ts`
- `tool-description.ts`
- `commands/sgrep.ts`

Important differences:

- Do not copy SMFS filepath validation requiring file extensions. Stratum supports regular file names by default.
- Do not copy the Supermemory document model. Stratum has real directories, raw file content, workspace scopes, VCS, idempotency, and audit.
- Use workspace bearer headers on every request: `Authorization: Bearer <workspaceToken>` and `X-Stratum-Workspace: <workspaceId>`.
- All SDK-visible paths are workspace-relative, matching Stratum HTTP behavior for mounted workspace sessions.
- Semantic search is not built yet. Reserve `sgrep` as a clear unsupported command or alias only after the search contract exists.

## Task 1: Package Scaffold And Basic Exports

**Files:**

- Create: `sdk/bash/package.json`
- Create: `sdk/bash/tsconfig.json`
- Create: `sdk/bash/vitest.config.ts`
- Create: `sdk/bash/src/index.ts`
- Create: `sdk/bash/src/create-bash.ts`
- Create: `sdk/bash/src/tool-description.ts`
- Create: `sdk/bash/src/types.ts`
- Create: `sdk/bash/tests/create-bash.test.ts`

**Step 1: Write failing tests**

Add tests proving:

- `createBash` is exported.
- `TOOL_DESCRIPTION` is exported and mentions Stratum. Task 1 can keep this as a minimal stub; Task 4 expands the agent-facing command guidance.
- options require `baseUrl`, `workspaceId`, and `workspaceToken`.

Run:

```bash
cd sdk/bash
bun install
bun run test:run tests/create-bash.test.ts
```

Expected: fail because files/exports do not exist.

**Step 2: Implement minimal package scaffold**

Create package scripts:

- `typecheck`: `tsc --noEmit`
- `test:run`: `vitest run`
- `build`: `tsc`

Dependencies:

- `just-bash`

Dev dependencies:

- `typescript`
- `vitest`
- `@types/node`

Export package root from `src/index.ts`.

**Step 3: Verify**

Run:

```bash
cd sdk/bash
bun run typecheck
bun run test:run tests/create-bash.test.ts
```

Expected: pass.

## Task 2: Stratum HTTP Client

**Files:**

- Create: `sdk/bash/src/client.ts`
- Create: `sdk/bash/tests/client.test.ts`
- Modify: `sdk/bash/src/index.ts`

**Step 1: Write failing tests**

Use a fake `fetch` function. Test:

- auth headers are sent.
- `readFile` calls `GET /fs/<path>`.
- `writeFile` calls `PUT /fs/<path>` with an `Idempotency-Key`.
- `listDirectory` parses `/fs/<path>` JSON.
- non-2xx responses throw `StratumHttpError` with status and body.

Run:

```bash
cd sdk/bash
bun run test:run tests/client.test.ts
```

Expected: fail because client does not exist.

**Step 2: Implement client**

Implement a small `StratumClient` with:

- `readFile(path): Promise<string>`
- `writeFile(path, content, opts?): Promise<unknown>`
- `listDirectory(path): Promise<StratumDirectoryListing>`
- `stat(path): Promise<StratumStat>`
- `deletePath(path, recursive?)`
- `copyPath(source, destination)`
- `movePath(source, destination)`
- `grep(pattern, path?)`
- `find(name, path?)`
- `tree(path?)`
- `status()`
- `diff(path?)`
- `commit(message)`

Use `globalThis.fetch` by default and allow `fetch?: typeof fetch` injection.

**Step 3: Verify**

Run:

```bash
cd sdk/bash
bun run typecheck
bun run test:run tests/client.test.ts
```

Expected: pass.

## Task 3: Path Index, Session Cache, And Volume Layer

**Files:**

- Create: `sdk/bash/src/path-index.ts`
- Create: `sdk/bash/src/session-cache.ts`
- Create: `sdk/bash/src/volume.ts`
- Create: `sdk/bash/tests/path-index.test.ts`
- Create: `sdk/bash/tests/session-cache.test.ts`
- Create: `sdk/bash/tests/volume.test.ts`

**Step 1: Write failing tests**

Cover:

- path normalization preserves Stratum unrestricted names.
- path index tracks files and directories from list responses.
- session cache honors TTL and max bytes.
- volume reads through cache after write.
- volume maps directory listing, reads, writes, deletes, moves, copies, grep/find/tree, status/diff/commit.

Run:

```bash
cd sdk/bash
bun run test:run tests/path-index.test.ts tests/session-cache.test.ts tests/volume.test.ts
```

Expected: fail because implementation does not exist.

**Step 2: Implement minimal layer**

Implement:

- `normalizePath(input, cwd?)`
- `PathIndex`
- `SessionCache`
- `StratumVolume`

Keep validation conservative:

- paths normalize to absolute workspace paths.
- reserved path can be added later; no extension requirement.
- directory/file collisions should produce POSIX-like errors at the FS layer.

**Step 3: Verify**

Run:

```bash
cd sdk/bash
bun run typecheck
bun run test:run tests/path-index.test.ts tests/session-cache.test.ts tests/volume.test.ts
```

Expected: pass.

## Task 4: Virtual Filesystem Adapter And Bash Wrapper

**Files:**

- Create: `sdk/bash/src/errors.ts`
- Create: `sdk/bash/src/stratum-fs.ts`
- Create: `sdk/bash/src/create-bash.ts`
- Modify: `sdk/bash/src/tool-description.ts`
- Create: `sdk/bash/tests/stratum-fs.test.ts`
- Create: `sdk/bash/tests/bash.test.ts`
- Modify: `sdk/bash/src/index.ts`

**Step 1: Write failing tests**

Cover:

- `cat`, `echo >`, append, `mkdir -p`, `rm`, `mv`, `cp`, `ls`.
- `grep` delegates literal search to Stratum.
- `status`, `diff`, and `commit` commands are available.
- unsupported symlink/chmod style APIs return clear errors if `just-bash` calls them.
- tool description tells agents to use Stratum workspace paths and VCS commands.

Run:

```bash
cd sdk/bash
bun run test:run tests/stratum-fs.test.ts tests/bash.test.ts
```

Expected: fail because adapter/wrapper do not exist.

**Step 2: Implement adapter**

Adapt SMFS `SupermemoryFs` structure into `StratumFs` implementing the `just-bash` filesystem interface.

Implement `createBash(options)` returning:

- `bash`
- `volume`
- `toolDescription`
- `refresh()`

Add custom commands:

- `status`
- `diff [PATH]`
- `commit MESSAGE`
- `sgrep` returning exit 2 and a clear message that semantic search is not available yet.

**Step 3: Verify**

Run:

```bash
cd sdk/bash
bun run typecheck
bun run test:run tests/stratum-fs.test.ts tests/bash.test.ts
```

Expected: pass.

## Task 5: Docs, Status, And Full Verification

**Files:**

- Create: `sdk/bash/README.md`
- Modify: `docs/project-status.md`

**Step 1: Write/update docs**

Document:

- install from local package path
- `createBash` options
- workspace bearer auth requirements
- supported commands
- VCS commands
- idempotency behavior
- unsupported semantic search status

**Step 2: Run package verification**

Run:

```bash
cd sdk/bash
bun run typecheck
bun run test:run
bun run build
```

Expected: pass.

**Step 3: Run repo-level verification**

Run:

```bash
cargo test --locked --no-run
git diff --check
```

Expected: pass.

**Step 4: Commit**

Commit implementation and docs in purposeful commits:

```bash
git add sdk/bash docs/project-status.md docs/plans/2026-05-02-typescript-virtual-bash-sdk.md
git commit -m "feat: add typescript virtual bash sdk foundation"
```
