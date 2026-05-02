# TypeScript SDK Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add a reusable `@stratum/sdk` TypeScript package over the current Stratum HTTP API and refactor `@stratum/bash` to consume it.

**Architecture:** Keep the Rust server unchanged. Extract the current bash HTTP client, route normalization, auth headers, error handling, and public response types into `sdk/typescript`, then expose resource clients for filesystem, search, VCS, reviews, runs, and workspaces. `@stratum/bash` keeps the virtual filesystem, cache, volume, and `just-bash` command layer, but imports `StratumClient` and HTTP/API types from `@stratum/sdk`.

**Tech Stack:** TypeScript, Bun, Vitest, `just-bash`, current Stratum HTTP API from `docs/http-api-guide.md`.

---

## Constraints

- Do not edit Rust server behavior in this slice.
- Do not implement semantic search backend. Add a typed unsupported `search.semantic()`/`sgrep` boundary only.
- Preserve workspace bearer auth support for `@stratum/bash`.
- Keep route construction safe against dot-segment URL prefix escapes.
- Keep idempotency-key support on mutating methods.
- Keep package output ESM with declarations and source maps.

## Task 1: Scaffold `@stratum/sdk`

**Files:**
- Create: `sdk/typescript/package.json`
- Create: `sdk/typescript/tsconfig.json`
- Create: `sdk/typescript/vitest.config.ts`
- Create: `sdk/typescript/src/index.ts`
- Create: `sdk/typescript/src/errors.ts`
- Create: `sdk/typescript/src/paths.ts`
- Create: `sdk/typescript/src/http.ts`
- Create: `sdk/typescript/src/types.ts`
- Create: `sdk/typescript/tests/http.test.ts`
- Modify: `.gitignore`

**Behavior:**
- Package name is `@stratum/sdk`, version `0.0.0`.
- Export ESM from `dist/index.js` with declarations.
- Provide `StratumHttpError` and `UnsupportedFeatureError`.
- Provide safe path normalization helpers for filesystem, tree, and ref routes.
- Provide auth modes:
  - `{ type: "user", username }`
  - `{ type: "bearer", token }`
  - `{ type: "workspace", workspaceId, workspaceToken }`
- Also accept compatibility `workspaceId` plus `workspaceToken` options for the bash package.
- Provide idempotency-key generation with configurable prefix, defaulting to `stratum-sdk`.

**Tests:**
- Header generation for user, bearer, and workspace auth.
- No auth header when auth is omitted for health-check use.
- Dot-segment paths stay inside the intended route prefix.
- Generated idempotency keys are visible ASCII and prefixed.

**Verification:**

```bash
cd sdk/typescript
bun install --frozen-lockfile
bun run typecheck
bun run test:run
bun run build
```

## Task 2: Implement SDK Resource Clients

**Files:**
- Modify: `sdk/typescript/src/index.ts`
- Modify: `sdk/typescript/src/http.ts`
- Modify: `sdk/typescript/src/types.ts`
- Create: `sdk/typescript/src/client.ts`
- Create: `sdk/typescript/tests/client.test.ts`

**Behavior:**
- `new StratumClient(options)` exposes:
  - `client.fs`
  - `client.search`
  - `client.vcs`
  - `client.reviews`
  - `client.runs`
  - `client.workspaces`
- Keep compatibility methods on `StratumClient`:
  - `readFile`, `readFileBuffer`, `writeFile`, `mkdir`, `listDirectory`, `stat`, `deletePath`, `copyPath`, `movePath`
  - `grep`, `find`, `tree`
  - `status`, `diff`, `commit`
- Filesystem methods cover read bytes/text, write, mkdir, list, stat, metadata patch, delete, copy, and move.
- Search methods cover grep, find, tree, and `semantic()` throwing `UnsupportedFeatureError`.
- VCS methods cover commit, log, revert, status, diff, list/create/update refs.
- Reviews methods cover list/get/create change requests, approvals, reviewers, comments, dismiss approval, reject, and merge.
- Runs methods cover create, get, stdout, and stderr.
- Workspaces methods cover list, get, create, and issue token. Do not add idempotency support to token issuance.

**Tests:**
- Each resource builds the expected URL/method/body/headers for at least one representative call.
- Mutating methods include supplied idempotency keys.
- Auto idempotency is enabled for mutating fs/vcs/review/runs/workspace-create calls.
- Workspace token issuance rejects an `idempotencyKey` option at compile/API shape level by not accepting it.
- `search.semantic()` throws `UnsupportedFeatureError`.

**Verification:**

```bash
cd sdk/typescript
bun run typecheck
bun run test:run
bun run build
```

## Task 3: Refactor `@stratum/bash` To Consume `@stratum/sdk`

**Files:**
- Modify: `sdk/bash/package.json`
- Modify: `sdk/bash/tsconfig.json`
- Modify: `sdk/bash/vitest.config.ts`
- Modify: `sdk/bash/src/create-bash.ts`
- Modify: `sdk/bash/src/index.ts`
- Modify: `sdk/bash/src/types.ts`
- Modify: `sdk/bash/src/volume.ts`
- Modify: `sdk/bash/tests/client.test.ts` or move coverage to `sdk/typescript/tests/client.test.ts`
- Modify: `sdk/bash/tests/create-bash.test.ts`
- Modify: `sdk/bash/tests/volume.test.ts`

**Behavior:**
- `@stratum/bash` imports `StratumClient`, HTTP response types, and `StratumClientOptions` from `@stratum/sdk`.
- `createBash()` constructs `StratumClient` with `idempotencyKeyPrefix: "stratum-bash"` so bash-originated writes keep the bash-specific retry-key prefix.
- `@stratum/bash` re-exports SDK client/errors/types for compatibility where practical.
- Bash tests continue to run without requiring a published package by using Vitest alias and TypeScript paths to `../typescript/src/index.ts`.

**Tests:**
- Existing bash volume/fs/session/path tests remain green.
- Create-bash test proves `idempotencyKeyPrefix` is passed.
- Bash package no longer owns duplicate HTTP route construction tests; those live in `@stratum/sdk`.

**Verification:**

```bash
cd sdk/bash
bun install --frozen-lockfile
bun run typecheck
bun run test:run
bun run build
```

## Task 4: Docs, Status, And Packaging

**Files:**
- Create: `sdk/typescript/README.md`
- Modify: `sdk/bash/README.md`
- Modify: `docs/project-status.md`

**Behavior:**
- `sdk/typescript/README.md` shows workspace bearer usage, user/admin usage, fs/search/vcs/review/runs/workspace examples, and semantic-search boundary.
- `sdk/bash/README.md` explains that bash uses `@stratum/sdk` underneath.
- `docs/project-status.md` records the active TypeScript SDK foundation slice and remaining SDK/search boundaries.

**Verification:**

```bash
cd sdk/typescript
npm pack --dry-run
cd ../bash
npm pack --dry-run
cd ../..
git diff --check
```

## Final Verification

Run before merge:

```bash
cd sdk/typescript
bun run typecheck
bun run test:run
bun run build
npm pack --dry-run

cd ../bash
bun run typecheck
bun run test:run
bun run build
npm pack --dry-run

cd ../..
cargo test --locked --no-run
git diff --check
```

