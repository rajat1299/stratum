# TypeScript In-Process Mount Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Promote the virtual filesystem primitives currently inside `@stratum/bash` into `@stratum/sdk` so TypeScript agents can mount a Stratum workspace in-process without the shell wrapper or FUSE.

**Architecture:** Keep the Rust server unchanged. Move cwd-aware path normalization, path indexing, session caching, and `StratumVolume` into `sdk/typescript` as the SDK mount layer. Add a `StratumClient.mount()` convenience method returning the shared volume. Refactor `@stratum/bash` to import/re-export those primitives from `@stratum/sdk`, preserving its existing public API while removing duplicate ownership of mount logic.

**Tech Stack:** TypeScript, Bun, Vitest, existing Stratum HTTP API, existing `@stratum/bash` virtual filesystem code adapted from the SMFS reference repo.

---

## Constraints

- Do not edit Rust server behavior in this slice.
- Do not implement semantic search or `sgrep`; keep that unsupported until the backend derived index lands.
- Keep `@stratum/bash` source-compatible for existing imports of `PathIndex`, `SessionCache`, and `StratumVolume`.
- Keep binary read/write cache behavior defensive: callers must not be able to mutate cached `Uint8Array` content by retaining a previous reference.
- Preserve safe path normalization and root stat synthesis, because the HTTP root filesystem route returns directory listings rather than stat JSON.
- Keep package output ESM with declarations and source maps.

## Task 1: Add SDK Mount Primitives

**Files:**
- Create: `sdk/typescript/src/mount-paths.ts`
- Create: `sdk/typescript/src/mount-cache.ts`
- Create: `sdk/typescript/src/mount.ts`
- Modify: `sdk/typescript/src/index.ts`
- Modify: `sdk/typescript/src/client.ts`

**Behavior:**
- Export `normalizeMountPath`, `toClientPath`, `dirname`, `PathIndex`, `SessionCache`, and `StratumVolume` from `@stratum/sdk`.
- Provide `normalizePath` as a compatibility alias for mount paths only if needed by downstream bash shims.
- Add `StratumClient.mount(options?)` as the ergonomic SDK entry point for a workspace-backed in-process volume.
- Preserve existing `StratumClient` compatibility methods used by `StratumVolume`.

**Tests:**
- Port path-index, session-cache, and volume coverage from `sdk/bash/tests` into `sdk/typescript/tests`.
- Add coverage for `StratumClient.mount()` returning a volume that delegates through the SDK client with cwd-aware path normalization.

## Task 2: Refactor Bash To Consume The SDK Mount Layer

**Files:**
- Modify: `sdk/bash/src/path-index.ts`
- Modify: `sdk/bash/src/session-cache.ts`
- Modify: `sdk/bash/src/volume.ts`
- Modify: `sdk/bash/src/index.ts`
- Modify: `sdk/bash/src/types.ts`
- Modify: `sdk/bash/src/create-bash.ts` if import cleanup is useful

**Behavior:**
- Bash keeps its public API shape by re-exporting mount primitives from `@stratum/sdk`.
- `StratumFs`, commands, and tests continue to import the old bash-local module paths, but those modules no longer own the implementation.
- `createBash()` still constructs a `StratumClient` with `idempotencyKeyPrefix: "stratum-bash"` and wraps the shared `StratumVolume`.

**Tests:**
- Existing bash tests remain green without requiring a live Stratum server.
- Bash package typecheck confirms the shim exports preserve downstream types.

## Task 3: Docs, Status, And Packaging

**Files:**
- Modify: `sdk/typescript/README.md`
- Modify: `sdk/bash/README.md`
- Modify: `docs/project-status.md`

**Behavior:**
- Document the SDK mount as an in-process workspace abstraction for agents that cannot or should not use FUSE.
- Clarify that `@stratum/bash` is now a shell adapter over the SDK mount layer.
- Record completed scope and remaining boundaries in the living status file.

## Verification

Run before merge:

```bash
cd sdk
bun install --frozen-lockfile
bun run typecheck
bun run test:run
bun run build

cd typescript
npm pack --dry-run

cd ../bash
npm pack --dry-run

cd ../..
cargo test --locked --no-run
git diff --check
```
