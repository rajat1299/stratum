# @stratum/bash

Virtual bash SDK for Stratum workspaces.

`@stratum/bash` gives agents a bash-like shell backed by a Stratum workspace over HTTP workspace bearer auth. It is adapted from the SMFS virtual bash shape, but uses Stratum's native workspace paths, raw file APIs, directory semantics, idempotent mutations, search, and VCS endpoints through `@stratum/sdk`.

The package is now a shell adapter over the shared SDK in-process mount layer. `PathIndex`, `SessionCache`, and `StratumVolume` are re-exported from `@stratum/sdk`; bash-specific code owns `StratumFs`, custom commands, `just-bash` wiring, and error translation.

## Usage

```ts
import { createBash } from "@stratum/bash";

const { bash, refresh } = await createBash({
  baseUrl: "https://stratum.example/api/",
  workspaceId: process.env.STRATUM_WORKSPACE_ID!,
  workspaceToken: process.env.STRATUM_WORKSPACE_TOKEN!,
});

await refresh();

const result = await bash.exec("cat /docs/README.md && grep TODO /docs");
console.log(result.stdout);
```

## Shell Surface

The shell is rooted at `/`, which maps to the mounted Stratum workspace. Relative paths resolve against the shell cwd.

Supported through `just-bash` plus `StratumFs`:

- `cat`, `ls`, `pwd`
- `echo > file` and `echo >> file`
- `mkdir`, including recursive mkdir
- `rm`, `cp`, `mv`
- common text utilities provided by `just-bash`

Stratum-specific commands:

- `status`
- `diff [path]`
- `commit <message>`
- `grep <pattern> [path]`
- `sgrep` returns a clear unsupported message until Stratum has semantic search.

Unsupported filesystem APIs return `ENOSYS`-style errors for `chmod`, symlink creation, hard links, `readlink`, and `utimes`.

## Auth And Scope

Every HTTP request sends:

- `Authorization: Bearer <workspaceToken>`
- `X-Stratum-Workspace: <workspaceId>`

Filesystem, search, and tree paths are workspace-relative on the wire. The SDK exposes them as shell paths rooted at `/`.

VCS `status`, `diff`, and `commit` are exposed because the server has those endpoints. They may return `403` unless the workspace token maps to a session authorized for the current VCS operation.

## Current Boundaries

- This package does not execute host processes; it runs `just-bash` against Stratum-backed virtual filesystem calls.
- Semantic search is not implemented here.
- HTTP auth, route construction, response typing, retry idempotency, path indexing, session caching, and `StratumVolume` are provided by `@stratum/sdk`.
- The package is TypeScript-first and currently verified with Bun, TypeScript, and Vitest.

## Live smoke (optional)

`bun run test:run` stays mock-only. With `STRATUM_SDK_LIVE=1` and the same `STRATUM_SDK_LIVE_*` variables as `@stratum/sdk`, run:

```bash
cd sdk/bash
STRATUM_SDK_LIVE=1 STRATUM_SDK_LIVE_BASE_URL=http://127.0.0.1:3000 \
  STRATUM_SDK_LIVE_ADMIN_USER=root STRATUM_SDK_LIVE_AGENT_TOKEN="$STRATUM_TOKEN" \
  bun run test:live
```

Example script (no secret tokens on stdout):

```bash
cd sdk/bash
STRATUM_SDK_LIVE=1 STRATUM_SDK_LIVE_BASE_URL=http://127.0.0.1:3000 \
  STRATUM_SDK_LIVE_ADMIN_USER=root STRATUM_SDK_LIVE_AGENT_TOKEN="$STRATUM_TOKEN" \
  bun run examples/live-bash.ts
```

Mount coverage for `client.mount()` lives in the `@stratum/sdk` package; bash live smoke exercises both the shared volume and virtual shell commands (`sgrep` should remain explicitly unsupported).
