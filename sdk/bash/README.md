# @stratum/bash

Virtual bash SDK for Stratum workspaces.

`@stratum/bash` gives agents a bash-like shell backed by a Stratum workspace over HTTP workspace bearer auth. It is adapted from the SMFS virtual bash shape, but uses Stratum's native workspace paths, raw file APIs, directory semantics, idempotent mutations, search, and VCS endpoints through `@stratum/sdk`.

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
- HTTP auth, route construction, response typing, and retry idempotency are provided by `@stratum/sdk`.
- The package is TypeScript-first and currently verified with Bun, TypeScript, and Vitest.
