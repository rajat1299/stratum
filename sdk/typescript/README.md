# @stratum/sdk

TypeScript SDK for the current Stratum HTTP API.

`@stratum/sdk` is the reusable client layer for applications, agents, CLIs, and higher-level packages such as `@stratum/bash`. It keeps Stratum's Rust server as the source of truth and wraps the implemented HTTP routes without inventing future backend behavior.

## Usage

Workspace bearer auth:

```ts
import { StratumClient } from "@stratum/sdk";

const client = new StratumClient({
  baseUrl: "https://stratum.example",
  workspaceId: process.env.STRATUM_WORKSPACE_ID!,
  workspaceToken: process.env.STRATUM_WORKSPACE_TOKEN!,
});

const readme = await client.fs.readFile("/docs/README.md");
await client.fs.writeFile("/runs/note.txt", "agent note", {
  idempotencyKey: "run-note-1",
});

const status = await client.vcs.status();
```

In-process mount:

```ts
const volume = client.mount({ cwd: "/" });

await volume.writeFile("/work/notes.txt", "agent note");
const notes = await volume.readFile("work/notes.txt");
const listing = await volume.listDirectory("/work");

await volume.cd("/work");
const matches = await volume.grep("TODO", ".", true);
```

The mount is a process-local workspace abstraction for agents and tools that cannot use FUSE. It provides cwd-aware paths, a path index, TTL/LRU session caching, root stat synthesis, binary-safe read/write caching, and filesystem/search/VCS helpers over the same HTTP client.

Admin/user auth:

```ts
const admin = new StratumClient({
  baseUrl: "http://127.0.0.1:3000",
  auth: { type: "user", username: "root" },
});

const workspace = await admin.workspaces.create({
  name: "incident-demo",
  root_path: "/incidents/checkout-latency",
});

const token = await admin.workspaces.issueToken(workspace.id, {
  name: "agent",
  agent_token: process.env.STRATUM_AGENT_TOKEN!,
  read_prefixes: ["/incidents/checkout-latency/read"],
  write_prefixes: ["/incidents/checkout-latency/work"],
});
```

## API Surface

- `client.fs`: read/write bytes and text, mkdir, list, stat, metadata patch, delete, copy, move.
- `client.search`: grep, find, tree, and an explicit unsupported semantic-search placeholder.
- `client.vcs`: commit, log, revert, status, diff, list/create/update refs.
- `client.reviews`: protected refs/paths, change requests, approvals, reviewers, comments, reject, merge.
- `client.runs`: create and read run records, stdout, stderr.
- `client.workspaces`: list, get, create, issue workspace tokens.

The top-level `StratumClient` also keeps compatibility methods used by `@stratum/bash`, such as `readFile`, `writeFile`, `grep`, `status`, `diff`, and `commit`.

Mount exports:

- `client.mount(options?)`: returns a `StratumVolume`.
- `StratumVolume` / `StratumMount`: in-process mounted workspace with `pwd`, `cd`, `ls`, `readFile`, `readFileBuffer`, `writeFile`, `mkdir`, `deletePath`, `copyPath`, `movePath`, `grep`, `find`, `tree`, `status`, `diff`, `commit`, and `stat`.
- `PathIndex`, `SessionCache`, `normalizeMountPath`, `normalizePath`, `toClientPath`, and `dirname` for advanced adapters.

## Current Boundaries

- Semantic search is not implemented by the Stratum backend yet. `client.search.semantic()` throws `UnsupportedFeatureError` until the derived index described in `docs/semantic-index.md` exists.
- Workspace token issuance intentionally has no idempotency option because successful responses include a raw workspace secret.
- This package does not execute commands. Run records are durable artifacts only until the execution roadmap's runner phases land.
- The in-process mount is not POSIX/FUSE. It is a TypeScript object model over the HTTP workspace API.
