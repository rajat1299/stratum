# @stratum/agents

> **Beta.** First-party agent-framework adapters for mounted Stratum workspaces. APIs may change between beta releases. This package adds no backend capabilities; release remains a manual operator action.

`@stratum/agents` lets OpenAI Agents, the Vercel AI SDK, LangChain/deepagents, and Mastra read, list, write, and edit files in a mounted Stratum workspace through `@stratum/sdk`, and run commands only through the Stratum `/execute` route when the workspace capability manifest enables execution. There is no host-shell fallback: when execution is unavailable, execute/shell tools fail explicitly.

## Install

Install matching beta releases:

```bash
npm install @stratum/sdk@0.0.0-beta.0 @stratum/agents@0.0.0-beta.0
```

In this repository, install from the SDK workspace and build locally:

```bash
cd sdk
bun install
bun run build
```

Install only the framework harness you use. Vercel and Mastra subpath adapters also require `zod`; OpenAI and LangChain/deepagents do not.

## Supported target harness versions

These adapters are built and tested against:

- OpenAI Agents SDK: `@openai/agents` `0.11.6`
- Vercel AI SDK: `ai` `6.0.195`
- LangChain / deepagents: `deepagents` `1.10.2`
- Mastra: `@mastra/core` `1.38.0`
- Validation: `zod` `4.4.3`

Framework peer ranges pin the tested minor line (for example `@openai/agents >=0.11.6 <0.12.0`). `zod` is accepted across the current v4 line.

## Required Stratum capabilities

Every adapter operates on a `StratumAgentWorkspace`, which is gated by the v1 capability manifest:

- Mounted workspace authentication (a workspace bearer token via `@stratum/sdk`).
- `routes.filesystem.read` / `list` / `stat` / `write` for the file tools.
- `routes.filesystem.delete` for OpenAI editor delete operations.
- `routes.search.grep` for LangChain/deepagents grep.
- `routes.execute.available === true` only for the execute / shell tools. When execution is unavailable, file tools keep working and execution fails explicitly.

## Usage

```ts
import { StratumClient } from "@stratum/sdk";
import { StratumAgentWorkspace } from "@stratum/agents";

const workspaceCredentials = await loadWorkspaceCredentialsFromYourSecretStore();

const client = new StratumClient({
  baseUrl: "https://stratum.example",
  auth: {
    type: "workspace",
    workspaceId: workspaceCredentials.id,
    workspaceToken: workspaceCredentials.token,
  },
});

const capabilities = await client.getCapabilities();
const workspace = new StratumAgentWorkspace({ client, capabilities });
```

## Runnable examples

See [`examples/README.md`](examples/README.md) for the incident change-request example. It consumes the env file from `stratumctl workspace seed-demo`, edits workspace files through the OpenAI Agents editor adapter, and opens a Stratum change request through the TypeScript SDK.

### OpenAI Agents

```ts
import { StratumEditor, StratumShell } from "@stratum/agents/openai";

const editor = new StratumEditor(workspace);
const shell = new StratumShell(workspace); // run() throws if execution is unavailable
```

### Vercel AI SDK

```ts
import { stratumTools } from "@stratum/agents/vercel";

const tools = stratumTools(workspace); // execute, readFile, writeFile, editFile, ls
```

### LangChain / deepagents

```ts
import { StratumLangChainWorkspace } from "@stratum/agents/langchain";

const backend = new StratumLangChainWorkspace(workspace, { sandboxId: "case-42" });
```

### Mastra

```ts
import { stratumTools } from "@stratum/agents/mastra";

const tools = stratumTools(workspace); // stratum-execute, stratum-read-file, ...
```

## Execution model

Execution is disabled by default in Stratum. When `capabilities.routes.execute.available !== true`, execute / shell tools fail explicitly:

- OpenAI `StratumShell.run` throws an `UnsupportedFeatureError`.
- Vercel and Mastra `execute` tools return a stable error object.
- LangChain `execute` returns an `ExecuteResponse` with a `null` exit code and an unavailable message.

No adapter falls back to `child_process`, `/bin/sh`, or any local process execution, and no adapter reports a fake successful result when execution is unavailable. When execution is enabled, commands run through the Stratum `/execute` route and output is read back from the server-owned `/runs/<run-id>/` artifacts.

## Redaction posture

Adapters do not log or surface tokens, environment variables, raw commands, raw stdout/stderr, provider errors, backing paths, or temporary paths in thrown messages, tool metadata, or debug output. The command string is only ever sent in the `/execute` request body and recorded by the server in `/runs/<run-id>/command.md`.

## Out of scope

- Automated npm publishing.
- Live OpenAI / Vercel / LangChain / Mastra / model calls in tests.
- Host-shell or local-process execution fallback.
- Python adapters.
- Enabling Stratum execution by default.
