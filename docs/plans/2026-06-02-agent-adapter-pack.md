# Agent Adapter Pack Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ship first-party beta TypeScript adapters for OpenAI Agents, Vercel AI SDK, LangChain/deepagents, and Mastra that operate on mounted Stratum workspaces through supported SDK routes and fail explicitly when a required capability is unavailable.

**Architecture:** Add one new `sdk/agents` workspace package named `@stratum/agents` with subpath exports for `./openai`, `./vercel`, `./langchain`, and `./mastra`. The package shares a small Stratum workspace facade over `@stratum/sdk` / `StratumVolume`, gates behavior through the v1 capability manifest, and uses the Stratum `/execute` route only when the manifest says execution is available. Do not add backend capabilities, host-shell fallbacks, live provider calls, or Python adapters in this slice.

**Tech Stack:** TypeScript, Bun workspaces, NodeNext ESM, Vitest, `@stratum/sdk`, optional peer harnesses `@openai/agents`, `ai`, `deepagents`, `@mastra/core`, `zod`, and provider-free fake clients/tests.

---

## Current Inventory

- `sdk/package.json` currently has workspaces `typescript` and `bash`; root scripts run build/typecheck/test across those two packages.
- `sdk/typescript` publishes `@stratum/sdk`, exports `StratumClient`, `StratumVolume`, HTTP helpers, mount/cache/path helpers, and types.
- `sdk/typescript/src/types.ts` does not currently include `CapabilityRoutes.execute`, even though the checked-in fixtures already contain `routes.execute`.
- `sdk/typescript/src/client.ts` has `runs` wrappers for `POST /runs`, `GET /runs/{id}`, `stdout`, and `stderr`, but no first-class `/execute` client.
- Slice 18 added the server-side `/execute` route family. Public job summaries include metadata only: `workspace_id`, `job_id`, `run_id`, `status`, projected `run_paths`, timestamps, `exit_code`, `stdout_truncated`, and `stderr_truncated`.
- Slice 18 intentionally keeps execution disabled by default and rejects `Idempotency-Key` on execution routes.
- Mirage references live at `/Users/rajattiwari/virtualfilesystem/mirage/typescript/packages/agents/src/{openai,vercel,langchain,mastra}`. Use them as behavior references, not as direct Stratum semantics.
- Mirage uses an in-process `Workspace` with `fs` and `execute`. Stratum must instead use mounted HTTP workspace routes and must not imply POSIX/FUSE or arbitrary host-shell access.

## Package Decision

Use one beta package:

- Create `sdk/agents`.
- Package name: `@stratum/agents`.
- Exports:
  - `@stratum/agents`
  - `@stratum/agents/openai`
  - `@stratum/agents/vercel`
  - `@stratum/agents/langchain`
  - `@stratum/agents/mastra`
- Keep root exports provider-free: shared prompt, workspace facade, capability helpers, and public types only.
- Keep framework packages as optional peers so users do not need all four harnesses to consume one subpath.

Supported target harness versions checked from npm on 2026-06-02:

- `@openai/agents`: `0.11.6`
- `ai`: `6.0.195`
- `deepagents`: `1.10.2`
- `@mastra/core`: `1.38.0`
- `zod`: `4.4.3`

Use exact versions in `devDependencies` for test/typecheck repeatability. Use narrow peer ranges that document the tested minor line:

```json
{
  "@openai/agents": ">=0.11.6 <0.12.0",
  "ai": ">=6.0.195 <6.1.0",
  "deepagents": ">=1.10.2 <1.11.0",
  "@mastra/core": ">=1.38.0 <1.39.0",
  "zod": ">=4.4.3 <5.0.0"
}
```

## Hard Rules

- Normal tests must make no live OpenAI, Vercel, LangChain, Mastra, model, hosted Stratum, or provider-network calls.
- Adapter read/write/list/edit behavior must use `@stratum/sdk` / `StratumVolume` methods.
- Adapter execution behavior must use the Stratum SDK `/execute` wrapper added in this plan.
- If `manifest.routes.execute.available !== true`, execute methods/tools must fail explicitly with a stable unsupported error or error object. They must not use `child_process`, `bun`, `/bin/sh`, local process execution, or a fake successful shell result.
- Do not send `Idempotency-Key` on `/execute`, `/execute/jobs/{id}/wait`, or `/execute/jobs/{id}/cancel`.
- Do not include raw workspace tokens, bearer tokens, environment variables, raw commands, stdout/stderr, local paths, backing paths, temp paths, or provider errors in adapter debug output, public thrown messages, tool metadata, or docs examples.
- The command string may appear only in the request body sent to Stratum `/execute` and in the server-owned `/runs/<run-id>/command.md` artifact.
- Keep the package beta and do not add publish automation.

## Task 1: SDK Execute Contract And Narrow Client Wrapper

**Files:**
- Modify: `sdk/typescript/src/types.ts`
- Modify: `sdk/typescript/src/client.ts`
- Modify: `sdk/typescript/tests/client.test.ts`

**Step 1: Write failing SDK contract tests**

In `sdk/typescript/tests/client.test.ts`, extend the capability fixture tests:

```ts
expect(capabilitiesFixture.routes.execute.available).toBe(false);
expect(capabilitiesFixture.routes.execute.execution).toBe(false);
expect(capabilitiesFixture.routes.execute.requires).toContain("STRATUM_EXECUTION_RUNNER=process-local");
expect(durableCapabilitiesFixture.routes.execute.available).toBe(false);
expect(durableCapabilitiesFixture.routes.execute.reason).toBe("durable-cloud route is not supported yet");
```

Add execute client tests:

```ts
it("builds execute submit/list/get/wait/cancel without idempotency", async () => {
  const summary = executeSummary({ status: "running" });
  const { fetchImpl, requests } = recordFetch(jsonResponse(summary));
  const client = new StratumClient({
    baseUrl: "https://stratum.example",
    auth: { type: "workspace", workspaceId: "ws_1", workspaceToken: "secret" },
    fetch: fetchImpl,
  });

  await client.execute.submit({ command: "bun test", prompt: "Run tests" });
  await client.execute.list();
  await client.execute.get(summary.job_id);
  await client.execute.wait(summary.job_id, { timeout_ms: 1000 });
  await client.execute.cancel(summary.job_id);

  expect(requests.map((r) => [r.method, new URL(r.url).pathname])).toEqual([
    ["POST", "/execute"],
    ["GET", "/execute/jobs"],
    ["GET", `/execute/jobs/${summary.job_id}`],
    ["POST", `/execute/jobs/${summary.job_id}/wait`],
    ["POST", `/execute/jobs/${summary.job_id}/cancel`],
  ]);
  expect(requests.every((r) => !r.headers.has("Idempotency-Key"))).toBe(true);
});
```

Add a convenience run test that composes submit, wait, and run stdout/stderr reads:

```ts
it("runs execute through metadata routes then reads run output", async () => {
  const summary = executeSummary({ status: "queued" });
  const terminal = executeSummary({ ...summary, status: "succeeded", exit_code: 0 });
  const responses = [
    jsonResponse(summary),
    jsonResponse(terminal),
    textResponse("ok\n"),
    textResponse(""),
  ];
  const requests: Request[] = [];
  const client = new StratumClient({
    baseUrl: "https://stratum.example",
    auth: { type: "workspace", workspaceId: "ws_1", workspaceToken: "secret" },
    fetch: async (input, init) => {
      requests.push(new Request(input, init));
      return responses.shift()!.clone();
    },
  });

  await expect(client.execute.run({ command: "printf ok" })).resolves.toMatchObject({
    job_id: summary.job_id,
    status: "succeeded",
    exit_code: 0,
    stdout: "ok\n",
    stderr: "",
  });

  expect(requests.map((r) => [r.method, new URL(r.url).pathname])).toEqual([
    ["POST", "/execute"],
    ["POST", `/execute/jobs/${summary.job_id}/wait`],
    ["GET", `/runs/${summary.run_id}/stdout`],
    ["GET", `/runs/${summary.run_id}/stderr`],
  ]);
  expect(requests.every((r) => !r.headers.has("Idempotency-Key"))).toBe(true);
});
```

Add a helper in the test file:

```ts
function executeSummary(overrides: Partial<ExecuteJobSummary> = {}): ExecuteJobSummary {
  return {
    workspace_id: "550e8400-e29b-41d4-a716-446655440000",
    job_id: "550e8400-e29b-41d4-a716-446655440001",
    run_id: "550e8400-e29b-41d4-a716-446655440002",
    status: "queued",
    run_paths: {
      root: "/runs/550e8400-e29b-41d4-a716-446655440002",
      prompt: "/runs/550e8400-e29b-41d4-a716-446655440002/prompt.md",
      command: "/runs/550e8400-e29b-41d4-a716-446655440002/command.md",
      stdout: "/runs/550e8400-e29b-41d4-a716-446655440002/stdout.md",
      stderr: "/runs/550e8400-e29b-41d4-a716-446655440002/stderr.md",
      result: "/runs/550e8400-e29b-41d4-a716-446655440002/result.md",
      metadata: "/runs/550e8400-e29b-41d4-a716-446655440002/metadata.md",
      artifacts: "/runs/550e8400-e29b-41d4-a716-446655440002/artifacts",
    },
    created_at: "2026-06-02T00:00:00Z",
    started_at: null,
    ended_at: null,
    exit_code: null,
    stdout_truncated: false,
    stderr_truncated: false,
    ...overrides,
  };
}
```

Run:

```bash
cd sdk && bun run --cwd typescript test:run -- client.test.ts
```

Expected: FAIL because `CapabilityRoutes.execute`, execute types, and `client.execute` do not exist.

**Step 2: Add execute types**

In `sdk/typescript/src/types.ts`:

```ts
export interface CapabilityRoutes {
  readonly filesystem: CapabilityFilesystemRoutes;
  readonly search: CapabilitySearchRoutes;
  readonly vcs: CapabilityVcsRoutes;
  readonly review: CapabilityReviewRoutes;
  readonly workspaces: CapabilityWorkspaceRoutes;
  readonly audit: CapabilityRouteOperation;
  readonly runs: CapabilityRouteOperation;
  readonly execute: CapabilityRouteOperation;
}

export type ExecuteJobStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "timed_out";

export interface ExecuteRequest {
  readonly command: string;
  readonly prompt?: string;
  readonly run_id?: string;
}

export interface ExecuteWaitRequest {
  readonly timeout_ms?: number;
}

export interface ExecuteRunPaths {
  readonly root: string;
  readonly prompt: string;
  readonly command: string;
  readonly stdout: string;
  readonly stderr: string;
  readonly result: string;
  readonly metadata: string;
  readonly artifacts: string;
}

export interface ExecuteJobSummary {
  readonly workspace_id: string;
  readonly job_id: string;
  readonly run_id: string;
  readonly status: ExecuteJobStatus;
  readonly run_paths: ExecuteRunPaths;
  readonly created_at: string;
  readonly started_at: string | null;
  readonly ended_at: string | null;
  readonly exit_code: number | null;
  readonly stdout_truncated: boolean;
  readonly stderr_truncated: boolean;
}

export interface ExecuteJobListResponse {
  readonly jobs: readonly ExecuteJobSummary[];
}

export interface ExecuteRunResult extends ExecuteJobSummary {
  readonly stdout: string;
  readonly stderr: string;
}
```

**Step 3: Add `ExecuteClient`**

In `sdk/typescript/src/client.ts`, import the new types and add:

```ts
export class ExecuteClient {
  constructor(private readonly http: StratumHttpClient, private readonly runs: RunsClient) {}

  submit(request: ExecuteRequest): Promise<ExecuteJobSummary> {
    return this.http.json("execute", { method: "POST", body: request });
  }

  list(): Promise<ExecuteJobListResponse> {
    return this.http.json("execute/jobs", { method: "GET" });
  }

  get(jobId: string): Promise<ExecuteJobSummary> {
    return this.http.json(`execute/jobs/${encodeRouteSegment(jobId)}`, { method: "GET" });
  }

  wait(jobId: string, request: ExecuteWaitRequest = {}): Promise<ExecuteJobSummary> {
    return this.http.json(`execute/jobs/${encodeRouteSegment(jobId)}/wait`, {
      method: "POST",
      body: request,
    });
  }

  cancel(jobId: string): Promise<ExecuteJobSummary> {
    return this.http.json(`execute/jobs/${encodeRouteSegment(jobId)}/cancel`, {
      method: "POST",
    });
  }

  async run(request: ExecuteRequest, wait: ExecuteWaitRequest = {}): Promise<ExecuteRunResult> {
    const submitted = await this.submit(request);
    const terminal = await this.wait(submitted.job_id, wait);
    const [stdout, stderr] = await Promise.all([
      this.runs.stdout(terminal.run_id),
      this.runs.stderr(terminal.run_id),
    ]);
    return { ...terminal, stdout, stderr };
  }
}
```

Wire it in `StratumClient`:

```ts
readonly execute: ExecuteClient;
...
this.runs = new RunsClient(this.http);
this.execute = new ExecuteClient(this.http, this.runs);
```

Do not add auto idempotency or idempotency options to any execute method.

**Step 4: Verify focused SDK tests**

Run:

```bash
cd sdk && bun run --cwd typescript test:run -- client.test.ts
```

Expected: PASS.

**Step 5: Commit**

```bash
git add sdk/typescript/src/types.ts sdk/typescript/src/client.ts sdk/typescript/tests/client.test.ts
git commit -m "feat: add TypeScript execute client"
```

## Task 2: Agent Package Boundary And Shared Workspace Facade

**Files:**
- Modify: `sdk/package.json`
- Add: `sdk/agents/package.json`
- Add: `sdk/agents/tsconfig.json`
- Add: `sdk/agents/tsconfig.test.json`
- Add: `sdk/agents/vitest.config.ts`
- Add: `sdk/agents/src/index.ts`
- Add: `sdk/agents/src/prompt.ts`
- Add: `sdk/agents/src/workspace.ts`
- Add: `sdk/agents/src/mime.ts`
- Add: `sdk/agents/tests/package-boundary.test.ts`
- Add: `sdk/agents/tests/prompt.test.ts`
- Add: `sdk/agents/tests/workspace.test.ts`
- Modify: `sdk/bun.lock` after running install

**Step 1: Write failing package boundary tests**

Create `sdk/agents/tests/package-boundary.test.ts`:

```ts
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

type PackageJson = {
  exports?: Record<string, { types?: string; import?: string }>;
  files?: string[];
  scripts?: Record<string, string>;
  dependencies?: Record<string, string>;
  peerDependencies?: Record<string, string>;
  peerDependenciesMeta?: Record<string, { optional?: boolean }>;
  devDependencies?: Record<string, string>;
};

const packageJson = JSON.parse(
  readFileSync(fileURLToPath(new URL("../package.json", import.meta.url)), "utf8"),
) as PackageJson;

describe("agents package boundary", () => {
  it("publishes root and adapter subpath exports from dist only", () => {
    expect(packageJson.exports).toEqual({
      ".": { types: "./dist/index.d.ts", import: "./dist/index.js" },
      "./openai": { types: "./dist/openai/index.d.ts", import: "./dist/openai/index.js" },
      "./vercel": { types: "./dist/vercel/index.d.ts", import: "./dist/vercel/index.js" },
      "./langchain": { types: "./dist/langchain/index.d.ts", import: "./dist/langchain/index.js" },
      "./mastra": { types: "./dist/mastra/index.d.ts", import: "./dist/mastra/index.js" },
    });
    expect(packageJson.files).toEqual(["dist", "README.md"]);
  });

  it("pins beta target harness versions and marks framework peers optional", () => {
    expect(packageJson.peerDependencies).toMatchObject({
      "@openai/agents": ">=0.11.6 <0.12.0",
      ai: ">=6.0.195 <6.1.0",
      deepagents: ">=1.10.2 <1.11.0",
      "@mastra/core": ">=1.38.0 <1.39.0",
      zod: ">=4.4.3 <5.0.0",
    });
    for (const name of ["@openai/agents", "ai", "deepagents", "@mastra/core"]) {
      expect(packageJson.peerDependenciesMeta?.[name]?.optional).toBe(true);
    }
    expect(packageJson.devDependencies).toMatchObject({
      "@openai/agents": "0.11.6",
      ai: "6.0.195",
      deepagents: "1.10.2",
      "@mastra/core": "1.38.0",
      zod: "4.4.3",
    });
  });
});
```

Run:

```bash
cd sdk && bun run --cwd agents test:run -- package-boundary.test.ts
```

Expected: FAIL because the package does not exist.

**Step 2: Add workspace package config**

Update `sdk/package.json`:

```json
{
  "workspaces": ["typescript", "bash", "agents"],
  "scripts": {
    "build": "bun run --cwd typescript build && bun run --cwd bash build && bun run --cwd agents build",
    "typecheck": "bun run --cwd typescript typecheck && bun run --cwd bash typecheck && bun run --cwd agents typecheck",
    "test:run": "bun run --cwd typescript test:run && bun run --cwd bash test:run && bun run --cwd agents test:run"
  }
}
```

Create `sdk/agents/package.json`:

```json
{
  "name": "@stratum/agents",
  "version": "0.0.0-beta.0",
  "description": "Beta agent framework adapters for mounted Stratum workspaces.",
  "license": "MIT",
  "type": "module",
  "exports": {
    ".": { "types": "./dist/index.d.ts", "import": "./dist/index.js" },
    "./openai": { "types": "./dist/openai/index.d.ts", "import": "./dist/openai/index.js" },
    "./vercel": { "types": "./dist/vercel/index.d.ts", "import": "./dist/vercel/index.js" },
    "./langchain": { "types": "./dist/langchain/index.d.ts", "import": "./dist/langchain/index.js" },
    "./mastra": { "types": "./dist/mastra/index.d.ts", "import": "./dist/mastra/index.js" }
  },
  "files": ["dist", "README.md"],
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "prepare": "tsc -p tsconfig.json",
    "prepack": "tsc -p tsconfig.json",
    "typecheck": "tsc --noEmit -p tsconfig.test.json",
    "test": "vitest",
    "test:run": "vitest run"
  },
  "dependencies": {
    "@stratum/sdk": "0.0.0",
    "picomatch": "^4.0.4"
  },
  "peerDependencies": {
    "@openai/agents": ">=0.11.6 <0.12.0",
    "ai": ">=6.0.195 <6.1.0",
    "deepagents": ">=1.10.2 <1.11.0",
    "@mastra/core": ">=1.38.0 <1.39.0",
    "zod": ">=4.4.3 <5.0.0"
  },
  "peerDependenciesMeta": {
    "@openai/agents": { "optional": true },
    "ai": { "optional": true },
    "deepagents": { "optional": true },
    "@mastra/core": { "optional": true }
  },
  "devDependencies": {
    "@openai/agents": "0.11.6",
    "ai": "6.0.195",
    "deepagents": "1.10.2",
    "@mastra/core": "1.38.0",
    "@types/node": "^25.6.0",
    "typescript": "^5.6.0",
    "vitest": "^4.1.5",
    "zod": "4.4.3"
  },
  "engines": {
    "node": ">=20"
  }
}
```

Use the same NodeNext compiler settings as `sdk/typescript` and `sdk/bash`.

Run:

```bash
cd sdk && bun install
```

Expected: lockfile updates for `sdk/agents` and new harness dev dependencies.

**Step 3: Write failing prompt tests**

Create `sdk/agents/tests/prompt.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { STRATUM_SYSTEM_PROMPT, buildStratumSystemPrompt } from "../src/index.js";

describe("buildStratumSystemPrompt", () => {
  it("describes Stratum as a mounted HTTP workspace, not POSIX or a host shell", () => {
    expect(STRATUM_SYSTEM_PROMPT).toContain("mounted Stratum workspace");
    expect(STRATUM_SYSTEM_PROMPT).toContain("HTTP API");
    expect(STRATUM_SYSTEM_PROMPT).toContain("not a POSIX shell");
    expect(STRATUM_SYSTEM_PROMPT).not.toContain("FUSE");
  });

  it("adds mounted workspace info and extra instructions", () => {
    const out = buildStratumSystemPrompt({
      mountInfo: { "/": "Case review workspace", "/runs": "Durable run artifacts" },
      extraInstructions: "Keep edits minimal.",
    });
    expect(out).toContain("- /: Case review workspace");
    expect(out).toContain("- /runs: Durable run artifacts");
    expect(out.endsWith("Keep edits minimal.")).toBe(true);
  });

  it("omits empty optional sections", () => {
    expect(buildStratumSystemPrompt({ mountInfo: {}, extraInstructions: "" })).toBe(STRATUM_SYSTEM_PROMPT);
  });
});
```

Run:

```bash
cd sdk && bun run --cwd agents test:run -- prompt.test.ts
```

Expected: FAIL because prompt exports do not exist.

**Step 4: Implement prompt module**

Create `sdk/agents/src/prompt.ts`:

```ts
export const STRATUM_SYSTEM_PROMPT = `You are working in a mounted Stratum workspace exposed through the Stratum HTTP API.

Paths are workspace-relative Unix-style paths, but this is not a POSIX shell or a local FUSE mount. Use the provided Stratum tools to read, list, write, and edit files. Use execution only when the workspace capability manifest says Stratum execution is available.

Stratum records execution under /runs/<run-id>/ when execution is enabled. Do not claim access to arbitrary host files, host environment variables, network credentials, or local shell state.`;

export interface BuildStratumSystemPromptOptions {
  readonly mountInfo?: Record<string, string>;
  readonly extraInstructions?: string;
}

export function buildStratumSystemPrompt(options: BuildStratumSystemPromptOptions = {}): string {
  const parts = [STRATUM_SYSTEM_PROMPT];
  const entries = Object.entries(options.mountInfo ?? {});
  if (entries.length > 0) {
    parts.push("");
    parts.push("Mounted workspace areas:");
    for (const [path, description] of entries) {
      parts.push(`- ${path}: ${description}`);
    }
  }
  if (options.extraInstructions !== undefined && options.extraInstructions.length > 0) {
    parts.push("");
    parts.push(options.extraInstructions);
  }
  return parts.join("\n");
}
```

Export it from `sdk/agents/src/index.ts`.

**Step 5: Write failing workspace facade tests**

Create `sdk/agents/tests/workspace.test.ts` with a provider-free fake client implementing the `StratumVolumeClient` surface. Cover:

- Reads text and bytes through `StratumVolume`.
- Writes files and creates missing parents with repeated `mkdir`.
- `exists` returns false for a `StratumHttpError` 404 and rethrows other failures.
- `editFile` rejects missing files, missing strings, and non-unique strings unless `replaceAll` is true.
- `listFiles` returns path plus `is_dir`.
- `glob` recursively walks the mounted workspace and uses `picomatch`, not shell execution.
- `grep` delegates to `volume.grep`.
- `execute` throws `UnsupportedFeatureError` when `routes.execute.available` is false.
- `execute` calls `client.execute.run` and returns stdout/stderr/exitCode when the manifest enables execution.
- Thrown unsupported/errors do not include the raw command or fake token string.

Run:

```bash
cd sdk && bun run --cwd agents test:run -- workspace.test.ts
```

Expected: FAIL because workspace facade does not exist.

**Step 6: Implement shared workspace facade**

Create `sdk/agents/src/workspace.ts`.

Public shape:

```ts
import {
  StratumVolume,
  UnsupportedFeatureError,
  type CapabilityManifest,
  type ExecuteRunResult,
  type StratumClient,
  type StratumDirectoryEntry,
  type StratumStat,
} from "@stratum/sdk";

export interface StratumAgentWorkspaceOptions {
  readonly client: StratumClient;
  readonly capabilities: CapabilityManifest;
  readonly volume?: StratumVolume;
}

export interface StratumAgentExecuteResult {
  readonly stdout: string;
  readonly stderr: string;
  readonly exitCode: number | null;
  readonly status: ExecuteRunResult["status"];
  readonly runId: string;
  readonly jobId: string;
  readonly stdoutTruncated: boolean;
  readonly stderrTruncated: boolean;
}

export class StratumAgentWorkspace {
  readonly client: StratumClient;
  readonly capabilities: CapabilityManifest;
  readonly volume: StratumVolume;

  constructor(options: StratumAgentWorkspaceOptions) {
    this.client = options.client;
    this.capabilities = options.capabilities;
    this.volume = options.volume ?? options.client.mount();
  }

  canExecute(): boolean {
    return this.capabilities.routes.execute.available === true;
  }

  async readFileText(path: string): Promise<string> {
    this.requireRoute("filesystem.read", this.capabilities.routes.filesystem.read);
    return this.volume.readFile(path);
  }

  async readFileBytes(path: string): Promise<Uint8Array> {
    this.requireRoute("filesystem.read", this.capabilities.routes.filesystem.read);
    return this.volume.readFileBuffer(path);
  }

  async writeFile(path: string, content: string | Uint8Array): Promise<{ path: string }> {
    this.requireRoute("filesystem.write", this.capabilities.routes.filesystem.write);
    await this.ensureParent(path);
    await this.volume.writeFile(path, content);
    return { path };
  }

  async stat(path: string): Promise<StratumStat> {
    this.requireRoute("filesystem.stat", this.capabilities.routes.filesystem.stat);
    return this.volume.stat(path);
  }

  async exists(path: string): Promise<boolean> {
    try {
      await this.stat(path);
      return true;
    } catch (error) {
      if (isHttpNotFound(error)) return false;
      throw error;
    }
  }

  async isDirectory(path: string): Promise<boolean> {
    return (await this.stat(path)).kind === "directory";
  }

  async listFiles(path: string): Promise<readonly { path: string; is_dir: boolean }[]> {
    this.requireRoute("filesystem.list", this.capabilities.routes.filesystem.list);
    const listing = await this.volume.listDirectory(path);
    return listing.entries.map((entry) => ({
      path: joinWorkspacePath(path, entry.name),
      is_dir: entry.is_dir,
    }));
  }

  async editFile(path: string, oldString: string, newString: string, replaceAll = false): Promise<{ path: string; occurrences: number }> {
    const current = await this.readFileText(path);
    const count = current.split(oldString).length - 1;
    if (count === 0) throw new Error("string not found in file");
    if (count > 1 && replaceAll !== true) throw new Error(`string appears ${count} times; set replaceAll=true`);
    const next = replaceAll ? current.split(oldString).join(newString) : current.replace(oldString, newString);
    await this.writeFile(path, next);
    return { path, occurrences: replaceAll ? count : 1 };
  }

  async execute(command: string, prompt?: string): Promise<StratumAgentExecuteResult> {
    if (!this.canExecute()) {
      throw new UnsupportedFeatureError("Stratum execution is unavailable for this workspace.");
    }
    const result = await this.client.execute.run({ command, prompt });
    return {
      stdout: result.stdout,
      stderr: result.stderr,
      exitCode: result.exit_code,
      status: result.status,
      runId: result.run_id,
      jobId: result.job_id,
      stdoutTruncated: result.stdout_truncated,
      stderrTruncated: result.stderr_truncated,
    };
  }

  private requireRoute(name: string, route: { available: boolean; reason?: string }): void {
    if (route.available !== true) {
      throw new UnsupportedFeatureError(`${name} is unavailable for this Stratum workspace.`);
    }
  }
}
```

Also implement:

- `ensureParent(path)`: recursively create parents with `volume.mkdir`, tolerate "already exists" races by checking `exists`.
- `glob(pattern, path = "/")`: recursively list directories, match with `picomatch`, no shell fallback.
- `grep(pattern, path = "/", recursive = true)`: delegate to `volume.grep(pattern, path, recursive)` and return SDK results.
- `joinWorkspacePath(parent, name)`: stable absolute workspace path helper.
- `isHttpNotFound(error)`: check exported `StratumHttpError` status.

Create `sdk/agents/src/mime.ts` with shared extension/mime helpers copied conceptually from Mirage, but Stratum-specific:

- Prefer `StratumStat.mime_type` when present.
- Treat `text/*`, `application/json`, `application/xml`, `application/yaml`, and common source/data extensions as text.
- Treat `application/pdf`, `image/png`, `image/jpeg`, `image/gif`, and `image/webp` as presentable binary.
- Return unsupported binary metadata stubs for other binary types.

**Step 7: Verify shared package**

Run:

```bash
cd sdk && bun run --cwd agents test:run -- package-boundary.test.ts prompt.test.ts workspace.test.ts
cd sdk && bun run --cwd agents typecheck
```

Expected: PASS.

**Step 8: Commit**

```bash
git add sdk/package.json sdk/bun.lock sdk/agents
git commit -m "feat: add Stratum agents package foundation"
```

## Task 3: OpenAI Agents Adapter

**Files:**
- Add: `sdk/agents/src/openai/index.ts`
- Add: `sdk/agents/src/openai/editor.ts`
- Add: `sdk/agents/src/openai/shell.ts`
- Add: `sdk/agents/tests/openai.test.ts`

**Step 1: Write failing OpenAI tests**

Create tests based on Mirage `openai/editor.test.ts` and `openai/shell.test.ts`, but using `StratumAgentWorkspace` with fake Stratum client/capabilities.

Cover:

- `StratumEditor.createFile` applies OpenAI `applyDiff` and auto-creates missing parents.
- `StratumEditor.updateFile` applies a diff to existing content.
- `StratumEditor.updateFile` returns `{ status: "failed", output: "File not found: /path" }` for missing path.
- `StratumEditor.deleteFile` removes files through `volume.deletePath`.
- `StratumShell.run` runs one command and maps stdout/stderr/exit code.
- `StratumShell.run` runs multiple commands in order.
- `StratumShell.run` fails explicitly when execution is unavailable and does not include the command string in the thrown message.

Run:

```bash
cd sdk && bun run --cwd agents test:run -- openai.test.ts
```

Expected: FAIL because OpenAI adapter files do not exist.

**Step 2: Implement OpenAI adapter**

Create `StratumEditor`:

- Implements `Editor` from `@openai/agents`.
- Imports `applyDiff` from `@openai/agents`.
- Uses `StratumAgentWorkspace.writeFile`, `readFileText`, and `volume.deletePath`.
- Never logs or returns token material.

Create `StratumShell`:

- Implements `Shell` from `@openai/agents`.
- For each command in `ShellAction.commands`, calls `workspace.execute(command)`.
- Maps output:

```ts
{
  stdout: result.stdout,
  stderr: result.stderr,
  outcome: { type: "exit", exitCode: result.exitCode ?? 1 }
}
```

Export:

```ts
export { StratumEditor } from "./editor.js";
export { StratumShell } from "./shell.js";
export { STRATUM_SYSTEM_PROMPT, buildStratumSystemPrompt } from "../prompt.js";
export type { BuildStratumSystemPromptOptions } from "../prompt.js";
```

**Step 3: Verify OpenAI adapter**

Run:

```bash
cd sdk && bun run --cwd agents test:run -- openai.test.ts
cd sdk && bun run --cwd agents typecheck
```

Expected: PASS.

**Step 4: Commit**

```bash
git add sdk/agents/src/openai sdk/agents/tests/openai.test.ts
git commit -m "feat: add OpenAI Stratum adapter"
```

## Task 4: Vercel AI SDK Adapter

**Files:**
- Add: `sdk/agents/src/vercel/index.ts`
- Add: `sdk/agents/tests/vercel.test.ts`

**Step 1: Write failing Vercel tests**

Mirror Mirage `vercel/index.test.ts`, adjusted for Stratum:

- `stratumTools(workspace).execute` returns `{ stdout, stderr, exitCode }` when execution is available.
- `execute` returns a stable error object when execution is unavailable.
- `readFile` returns text for text files.
- `readFile` returns base64 media for images and PDFs.
- `readFile` returns a binary metadata stub for unsupported binaries.
- `readFile.toModelOutput` returns AI SDK text/content/error-text outputs.
- `writeFile` creates files and missing parents.
- `editFile` replaces one occurrence, rejects multiple occurrences, and supports `replaceAll`.
- `ls` lists entries with `is_dir` flags.
- No test makes a provider/model call.

Run:

```bash
cd sdk && bun run --cwd agents test:run -- vercel.test.ts
```

Expected: FAIL because Vercel adapter does not exist.

**Step 2: Implement Vercel tools**

Create `stratumTools(workspace: StratumAgentWorkspace)` using `tool` from `ai` and `zod`.

Tool set:

- `execute`: `command: string`; calls `workspace.execute`. On `UnsupportedFeatureError`, return `{ error: "Stratum execution is unavailable for this workspace." }`. Do not include the command in the error.
- `readFile`: `path: string`; uses stat/mime helpers and `readFileText` or `readFileBytes`.
- `writeFile`: `path`, `content`; calls `workspace.writeFile`.
- `editFile`: `path`, `oldString`, `newString`, `replaceAll?`; calls `workspace.editFile`.
- `ls`: `path`; calls `workspace.listFiles`.

Use `Buffer.from(bytes).toString("base64")` for Node-target base64 in tests.

**Step 3: Verify Vercel adapter**

Run:

```bash
cd sdk && bun run --cwd agents test:run -- vercel.test.ts
cd sdk && bun run --cwd agents typecheck
```

Expected: PASS.

**Step 4: Commit**

```bash
git add sdk/agents/src/vercel sdk/agents/tests/vercel.test.ts
git commit -m "feat: add Vercel Stratum tools"
```

## Task 5: LangChain / Deepagents Adapter

**Files:**
- Add: `sdk/agents/src/langchain/index.ts`
- Add: `sdk/agents/src/langchain/backend.ts`
- Add: `sdk/agents/src/langchain/convert.ts`
- Add: `sdk/agents/src/langchain/messages.ts`
- Add: `sdk/agents/tests/langchain.test.ts`

**Step 1: Write failing LangChain tests**

Use Mirage `langchain/backend.test.ts` and `messages.test.ts` as references, adjusted for Stratum.

Cover:

- `StratumLangChainWorkspace.id` defaults to `"stratum"` and accepts `sandboxId`.
- `execute` maps Stratum execution to `ExecuteResponse` and fails explicitly when unavailable.
- `write` creates new files, rejects existing paths, and creates missing parents.
- `read` supports offset/limit for text files.
- `read` returns `Uint8Array` for PDF and supported image mimes.
- `read` rejects unsupported binary mimes with an explicit binary message.
- `edit` follows exact-string replacement rules.
- `ls` returns file info with `is_dir`.
- `glob` uses recursive SDK listing plus `picomatch`, including paths with single quotes.
- `grep` maps SDK grep results into deepagents `GrepMatch[]`, with no shell fallback.
- `uploadFiles` and `downloadFiles` use SDK write/read.
- `readRaw` returns file data with content, mime type, and ISO timestamps.
- `extractText` handles plain string and array content blocks.

Run:

```bash
cd sdk && bun run --cwd agents test:run -- langchain.test.ts
```

Expected: FAIL because LangChain adapter does not exist.

**Step 2: Implement LangChain adapter**

Create `StratumLangChainWorkspace implements SandboxBackendProtocolV2` from `deepagents`.

Implementation rules:

- `execute(command)`: call `workspace.execute`; return `{ output, exitCode, truncated }`, with stderr appended to stdout only for the deepagents response shape. If unsupported, return `{ error: "Stratum execution is unavailable for this workspace." }` or the exact `ExecuteResponse` error shape expected by `deepagents` types.
- `ls(path)`: call `workspace.listFiles`.
- `read(filePath, offset = 0, limit = 500)`: use mime helpers; text returns sliced lines; presentable binary returns bytes; unsupported binary returns error.
- `readRaw(filePath)`: use `workspace.stat` plus text/binary read. Convert Stratum numeric timestamps to ISO using `new Date(stat.modified * 1000).toISOString()` unless tests reveal the SDK value is already milliseconds.
- `write`: reject existing path before writing.
- `edit`: call `workspace.editFile` and map errors.
- `grep`: call `workspace.grep(pattern, path ?? "/", true)` and map `file`, `line_num`, `line` to deepagents fields.
- `glob`: call `workspace.glob`.
- `uploadFiles` / `downloadFiles`: loop over SDK reads/writes.

Keep `extractText` as a small provider-free helper like Mirage.

**Step 3: Verify LangChain adapter**

Run:

```bash
cd sdk && bun run --cwd agents test:run -- langchain.test.ts
cd sdk && bun run --cwd agents typecheck
```

Expected: PASS.

**Step 4: Commit**

```bash
git add sdk/agents/src/langchain sdk/agents/tests/langchain.test.ts
git commit -m "feat: add LangChain Stratum workspace"
```

## Task 6: Mastra Adapter

**Files:**
- Add: `sdk/agents/src/mastra/index.ts`
- Add: `sdk/agents/tests/mastra.test.ts`

**Step 1: Write failing Mastra tests**

Mirror Mirage `mastra/index.test.ts`, adjusted for Stratum:

- `stratumTools(workspace).execute` returns stdout/stderr/exitCode when execution is available.
- `execute` has stable id `stratum-execute`.
- `execute` returns an explicit unsupported error object when unavailable.
- `readFile` reads text and returns error for missing files.
- `writeFile` creates files and parents.
- `editFile` follows exact-string replacement rules.
- `ls` lists entries with `is_dir`.
- No live Mastra or model calls.

Run:

```bash
cd sdk && bun run --cwd agents test:run -- mastra.test.ts
```

Expected: FAIL because Mastra adapter does not exist.

**Step 2: Implement Mastra tools**

Create `stratumTools(workspace: StratumAgentWorkspace)` using `createTool` from `@mastra/core/tools` and `zod`.

Use IDs:

- `stratum-execute`
- `stratum-read-file`
- `stratum-write-file`
- `stratum-edit-file`
- `stratum-ls`

Keep outputs bounded to metadata/text fields and stable error strings. Do not include command text in unsupported errors.

**Step 3: Verify Mastra adapter**

Run:

```bash
cd sdk && bun run --cwd agents test:run -- mastra.test.ts
cd sdk && bun run --cwd agents typecheck
```

Expected: PASS.

**Step 4: Commit**

```bash
git add sdk/agents/src/mastra sdk/agents/tests/mastra.test.ts
git commit -m "feat: add Mastra Stratum tools"
```

## Task 7: Adapter Docs, Status, And Final Verification

**Files:**
- Add: `sdk/agents/README.md`
- Modify: `sdk/typescript/README.md` if it has package list or SDK route coverage
- Modify: `docs/project-status.md`

**Step 1: Write docs**

Create `sdk/agents/README.md` with:

- Beta package status.
- Install examples for each adapter.
- Supported target harness versions:
  - OpenAI Agents SDK target: `@openai/agents 0.11.6`
  - Vercel AI SDK target: `ai 6.0.195`
  - LangChain/deepagents target: `deepagents 1.10.2`
  - Mastra target: `@mastra/core 1.38.0`
- Required Stratum SDK/backend capabilities:
  - mounted workspace auth
  - filesystem read/list/stat/write routes for file tools
  - `routes.execute.available === true` only for execute/shell tools
- Explicit unsupported behavior when execution is unavailable.
- Examples that construct a `StratumClient`, fetch capabilities, create `StratumAgentWorkspace`, and pass it to each adapter.
- Redaction posture: adapters do not log tokens, command output, raw commands, env vars, backing paths, or temp paths.
- Out of scope: npm publish automation, live provider tests, host-shell fallback, Python adapters, enabling execution by default.

Update `docs/project-status.md`:

- Fix the stale top header that still claims Slice 17 is latest.
- Add Slice 18 as completed if still missing from the top summary.
- Add Slice 19 as completed once this implementation passes final verification.
- Keep status factual and short.

**Step 2: Run focused checks**

Run:

```bash
git status --short --branch
cd sdk && bun run --cwd typescript test:run -- client.test.ts
cd sdk && bun run --cwd agents test:run
cd sdk && bun run --cwd agents typecheck
cd sdk && bun run build
cd sdk && bun run typecheck
cd sdk && bun run test:run
git diff --check
```

Expected: PASS.

**Step 3: Run existing backend capability/execution checks**

These should be unchanged by the SDK-only slice, but run them because the adapters depend on these contracts:

```bash
cargo test --locked server::routes_capabilities --lib -- --nocapture
cargo test --locked server::routes_execute --lib -- --nocapture
cargo test --locked --test server_startup execution -- --nocapture
```

Expected: PASS.

**Step 4: Run broader final checks**

Run:

```bash
cargo fmt --all -- --check
cargo check --locked
cargo test --locked --lib --tests
```

Expected: PASS.

Run final clippy only if Rust files changed:

```bash
cargo clippy --locked --all-targets -- -D warnings
```

Expected: PASS if run.

**Step 5: Final review before commit**

Do a local review against these risks:

- Package root or docs accidentally force users to install all harnesses.
- Any adapter uses local shell/process execution.
- Any adapter treats unavailable execution as success.
- Any debug/error/tool metadata includes raw command, raw output, token, env, local temp path, or backing path.
- `CapabilityRoutes.execute` type stays aligned with both SDK contract fixtures.
- `sdk/bun.lock` is coherent and generated by Bun.
- README examples do not imply FUSE, POSIX host access, or arbitrary shell availability.

**Step 6: Commit**

```bash
git add sdk/agents/README.md sdk/typescript/README.md docs/project-status.md
git commit -m "docs: record agent adapter pack"
```

If the final implementation changes are already split into the earlier task commits, this final commit should include only docs/status cleanup. If docs were committed earlier, skip an empty commit.

## Final Acceptance Checklist

- `@stratum/agents` exists as a beta workspace package.
- Root and four subpath exports build from `dist`.
- OpenAI, Vercel, LangChain/deepagents, and Mastra adapters compile against the documented target harness versions.
- Framework peers are optional and dev dependencies are exact target versions.
- Smoke tests cover each adapter against capability manifests and fake SDK clients.
- File read/write/list/edit behavior goes through `@stratum/sdk` / `StratumVolume`.
- Execution goes through the Stratum `/execute` SDK wrapper only and then reads stdout/stderr through existing runs routes.
- Execution disabled/unsupported states fail explicitly; no host shell fallback exists.
- Normal tests make no live provider/model/network calls beyond local package resolution during install.
- Token, env, command, stdout/stderr, temp-path, backing-path, and provider-error redaction requirements are covered by tests or review.
- `sdk/package.json`, `sdk/bun.lock`, package scripts, TS configs, and README docs are coherent.
- Existing SDK, bash SDK, capability, and execution route tests still pass.

## Handoff Boundary

Claude should implement the plan and stop with local commits on `v2/foundation`. The main Codex session will later inspect the diff, review correctness/security, run or rerun gates, handle merge/push decisions, and decide whether any follow-up fixes are required.
