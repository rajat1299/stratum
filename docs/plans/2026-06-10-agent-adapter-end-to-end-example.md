# Agent Adapter End-To-End Example Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add one runnable private-beta example showing an agent adapter inspecting the seeded incident workspace, writing files through Stratum workspace routes, and opening a Stratum change request.

**Architecture:** Keep this as a TypeScript example inside `sdk/agents`, not a backend or CLI expansion. Use the existing OpenAI Agents adapter path (`StratumEditor` from `@stratum/agents/openai`) to apply deterministic model-style patch operations, use `StratumAgentWorkspace`/`StratumClient` for all workspace reads and writes, and use the TypeScript SDK review/VCS clients for commit/ref/change-request operations. Do not add live OpenAI API calls in this slice; the real adapter path is the OpenAI Agents editor adapter already in `sdk/agents`.

**Tech Stack:** TypeScript, Bun, Vitest, `@stratum/sdk`, `@stratum/agents`, OpenAI Agents adapter types.

---

## Current Inventory

- Worktree: `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/private-beta-closeout`
- Current branch/head: `private-beta-closeout` at `04c87219f231ff71c5b79f8c50adf3b45c851493`, matching `origin/main`.
- Known untracked state: `.codex-scratch/`; preserve it.
- `sdk/agents` already exists and exports:
  - root `StratumAgentWorkspace`
  - OpenAI subpath `StratumEditor` and `StratumShell`
- `sdk/typescript` already has:
  - `StratumClient.fs.readFile/writeFile`
  - `StratumClient.vcs.commit/listRefs/createRef/updateRef/diff`
  - `StratumClient.reviews.createChangeRequest`
- `stratumctl` does not have change-request commands. Use the TypeScript SDK for the CR step.
- `stratumctl workspace seed-demo` writes `.stratum-demo/incident-workspace.env` with:
  - `STRATUM_URL`
  - `STRATUM_WORKSPACE_ID`
  - `STRATUM_WORKSPACE_TOKEN`
  - optional `STRATUM_REPO`
- Local VCS `commit` advances `main`. To create a meaningful CR, capture a baseline `main` commit, commit the agent update, reset `main` to the baseline commit, then create a source ref pointing at the update commit.
- Baseline checks already pass:
  - `bun run --cwd sdk/agents test:run`
  - `bun run --cwd sdk/agents typecheck`
  - `bun run --cwd sdk/typescript test:run`

## Hard Rules

- Do not touch Rust unless the TypeScript example uncovers a real SDK/server bug.
- Do not add a `stratumctl` CR command in this slice.
- Do not use `fs`, `node:fs`, shell commands, local paths, or host filesystem reads for workspace content.
- All workspace content reads/writes must go through `StratumAgentWorkspace`, `StratumEditor`, `StratumVolume`, or `StratumClient` workspace-auth routes.
- Do not print, log, snapshot, fixture, or commit raw API keys, bearer tokens, workspace tokens, DB URLs, generated secrets, or provider errors.
- Default tests and the default example must make no OpenAI/provider/network/model calls beyond the Stratum server selected by `STRATUM_URL`.
- Do not add a direct `openai` dependency in this slice.
- The example is for the local-state private-beta seed-demo path. Durable hosted admin polish is out of scope.

## Task 1: Add Failing Tests For The Incident Adapter Example

**Files:**
- Create: `sdk/agents/tests/incident-example.test.ts`
- Later create: `sdk/agents/examples/incident-change-request.ts`

**Step 1: Write the failing test**

Create `sdk/agents/tests/incident-example.test.ts` with tests for:

- env parsing accepts the Task 6 seed-demo variables and does not require a token to be printed.
- `createFileDiff()` creates OpenAI Agents create-file diffs.
- `runIncidentChangeRequestExample()` uses workspace auth for incident reads/writes and user/admin auth for VCS/review routes.
- the example resets `main` to baseline before creating the CR, so the CR source ref points to the update commit and target ref is `main`.
- no request URL, public result, or thrown error includes the workspace token.

Use a route-driven fake fetch rather than a sequence-only fake. The test should record every `Request`.

Skeleton:

```ts
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import type { CapabilityManifest } from "@stratum/sdk";
import {
  createFileDiff,
  loadIncidentExampleConfig,
  runIncidentChangeRequestExample,
} from "../examples/incident-change-request.js";

const capabilities = JSON.parse(
  readFileSync(
    fileURLToPath(new URL("../../contracts/capabilities.v1.json", import.meta.url)),
    "utf8",
  ),
) as CapabilityManifest;

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function textResponse(body: string, status = 200): Response {
  return new Response(body, { status });
}

function dirStatResponse(): Response {
  return jsonResponse({
    inode_id: 2,
    kind: "directory",
    size: 0,
    mode: "40755",
    uid: 0,
    gid: 0,
    created: 1,
    modified: 1,
    mime_type: null,
    content_hash: null,
    custom_attrs: {},
  });
}

function mainRef(target: string, version: number) {
  return { name: "main", target, version };
}

describe("incident adapter example", () => {
  it("loads seed-demo env without exposing the workspace token in the returned public config", () => {
    const config = loadIncidentExampleConfig({
      STRATUM_URL: "http://127.0.0.1:3000",
      STRATUM_WORKSPACE_ID: "11111111-1111-1111-1111-111111111111",
      STRATUM_WORKSPACE_TOKEN: "workspace-secret",
      STRATUM_REPO: "tenant-a",
    });

    expect(config.baseUrl).toBe("http://127.0.0.1:3000");
    expect(config.workspaceId).toBe("11111111-1111-1111-1111-111111111111");
    expect(config.repoId).toBe("tenant-a");
    expect(JSON.stringify(config.publicConfig)).not.toContain("workspace-secret");
  });

  it("renders create-file diffs for OpenAI Agents editor operations", () => {
    expect(createFileDiff("hello\nworld\n")).toBe("+hello\n+world\n");
    expect(createFileDiff("hello")).toBe("+hello\n");
  });

  it("reads, writes, commits, resets main, creates a source ref, and opens a CR without token leakage", async () => {
    const baseline = "a".repeat(64);
    const update = "b".repeat(64);
    const requests: Request[] = [];
    let refsCall = 0;

    const fetchImpl: typeof fetch = async (input, init) => {
      const request = new Request(input, init);
      requests.push(request.clone());
      const url = new URL(request.url);
      const path = url.pathname;

      if (path === "/v1/capabilities") return jsonResponse(capabilities);

      if (path === "/vcs/commit") {
        return jsonResponse({ hash: refsCall === 0 ? "aaaaaaa" : "bbbbbbb", message: "ok", author: "root" });
      }

      if (path === "/vcs/refs" && request.method === "GET") {
        refsCall += 1;
        return jsonResponse({ refs: [refsCall === 1 ? mainRef(baseline, 1) : mainRef(update, 2)] });
      }

      if (path === "/vcs/refs/main" && request.method === "PATCH") {
        expect(await request.clone().json()).toEqual({
          expected_target: update,
          expected_version: 2,
          target: baseline,
        });
        return jsonResponse(mainRef(baseline, 3));
      }

      if (path === "/vcs/refs" && request.method === "POST") {
        const body = await request.clone().json();
        expect(body).toMatchObject({ target: update });
        expect(body.name).toMatch(/^agent\/incident-demo\/task7-/);
        return jsonResponse({ name: body.name, target: update, version: 1 }, 201);
      }

      if (path === "/change-requests" && request.method === "POST") {
        const body = await request.clone().json();
        expect(body).toMatchObject({ source_ref: expect.stringMatching(/^agent\/incident-demo\/task7-/), target_ref: "main" });
        return jsonResponse({
          change_request: {
            id: "cr-1",
            title: body.title,
            description: body.description,
            source_ref: body.source_ref,
            target_ref: "main",
            base_commit: baseline,
            head_commit: update,
            status: "open",
            created_by: 0,
            version: 1,
          },
          approval_state: {
            change_request_id: "cr-1",
            required_approvals: 0,
            approval_count: 0,
            approved_by: [],
            required_reviewers: [],
            approved_required_reviewers: [],
            missing_required_reviewers: [],
            approved: true,
            matched_ref_rules: [],
            matched_path_rules: [],
            require_all_files_viewed: false,
          },
          require_all_files_viewed: false,
        }, 201);
      }

      if (path === "/vcs/diff") return textResponse("diff --git a/root-cause.md b/root-cause.md\n");

      if (url.searchParams.get("stat") === "true") return dirStatResponse();

      if (path.endsWith("/evidence.md")) return textResponse("payment_service_timeout_rate: 7.4%\ncheckout_retry_rate: 3.1x\n");
      if (path.endsWith("/hypotheses.md")) return textResponse("Payment-service timeout regression\n");
      if (path.endsWith("/payment-service.md")) return textResponse("Inspect timeout and retry changes first.\n");
      if (path.endsWith("/researcher.md")) return textResponse("Payment-service deploys have previously introduced timeout regressions.\n");

      if (request.method === "PUT" && path.endsWith(".md")) {
        return jsonResponse({ written: path.replace(/^\/fs/, ""), size: (await request.clone().text()).length });
      }

      return jsonResponse({ error: `unexpected ${request.method} ${path}` }, 500);
    };

    const result = await runIncidentChangeRequestExample({
      env: {
        STRATUM_URL: "http://stratum.example",
        STRATUM_WORKSPACE_ID: "11111111-1111-1111-1111-111111111111",
        STRATUM_WORKSPACE_TOKEN: "workspace-secret",
      },
      fetch: fetchImpl,
      now: () => new Date("2026-06-10T00:00:00Z"),
    });

    expect(result.changeRequestId).toBe("cr-1");
    expect(result.targetRef).toBe("main");
    expect(result.sourceRef).toBe("agent/incident-demo/task7-20260610T000000000Z");
    expect(result.filesRead).toContain("/incidents/checkout-latency/evidence.md");
    expect(result.filesWritten).toEqual([
      "/incidents/checkout-latency/root-cause.md",
      "/incidents/checkout-latency/remediation.md",
    ]);
    expect(JSON.stringify(result)).not.toContain("workspace-secret");

    const workspaceRequests = requests.filter((request) => request.headers.get("X-Stratum-Workspace"));
    expect(workspaceRequests.map((request) => new URL(request.url).pathname)).toEqual(
      expect.arrayContaining([
        "/fs/incidents/checkout-latency/evidence.md",
        "/fs/incidents/checkout-latency/root-cause.md",
        "/fs/incidents/checkout-latency/remediation.md",
      ]),
    );
    expect(workspaceRequests.every((request) => request.headers.get("Authorization") === "Bearer workspace-secret")).toBe(true);

    const adminRequests = requests.filter((request) => new URL(request.url).pathname.startsWith("/vcs") || new URL(request.url).pathname.startsWith("/change-requests"));
    expect(adminRequests.every((request) => request.headers.get("Authorization") === "User root")).toBe(true);

    for (const request of requests) {
      expect(request.url).not.toContain("workspace-secret");
    }
  });
});
```

**Step 2: Run test to verify it fails**

Run:

```bash
bun run --cwd sdk/agents test:run -- incident-example.test.ts
```

Expected: FAIL because `sdk/agents/examples/incident-change-request.ts` does not exist.

## Task 2: Implement The Example Harness

**Files:**
- Create: `sdk/agents/examples/incident-change-request.ts`
- Optional modify: `sdk/agents/package.json`

**Step 1: Create the example file**

Create `sdk/agents/examples/incident-change-request.ts`.

Required exports:

- `loadIncidentExampleConfig(input?: NodeJS.ProcessEnv): IncidentExampleConfig`
- `createFileDiff(content: string): string`
- `fakeIncidentPlanner(input: IncidentPlannerInput): Promise<IncidentPatchPlan>`
- `runIncidentChangeRequestExample(options?: RunIncidentChangeRequestExampleOptions): Promise<IncidentExampleResult>`

Required implementation shape:

```ts
import type { ApplyPatchOperation } from "@openai/agents";
import { StratumClient, type StratumRef } from "@stratum/sdk";
import { StratumAgentWorkspace } from "../src/index.js";
import { StratumEditor } from "../src/openai/index.js";

const INCIDENT_FILES = {
  evidence: "/incidents/checkout-latency/evidence.md",
  hypotheses: "/incidents/checkout-latency/hypotheses.md",
  runbook: "/runbooks/payment-service.md",
  memory: "/memory/agents/researcher.md",
} as const;

const OUTPUT_FILES = {
  rootCause: "/incidents/checkout-latency/root-cause.md",
  remediation: "/incidents/checkout-latency/remediation.md",
} as const;

const TARGET_REF = "main";

export interface IncidentExampleConfig {
  readonly baseUrl: string;
  readonly workspaceId: string;
  readonly workspaceToken: string;
  readonly repoId?: string;
  readonly adminUser: string;
  readonly adapter: "openai-agents";
  readonly planner: "deterministic";
  readonly publicConfig: {
    readonly baseUrl: string;
    readonly workspaceId: string;
    readonly repoId?: string;
    readonly adminUser: string;
    readonly adapter: "openai-agents";
    readonly planner: "deterministic";
  };
}

export interface IncidentPlannerInput {
  readonly evidence: string;
  readonly hypotheses: string;
  readonly runbook: string;
  readonly memory: string;
}

export interface IncidentPatchPlan {
  readonly title: string;
  readonly description: string;
  readonly operations: readonly ApplyPatchOperation[];
}

export interface IncidentExampleResult {
  readonly workspaceId: string;
  readonly adapter: "openai-agents";
  readonly planner: "deterministic";
  readonly filesRead: readonly string[];
  readonly filesWritten: readonly string[];
  readonly baselineCommit: string;
  readonly updateCommit: string;
  readonly sourceRef: string;
  readonly targetRef: string;
  readonly changeRequestId: string;
  readonly diffPreview: string;
}

export interface RunIncidentChangeRequestExampleOptions {
  readonly env?: NodeJS.ProcessEnv;
  readonly fetch?: typeof fetch;
  readonly now?: () => Date;
  readonly planner?: (input: IncidentPlannerInput) => Promise<IncidentPatchPlan>;
}
```

Implementation requirements:

- `loadIncidentExampleConfig()` must require `STRATUM_URL`, `STRATUM_WORKSPACE_ID`, and `STRATUM_WORKSPACE_TOKEN`.
- `STRATUM_ADMIN_USER` defaults to `root`.
- `adapter` is always `"openai-agents"` and `planner` is always `"deterministic"` in this slice.
- `publicConfig` must omit `workspaceToken`.
- `createFileDiff("hello\nworld\n")` returns `"+hello\n+world\n"`.
- Use `new StratumClient({ auth: { type: "workspace", workspaceId, workspaceToken, repoId }, ... })` for workspace reads/writes.
- Use `new StratumClient({ auth: { type: "user", username: adminUser }, ... })` for local VCS/review routes.
- If `repoId` is present, wrap the provided fetch for the admin client and add `X-Stratum-Repo` without touching `Authorization`.
- Fetch capabilities once with the workspace client, then construct `new StratumAgentWorkspace({ client: workspaceClient, capabilities })`.
- Read all files in `INCIDENT_FILES` via `workspace.readFileText`.
- Apply planner operations via `new StratumEditor(workspace)`.
- Only permit `create_file` and `update_file` operations whose path is one of `OUTPUT_FILES`; reject deletes and unexpected paths.
- Create a baseline commit before agent writes, then read the full baseline `main` target from `admin.vcs.listRefs()`.
- Commit after agent writes, then read the full update `main` target/version from `admin.vcs.listRefs()`.
- Reset `main` back to the baseline with `admin.vcs.updateRef("main", { target: baseline.target, expected_target: update.target, expected_version: update.version })`.
- Create source ref `agent/incident-demo/task7-${timestamp}` pointing to the full update commit.
- Open the CR with `admin.reviews.createChangeRequest({ title, description, source_ref: sourceRef, target_ref: "main" })`.
- Fetch `admin.vcs.diff({ base: "main", head: sourceRef })` and return only the first 1200 characters as `diffPreview`.
- Do not print inside `runIncidentChangeRequestExample`; the CLI wrapper at bottom can print the returned JSON.

Helper requirements:

```ts
function requiredEnv(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]?.trim();
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

function sourceRefName(now: Date): string {
  return `agent/incident-demo/task7-${now.toISOString().replace(/[-:.]/g, "")}`;
}

function findMainRef(refs: readonly StratumRef[]): StratumRef {
  const main = refs.find((ref) => ref.name === TARGET_REF);
  if (main === undefined) throw new Error("Stratum main ref is unavailable.");
  return main;
}
```

Fake planner content should be deterministic and grounded in the seed files:

```ts
export async function fakeIncidentPlanner(input: IncidentPlannerInput): Promise<IncidentPatchPlan> {
  const mentionsTimeout = /timeout/i.test(`${input.evidence}\n${input.hypotheses}\n${input.runbook}\n${input.memory}`);
  const mentionsRetry = /retry/i.test(`${input.evidence}\n${input.hypotheses}\n${input.runbook}\n${input.memory}`);
  const rootCause = [
    "# Root Cause",
    "",
    "The most likely root cause is a payment-service timeout and retry regression introduced by the latest deploy.",
    "",
    "## Evidence",
    `- Timeout signal present: ${mentionsTimeout ? "yes" : "no"}.`,
    `- Retry signal present: ${mentionsRetry ? "yes" : "no"}.`,
    "- The payment-service runbook says to inspect timeout and retry changes first for checkout latency spikes.",
    "- Prior agent memory links payment-service deploys to timeout regressions.",
    "",
  ].join("\n");
  const remediation = [
    "# Remediation",
    "",
    "- Compare timeout budget and retry configuration against the previous payment-service release.",
    "- Roll back the payment-service deploy if timeout and retry pressure remain elevated.",
    "- Keep checkout latency and payment confirmation timeout dashboards open during rollback.",
    "",
  ].join("\n");
  return {
    title: "Investigate checkout latency root cause",
    description: "Agent-generated incident update from the Stratum seed-demo workspace.",
    operations: [
      { type: "create_file", path: OUTPUT_FILES.rootCause, diff: createFileDiff(rootCause) },
      { type: "create_file", path: OUTPUT_FILES.remediation, diff: createFileDiff(remediation) },
    ],
  };
}
```

**Step 2: Add a package script**

Modify `sdk/agents/package.json`:

```json
"example:incident": "bun run examples/incident-change-request.ts"
```

Do not change dependencies or run `bun install` for this task.

**Step 3: Run tests**

Run:

```bash
bun run --cwd sdk/agents test:run -- incident-example.test.ts
```

Expected: PASS.

## Task 3: Add Minimal Run Documentation

**Files:**
- Create: `sdk/agents/examples/README.md`
- Modify: `sdk/agents/README.md`

**Step 1: Write example README**

Create `sdk/agents/examples/README.md`:

````md
# Agent Adapter Examples

## Incident Change Request

This example runs after `stratumctl workspace seed-demo`. It reads the seeded incident files through the Stratum workspace token, applies an OpenAI Agents editor adapter patch, commits the update, resets `main` to the baseline, creates a source ref, and opens a Stratum change request.

Start `stratum-server`, run the seed demo, then:

```bash
source .stratum-demo/incident-workspace.env
bun run --cwd sdk/agents example:incident
```

The command prints workspace id, file paths, refs, change-request id, and a short diff preview. It never prints the workspace token.

Default mode is deterministic and does not call any model provider.
````

**Step 2: Link from agents README**

Add one short section to `sdk/agents/README.md` after Usage:

```md
## Runnable examples

See [`examples/README.md`](examples/README.md) for the incident change-request example. It consumes the env file from `stratumctl workspace seed-demo`, edits workspace files through the OpenAI Agents editor adapter, and opens a Stratum change request through the TypeScript SDK.
```

Do not add broad product copy.

## Task 4: Focused Verification

**Files:**
- All touched files from Tasks 1-3.

**Step 1: Run targeted checks**

Run:

```bash
bun run --cwd sdk/agents test:run -- incident-example.test.ts
bun run --cwd sdk/agents test:run
bun run --cwd sdk/agents typecheck
```

Expected: all pass.

**Step 2: Run SDK regression check**

Run:

```bash
bun run --cwd sdk/typescript test:run
```

Expected: pass.

**Step 3: Check formatting and whitespace**

Run:

```bash
git diff --check
```

Expected: no output.

**Step 4: Token leak audit**

Run targeted searches:

```bash
rg -n "workspace-secret|issued-workspace-secret|OPENAI_API_KEY=.*[A-Za-z0-9_-]{8,}|STRATUM_WORKSPACE_TOKEN=.*[A-Za-z0-9_-]{8,}|Bearer [A-Za-z0-9_-]{8,}" sdk/agents/examples sdk/agents/tests sdk/agents/README.md
```

Expected: no real secrets. The string `workspace-secret` may appear only inside test-only assertions and must not be printed by the example result.

## Done Criteria

- A fresh seeded local demo can run `bun run --cwd sdk/agents example:incident` after sourcing `.stratum-demo/incident-workspace.env`.
- Default mode is deterministic and provider-free.
- The example uses the existing OpenAI Agents adapter path for file writes.
- Workspace reads/writes use workspace bearer auth and Stratum SDK/volume routes.
- VCS/ref/change-request operations use SDK routes.
- The example opens a non-empty change request from a source ref to `main`.
- No tokens or API keys are printed, committed, snapshotted, or included in thrown public messages.
- No Rust code is touched unless a real existing bug blocks the TypeScript slice.

## Sidecar Implementer Boundary

The sidecar/smaller model should implement Tasks 1-4 only and stop. It should not commit, push, broaden this into CLI commands, add UI, redesign SDK APIs, or attempt durable-hosted admin support. The controller session will do final review, integration, commit, and push.
