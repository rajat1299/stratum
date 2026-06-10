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

const ALLOWED_OUTPUT_PATHS = new Set<string>(Object.values(OUTPUT_FILES));

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

function withRepoHeader(fetchImpl: typeof fetch, repoId: string): typeof fetch {
  return async (input, init) => {
    const request = new Request(input, init);
    const headers = new Headers(request.headers);
    headers.set("X-Stratum-Repo", repoId);
    return fetchImpl(new Request(request, { headers }));
  };
}

function redactSecret(value: string, secret: string): string {
  return secret === "" ? value : value.split(secret).join("[redacted]");
}

function redactIncidentExampleError(error: unknown, workspaceToken: string): Error {
  const message = error instanceof Error ? error.message : String(error);
  const redacted = new Error(redactSecret(message, workspaceToken));
  redacted.name = error instanceof Error ? error.name : "Error";
  if (error instanceof Error && error.stack !== undefined) {
    redacted.stack = redactSecret(error.stack, workspaceToken);
  }
  return redacted;
}

export function loadIncidentExampleConfig(input: NodeJS.ProcessEnv = process.env): IncidentExampleConfig {
  const baseUrl = requiredEnv(input, "STRATUM_URL");
  const workspaceId = requiredEnv(input, "STRATUM_WORKSPACE_ID");
  const workspaceToken = requiredEnv(input, "STRATUM_WORKSPACE_TOKEN");
  const repoId = input.STRATUM_REPO?.trim() || undefined;
  const adminUser = input.STRATUM_ADMIN_USER?.trim() || "root";

  const publicConfig = {
    baseUrl,
    workspaceId,
    ...(repoId !== undefined ? { repoId } : {}),
    adminUser,
    adapter: "openai-agents" as const,
    planner: "deterministic" as const,
  };

  return {
    baseUrl,
    workspaceId,
    workspaceToken,
    repoId,
    adminUser,
    adapter: "openai-agents",
    planner: "deterministic",
    publicConfig,
  };
}

export function createFileDiff(content: string): string {
  const parts = content.split("\n");
  const lines: string[] = [];
  for (let index = 0; index < parts.length; index += 1) {
    const line = parts[index];
    if (index === parts.length - 1 && line === "" && content.endsWith("\n")) {
      continue;
    }
    lines.push(`+${line}`);
  }
  return `${lines.join("\n")}\n`;
}

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

async function applyPatchPlan(
  editor: StratumEditor,
  operations: readonly ApplyPatchOperation[],
): Promise<readonly string[]> {
  const filesWritten: string[] = [];
  for (const operation of operations) {
    switch (operation.type) {
      case "delete_file":
        throw new Error(`Delete operations are not permitted in the incident example: ${operation.path}`);
      case "create_file":
      case "update_file": {
        if (!ALLOWED_OUTPUT_PATHS.has(operation.path)) {
          throw new Error(`Patch path is not permitted in the incident example: ${operation.path}`);
        }
        const result =
          operation.type === "create_file"
            ? await editor.createFile(operation)
            : await editor.updateFile(operation);
        if (result.status === "failed") {
          throw new Error(`Patch operation failed for ${operation.path}: ${result.output ?? "unknown error"}`);
        }
        filesWritten.push(operation.path);
        break;
      }
      default: {
        const unexpected: never = operation;
        throw new Error(`Unexpected patch operation type: ${(unexpected as ApplyPatchOperation).type}`);
      }
    }
  }
  return filesWritten;
}

export async function runIncidentChangeRequestExample(
  options: RunIncidentChangeRequestExampleOptions = {},
): Promise<IncidentExampleResult> {
  const env = options.env ?? process.env;
  const config = loadIncidentExampleConfig(env);
  try {
    return await runIncidentChangeRequestExampleWithConfig(config, options);
  } catch (error) {
    throw redactIncidentExampleError(error, config.workspaceToken);
  }
}

async function runIncidentChangeRequestExampleWithConfig(
  config: IncidentExampleConfig,
  options: RunIncidentChangeRequestExampleOptions,
): Promise<IncidentExampleResult> {
  const fetchImpl = options.fetch ?? globalThis.fetch;
  if (!fetchImpl) {
    throw new Error("runIncidentChangeRequestExample requires fetch");
  }
  const now = options.now ?? (() => new Date());
  const planner = options.planner ?? fakeIncidentPlanner;

  const workspaceClient = new StratumClient({
    baseUrl: config.baseUrl,
    auth: {
      type: "workspace",
      workspaceId: config.workspaceId,
      workspaceToken: config.workspaceToken,
      repoId: config.repoId,
    },
    fetch: fetchImpl,
  });

  const adminFetch =
    config.repoId !== undefined ? withRepoHeader(fetchImpl, config.repoId) : fetchImpl;

  const adminClient = new StratumClient({
    baseUrl: config.baseUrl,
    auth: { type: "user", username: config.adminUser },
    fetch: adminFetch,
  });

  const capabilities = await workspaceClient.getCapabilities();
  const workspace = new StratumAgentWorkspace({ client: workspaceClient, capabilities });

  const filesRead: string[] = [];
  const evidence = await workspace.readFileText(INCIDENT_FILES.evidence);
  filesRead.push(INCIDENT_FILES.evidence);
  const hypotheses = await workspace.readFileText(INCIDENT_FILES.hypotheses);
  filesRead.push(INCIDENT_FILES.hypotheses);
  const runbook = await workspace.readFileText(INCIDENT_FILES.runbook);
  filesRead.push(INCIDENT_FILES.runbook);
  const memory = await workspace.readFileText(INCIDENT_FILES.memory);
  filesRead.push(INCIDENT_FILES.memory);

  await adminClient.vcs.commit("Baseline before agent incident update");

  const baselineRefs = await adminClient.vcs.listRefs();
  const baselineMain = findMainRef(baselineRefs.refs);
  const baselineCommit = baselineMain.target;

  const plan = await planner({ evidence, hypotheses, runbook, memory });

  const editor = new StratumEditor(workspace);
  const filesWritten = await applyPatchPlan(editor, plan.operations);

  await adminClient.vcs.commit(plan.title);

  const updateRefs = await adminClient.vcs.listRefs();
  const updateMain = findMainRef(updateRefs.refs);
  const updateCommit = updateMain.target;

  await adminClient.vcs.updateRef(TARGET_REF, {
    target: baselineMain.target,
    expected_target: updateMain.target,
    expected_version: updateMain.version,
  });

  const sourceRef = sourceRefName(now());
  await adminClient.vcs.createRef({ name: sourceRef, target: updateCommit });

  const changeRequest = await adminClient.reviews.createChangeRequestFromSession({
    title: plan.title,
    description: plan.description,
    session_ref: sourceRef,
    target_ref: TARGET_REF,
  });

  const diff = await adminClient.vcs.diff({ base: TARGET_REF, head: sourceRef });
  const diffPreview = diff.slice(0, 1200);

  return {
    workspaceId: config.workspaceId,
    adapter: config.adapter,
    planner: config.planner,
    filesRead,
    filesWritten,
    baselineCommit,
    updateCommit,
    sourceRef,
    targetRef: TARGET_REF,
    changeRequestId: changeRequest.change_request.id,
    diffPreview,
  };
}

if (import.meta.main) {
  const result = await runIncidentChangeRequestExample();
  console.log(JSON.stringify(result, null, 2));
}
