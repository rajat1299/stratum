import type { TaskContext } from "vitest";
import { StratumClient, type WorkspaceRecord } from "../src/index.js";

export interface LiveSmokeConfig {
  readonly baseUrl: string;
  readonly adminUser: string;
  readonly agentToken: string;
  readonly workspaceName: string;
  readonly workspaceRoot: string;
}

export function liveConfigOrSkip(ctx: TaskContext): LiveSmokeConfig {
  if (process.env.STRATUM_SDK_LIVE !== "1") {
    ctx.skip("Live smoke disabled: set STRATUM_SDK_LIVE=1");
  }

  const baseUrl = process.env.STRATUM_SDK_LIVE_BASE_URL?.trim();
  const adminUser = process.env.STRATUM_SDK_LIVE_ADMIN_USER?.trim();
  const agentToken = process.env.STRATUM_SDK_LIVE_AGENT_TOKEN?.trim();

  if (!baseUrl || !adminUser || !agentToken) {
    ctx.skip(
      "Missing STRATUM_SDK_LIVE_BASE_URL, STRATUM_SDK_LIVE_ADMIN_USER, and/or STRATUM_SDK_LIVE_AGENT_TOKEN",
    );
  }

  const suffix = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  return {
    baseUrl,
    adminUser,
    agentToken,
    workspaceName: `sdk-smoke-${suffix}`,
    workspaceRoot: `/sdk-smoke/${suffix}`,
  };
}

export async function createLiveWorkspace(
  client: StratumClient,
  config: Pick<LiveSmokeConfig, "workspaceName" | "workspaceRoot" | "agentToken">,
): Promise<{ workspace: WorkspaceRecord; workspaceToken: string }> {
  const workspace = await client.workspaces.create({
    name: config.workspaceName,
    root_path: config.workspaceRoot,
  });

  const issued = await client.workspaces.issueToken(workspace.id, {
    name: `${config.workspaceName}-token`,
    agent_token: config.agentToken,
    read_prefixes: [config.workspaceRoot],
    write_prefixes: [config.workspaceRoot],
  });

  return { workspace, workspaceToken: issued.workspace_token };
}
