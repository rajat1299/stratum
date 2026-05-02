import type { BashExecResult, ExecOptions } from "just-bash";
import { Bash } from "just-bash";
import { StratumClient } from "./client.js";
import { stratumCommands } from "./commands.js";
import { FsError } from "./errors.js";
import { StratumFs } from "./stratum-fs.js";
import { TOOL_DESCRIPTION } from "./tool-description.js";
import type { CreateBashOptions, CreateBashResult } from "./types.js";
import { StratumVolume } from "./volume.js";

type RequiredStringOption = "baseUrl" | "workspaceId" | "workspaceToken";

function assertRequiredOption(
  options: Partial<CreateBashOptions>,
  field: RequiredStringOption,
): asserts options is Partial<CreateBashOptions> & Record<RequiredStringOption, string> {
  const value = options[field];
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`createBash requires ${field}`);
  }
}

export async function createBash(options: CreateBashOptions): Promise<CreateBashResult> {
  assertRequiredOption(options, "baseUrl");
  assertRequiredOption(options, "workspaceId");
  assertRequiredOption(options, "workspaceToken");

  const client = new StratumClient(options);
  const volume = new StratumVolume(client, { cacheOptions: options.cacheOptions });
  const fs = new StratumFs(volume);
  const env: Record<string, string> = { PATH: "", ...(options.env ?? {}) };

  const bash = new Bash({
    fs,
    cwd: "/",
    env,
    customCommands: stratumCommands,
    defenseInDepth: false,
    ...(options.executionLimits ? { executionLimits: options.executionLimits } : {}),
    ...(options.logger ? { logger: options.logger } : {}),
  });

  const originalExec = bash.exec.bind(bash);
  bash.exec = async (command: string, execOptions?: ExecOptions): Promise<BashExecResult> => {
    try {
      return await originalExec(command, execOptions);
    } catch (error) {
      if (error instanceof FsError) {
        return {
          stdout: "",
          stderr: `bash: ${error.message}\n`,
          exitCode: 1,
          env: bash.getEnv(),
        };
      }
      throw error;
    }
  };

  const refresh = async (): Promise<void> => {
    volume.cache.clear();
    volume.pathIndex.clear();
    await volume.ls("/");
  };

  return {
    bash,
    volume,
    fs,
    toolDescription: TOOL_DESCRIPTION,
    refresh,
  };
}
