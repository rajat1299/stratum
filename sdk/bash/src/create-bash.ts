import { TOOL_DESCRIPTION } from "./tool-description.js";
import type { BashPlaceholder, CreateBashOptions, CreateBashResult } from "./types.js";

function assertRequiredOption(
  options: Partial<CreateBashOptions>,
  field: keyof CreateBashOptions,
): asserts options is Partial<CreateBashOptions> & Record<typeof field, string> {
  const value = options[field];
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`createBash requires ${field}`);
  }
}

export async function createBash(options: CreateBashOptions): Promise<CreateBashResult> {
  assertRequiredOption(options, "baseUrl");
  assertRequiredOption(options, "workspaceId");
  assertRequiredOption(options, "workspaceToken");

  const bash: BashPlaceholder = {
    kind: "stratum-bash-placeholder",
  };

  return {
    options,
    bash,
    toolDescription: TOOL_DESCRIPTION,
    refresh: async () => {},
  };
}
