import { applyDiff } from "@openai/agents";
import type { ApplyPatchOperation, ApplyPatchResult, Editor } from "@openai/agents";
import { StratumAgentWorkspace } from "../workspace.js";

/**
 * OpenAI Agents {@link Editor} backed by a mounted Stratum workspace.
 *
 * All edits go through `@stratum/sdk` file routes; there is no host filesystem
 * access. Failure outputs reference only the model-supplied workspace path.
 */
export class StratumEditor implements Editor {
  constructor(private readonly workspace: StratumAgentWorkspace) {}

  async createFile(
    operation: Extract<ApplyPatchOperation, { type: "create_file" }>,
  ): Promise<ApplyPatchResult> {
    const content = applyDiff("", operation.diff, "create");
    await this.workspace.writeFile(operation.path, content);
    return { status: "completed" };
  }

  async updateFile(
    operation: Extract<ApplyPatchOperation, { type: "update_file" }>,
  ): Promise<ApplyPatchResult> {
    let current: string;
    try {
      current = await this.workspace.readFileText(operation.path);
    } catch {
      return { status: "failed", output: `File not found: ${operation.path}` };
    }
    const next = applyDiff(current, operation.diff);
    await this.workspace.writeFile(operation.path, next);
    return { status: "completed" };
  }

  async deleteFile(
    operation: Extract<ApplyPatchOperation, { type: "delete_file" }>,
  ): Promise<ApplyPatchResult> {
    if (!(await this.workspace.exists(operation.path))) {
      return { status: "failed", output: `File not found: ${operation.path}` };
    }
    await this.workspace.volume.deletePath(operation.path);
    return { status: "completed" };
  }
}
