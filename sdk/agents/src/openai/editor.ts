import { applyDiff } from "@openai/agents";
import type { ApplyPatchOperation, ApplyPatchResult, Editor } from "@openai/agents";
import { UnsupportedFeatureError } from "@stratum/sdk";
import { StratumAgentWorkspace, isHttpNotFound } from "../workspace.js";

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
    try {
      await this.workspace.writeFile(operation.path, content);
      return { status: "completed" };
    } catch (error) {
      return filesystemFailure(error, operation.path);
    }
  }

  async updateFile(
    operation: Extract<ApplyPatchOperation, { type: "update_file" }>,
  ): Promise<ApplyPatchResult> {
    let current: string;
    try {
      current = await this.workspace.readFileText(operation.path);
    } catch (error) {
      return filesystemFailure(error, operation.path);
    }
    const next = applyDiff(current, operation.diff);
    try {
      await this.workspace.writeFile(operation.path, next);
      return { status: "completed" };
    } catch (error) {
      return filesystemFailure(error, operation.path);
    }
  }

  async deleteFile(
    operation: Extract<ApplyPatchOperation, { type: "delete_file" }>,
  ): Promise<ApplyPatchResult> {
    try {
      if (!(await this.workspace.exists(operation.path))) {
        return { status: "failed", output: `File not found: ${operation.path}` };
      }
      await this.workspace.deleteFile(operation.path);
      return { status: "completed" };
    } catch (error) {
      return filesystemFailure(error, operation.path);
    }
  }
}

function filesystemFailure(error: unknown, path: string): ApplyPatchResult {
  if (isHttpNotFound(error)) return { status: "failed", output: `File not found: ${path}` };
  if (error instanceof UnsupportedFeatureError) return { status: "failed", output: error.message };
  return { status: "failed", output: "Stratum filesystem operation failed." };
}
