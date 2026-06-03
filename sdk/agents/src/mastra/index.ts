import { createTool } from "@mastra/core/tools";
import { z } from "zod";
import { UnsupportedFeatureError } from "@stratum/sdk";
import { StratumAgentWorkspace, isHttpNotFound } from "../workspace.js";

export { STRATUM_SYSTEM_PROMPT, buildStratumSystemPrompt } from "../prompt.js";
export type { BuildStratumSystemPromptOptions } from "../prompt.js";

const EXECUTION_UNAVAILABLE = "Stratum execution is unavailable for this workspace.";

/**
 * Build a Mastra tool set bound to a mounted Stratum workspace.
 *
 * File tools use `@stratum/sdk`; the execute tool uses the Stratum `/execute`
 * route only and returns a stable unsupported error object when execution is
 * unavailable. No tool shells out, and outputs are bounded to metadata/text
 * fields and stable error strings that never echo a raw command, token,
 * provider error, or backing path.
 */
export function stratumTools(workspace: StratumAgentWorkspace) {
  return {
    execute: createTool({
      id: "stratum-execute",
      description:
        "Run a command through Stratum execution and return stdout, stderr, and exitCode. Only available when the workspace capability manifest enables execution.",
      inputSchema: z.object({
        command: z.string().describe("The command to execute in the Stratum workspace."),
      }),
      outputSchema: z.object({
        stdout: z.string().optional(),
        stderr: z.string().optional(),
        exitCode: z.number().nullable().optional(),
        error: z.string().optional(),
      }),
      execute: async ({ command }) => {
        try {
          const result = await workspace.execute(command);
          return { stdout: result.stdout, stderr: result.stderr, exitCode: result.exitCode };
        } catch (error) {
          if (error instanceof UnsupportedFeatureError) return { error: EXECUTION_UNAVAILABLE };
          return { error: "Stratum execution failed." };
        }
      },
    }),

    readFile: createTool({
      id: "stratum-read-file",
      description: "Read the contents of a file from the Stratum workspace as UTF-8 text.",
      inputSchema: z.object({
        path: z.string().describe("Workspace-relative path to read."),
      }),
      outputSchema: z.object({
        content: z.string().optional(),
        error: z.string().optional(),
      }),
      execute: async ({ path }) => {
        try {
          return { content: await workspace.readFileText(path) };
        } catch (error) {
          return { error: readErrorMessage(error, path) };
        }
      },
    }),

    writeFile: createTool({
      id: "stratum-write-file",
      description: "Write UTF-8 text to a file in the Stratum workspace, creating missing parent directories.",
      inputSchema: z.object({
        path: z.string().describe("Workspace-relative path to write."),
        content: z.string().describe("UTF-8 text content."),
      }),
      outputSchema: z.object({
        path: z.string().optional(),
        error: z.string().optional(),
      }),
      execute: async ({ path, content }) => {
        try {
          await workspace.writeFile(path, content);
          return { path };
        } catch (error) {
          return { error: filesystemErrorMessage(error) };
        }
      },
    }),

    editFile: createTool({
      id: "stratum-edit-file",
      description:
        "Replace an exact string inside an existing file. Fails when the string appears more than once unless replaceAll is true.",
      inputSchema: z.object({
        path: z.string().describe("Workspace-relative path of the file to edit."),
        oldString: z.string().describe("Exact string to replace."),
        newString: z.string().describe("Replacement string."),
        replaceAll: z.boolean().optional().describe("Replace every occurrence instead of requiring a unique match."),
      }),
      outputSchema: z.object({
        path: z.string().optional(),
        occurrences: z.number().optional(),
        error: z.string().optional(),
      }),
      execute: async ({ path, oldString, newString, replaceAll }) => {
        try {
          return await workspace.editFile(path, oldString, newString, replaceAll ?? false);
        } catch (error) {
          return { error: editErrorMessage(error, path) };
        }
      },
    }),

    ls: createTool({
      id: "stratum-ls",
      description: "List the entries of a directory in the Stratum workspace.",
      inputSchema: z.object({
        path: z.string().describe("Workspace-relative directory path."),
      }),
      outputSchema: z.object({
        files: z.array(z.object({ path: z.string(), is_dir: z.boolean() })).optional(),
        error: z.string().optional(),
      }),
      execute: async ({ path }) => {
        try {
          const files = await workspace.listFiles(path);
          return { files: files.map((entry) => ({ path: entry.path, is_dir: entry.is_dir })) };
        } catch (error) {
          return { error: filesystemErrorMessage(error) };
        }
      },
    }),
  };
}

function filesystemErrorMessage(error: unknown): string {
  if (error instanceof UnsupportedFeatureError) return error.message;
  return "Stratum filesystem operation failed.";
}

function readErrorMessage(error: unknown, path: string): string {
  if (isHttpNotFound(error)) return `Error: file '${path}' not found`;
  if (error instanceof UnsupportedFeatureError) return error.message;
  return "Stratum read failed.";
}

function editErrorMessage(error: unknown, path: string): string {
  if (isHttpNotFound(error)) return `Error: file '${path}' not found`;
  if (error instanceof UnsupportedFeatureError) return error.message;
  if (error instanceof Error) return error.message;
  return "Stratum edit failed.";
}
