import { tool } from "ai";
import { z } from "zod";
import { UnsupportedFeatureError } from "@stratum/sdk";
import { classifyMime, resolveMimeType } from "../mime.js";
import { StratumAgentEditError, StratumAgentWorkspace, isHttpNotFound } from "../workspace.js";

export { STRATUM_SYSTEM_PROMPT, buildStratumSystemPrompt } from "../prompt.js";
export type { BuildStratumSystemPromptOptions } from "../prompt.js";

const EXECUTION_UNAVAILABLE = "Stratum execution is unavailable for this workspace.";

type ReadFileResult =
  | { kind: "text"; path: string; mimeType: string; content: string; bytes: number }
  | { kind: "media"; path: string; mimeType: string; base64: string; bytes: number }
  | { kind: "binary"; path: string; mimeType: string; bytes: number; note: string }
  | { error: string };

function isReadError(result: ReadFileResult): result is { error: string } {
  return "error" in result;
}

/**
 * Build a Vercel AI SDK tool set bound to a mounted Stratum workspace.
 *
 * File tools use `@stratum/sdk` routes; the execute tool uses the Stratum
 * `/execute` route only and returns a stable error object when execution is
 * unavailable. No tool shells out, and no error surface echoes a raw command,
 * token, provider error, or backing path.
 */
export function stratumTools(workspace: StratumAgentWorkspace) {
  return {
    execute: tool({
      description:
        "Run a command through Stratum execution and return stdout, stderr, and exitCode. Only available when the workspace capability manifest enables execution.",
      inputSchema: z.object({
        command: z.string().describe("The command to execute in the Stratum workspace."),
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

    readFile: tool({
      description:
        "Read a file from the Stratum workspace. Text files return text; images and PDFs return base64 media; other binaries return a metadata stub.",
      inputSchema: z.object({
        path: z.string().describe("Workspace-relative path to read."),
      }),
      execute: async ({ path }): Promise<ReadFileResult> => {
        let mimeType: string;
        try {
          const stat = await workspace.stat(path);
          mimeType = resolveMimeType(path, stat.mime_type);
        } catch (error) {
          if (isHttpNotFound(error)) return { error: `File not found: ${path}` };
          if (error instanceof UnsupportedFeatureError) return { error: error.message };
          return { error: "Failed to read file." };
        }
        const contentClass = classifyMime(mimeType);
        try {
          if (contentClass === "text") {
            const content = await workspace.readFileText(path);
            return { kind: "text", path, mimeType, content, bytes: byteLength(content) };
          }
          const bytes = await workspace.readFileBytes(path);
          if (contentClass === "media") {
            return {
              kind: "media",
              path,
              mimeType,
              base64: Buffer.from(bytes).toString("base64"),
              bytes: bytes.length,
            };
          }
          return {
            kind: "binary",
            path,
            mimeType,
            bytes: bytes.length,
            note: `Binary file ${path} (${mimeType}, ${String(bytes.length)} bytes). Not rendered as text.`,
          };
        } catch (error) {
          if (error instanceof UnsupportedFeatureError) return { error: error.message };
          return { error: "Failed to read file." };
        }
      },
      toModelOutput: ({ output }) => {
        const result = output as ReadFileResult;
        if (isReadError(result)) return { type: "error-text", value: result.error };
        if (result.kind === "text") return { type: "text", value: result.content };
        if (result.kind === "media") {
          return {
            type: "content",
            value: [
              { type: "text", text: `[${result.path}] ${result.mimeType} (${String(result.bytes)} bytes)` },
              { type: "file-data", data: result.base64, mediaType: result.mimeType },
            ],
          };
        }
        return { type: "text", value: result.note };
      },
    }),

    writeFile: tool({
      description: "Write UTF-8 text to a file in the Stratum workspace, creating missing parent directories.",
      inputSchema: z.object({
        path: z.string().describe("Workspace-relative path to write."),
        content: z.string().describe("UTF-8 text content."),
      }),
      execute: async ({ path, content }) => {
        try {
          await workspace.writeFile(path, content);
          return { path };
        } catch (error) {
          return { error: writeErrorMessage(error) };
        }
      },
    }),

    editFile: tool({
      description:
        "Replace an exact string inside an existing file. Fails when the string appears more than once unless replaceAll is true.",
      inputSchema: z.object({
        path: z.string().describe("Workspace-relative path of the file to edit."),
        oldString: z.string().describe("Exact string to replace."),
        newString: z.string().describe("Replacement string."),
        replaceAll: z.boolean().optional().describe("Replace every occurrence instead of requiring a unique match."),
      }),
      execute: async ({ path, oldString, newString, replaceAll }) => {
        try {
          return await workspace.editFile(path, oldString, newString, replaceAll ?? false);
        } catch (error) {
          return { error: editErrorMessage(error, path) };
        }
      },
    }),

    ls: tool({
      description: "List the entries of a directory in the Stratum workspace.",
      inputSchema: z.object({
        path: z.string().describe("Workspace-relative directory path."),
      }),
      execute: async ({ path }) => {
        try {
          const files = await workspace.listFiles(path);
          return { files: files.map((entry) => ({ path: entry.path, is_dir: entry.is_dir })) };
        } catch (error) {
          return { error: writeErrorMessage(error) };
        }
      },
    }),
  };
}

function byteLength(value: string): number {
  return new TextEncoder().encode(value).length;
}

function writeErrorMessage(error: unknown): string {
  if (error instanceof UnsupportedFeatureError) return error.message;
  return "Stratum filesystem operation failed.";
}

function editErrorMessage(error: unknown, path: string): string {
  if (isHttpNotFound(error)) return `File not found: ${path}`;
  if (error instanceof UnsupportedFeatureError) return error.message;
  if (error instanceof StratumAgentEditError) return error.message;
  return "Stratum edit failed.";
}
