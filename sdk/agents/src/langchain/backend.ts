import type {
  EditResult,
  ExecuteResponse,
  FileDownloadResponse,
  FileUploadResponse,
  GlobResult,
  GrepResult,
  LsResult,
  ReadRawResult,
  ReadResult,
  SandboxBackendProtocolV2,
  WriteResult,
} from "deepagents";
import { UnsupportedFeatureError } from "@stratum/sdk";
import { classifyMime, resolveMimeType } from "../mime.js";
import { StratumAgentEditError, StratumAgentWorkspace, isHttpNotFound } from "../workspace.js";
import { globToFileInfos, toFileInfos, toGrepMatches, unixSecondsToIso } from "./convert.js";

const EXECUTION_UNAVAILABLE = "Stratum execution is unavailable for this workspace.";

export interface StratumLangChainWorkspaceOptions {
  readonly sandboxId?: string;
}

/**
 * deepagents {@link SandboxBackendProtocolV2} backed by a mounted Stratum
 * workspace.
 *
 * File operations use `@stratum/sdk`; grep and glob use the SDK search/listing
 * routes plus `picomatch`, never a shell. Execution uses the Stratum `/execute`
 * route only and reports an explicit unavailable result (no fake success) when
 * execution is disabled. No result surface echoes tokens, raw provider errors,
 * or backing/temp paths.
 */
export class StratumLangChainWorkspace implements SandboxBackendProtocolV2 {
  readonly id: string;
  private readonly workspace: StratumAgentWorkspace;

  constructor(workspace: StratumAgentWorkspace, options: StratumLangChainWorkspaceOptions = {}) {
    this.workspace = workspace;
    this.id = options.sandboxId ?? "stratum";
  }

  async execute(command: string): Promise<ExecuteResponse> {
    try {
      const result = await this.workspace.execute(command);
      const output =
        result.stderr.length > 0
          ? result.stdout.length > 0
            ? `${result.stdout}\n${result.stderr}`
            : result.stderr
          : result.stdout;
      return {
        output,
        exitCode: result.exitCode,
        truncated: result.stdoutTruncated || result.stderrTruncated,
      };
    } catch (error) {
      if (error instanceof UnsupportedFeatureError) {
        return { output: EXECUTION_UNAVAILABLE, exitCode: null, truncated: false };
      }
      return { output: "Stratum execution failed.", exitCode: null, truncated: false };
    }
  }

  async ls(path: string): Promise<LsResult> {
    try {
      return { files: toFileInfos(await this.workspace.listFiles(path)) };
    } catch (error) {
      return { error: filesystemErrorMessage(error) };
    }
  }

  async read(filePath: string, offset = 0, limit = 500): Promise<ReadResult> {
    let mimeType: string;
    try {
      const stat = await this.workspace.stat(filePath);
      mimeType = resolveMimeType(filePath, stat.mime_type);
    } catch (error) {
      return { error: readErrorMessage(error, filePath) };
    }

    const contentClass = classifyMime(mimeType);
    try {
      if (contentClass === "media") {
        return { content: await this.workspace.readFileBytes(filePath), mimeType };
      }
      if (contentClass === "binary") {
        const bytes = await this.workspace.readFileBytes(filePath);
        return {
          error: `Binary file '${filePath}' (${mimeType}, ${String(bytes.length)} bytes). Not rendered as text.`,
        };
      }
      const text = await this.workspace.readFileText(filePath);
      const lines = text.split("\n");
      if (offset > 0 && offset >= lines.length) {
        return { error: `Line offset ${String(offset)} exceeds file length (${String(lines.length)} lines)` };
      }
      const end = Math.min(offset + limit, lines.length);
      return { content: lines.slice(offset, end).join("\n"), mimeType };
    } catch (error) {
      return { error: readErrorMessage(error, filePath) };
    }
  }

  async readRaw(filePath: string): Promise<ReadRawResult> {
    try {
      const stat = await this.workspace.stat(filePath);
      const mimeType = resolveMimeType(filePath, stat.mime_type);
      const content =
        classifyMime(mimeType) === "text"
          ? await this.workspace.readFileText(filePath)
          : await this.workspace.readFileBytes(filePath);
      return {
        data: {
          content,
          mimeType,
          created_at: unixSecondsToIso(stat.created),
          modified_at: unixSecondsToIso(stat.modified),
        },
      };
    } catch (error) {
      return { error: readErrorMessage(error, filePath) };
    }
  }

  async write(filePath: string, content: string): Promise<WriteResult> {
    try {
      if (await this.workspace.exists(filePath)) {
        return { error: `Error: file '${filePath}' already exists` };
      }
      await this.workspace.writeFile(filePath, content);
      return { path: filePath };
    } catch (error) {
      return { error: filesystemErrorMessage(error) };
    }
  }

  async edit(filePath: string, oldString: string, newString: string, replaceAll = false): Promise<EditResult> {
    try {
      return await this.workspace.editFile(filePath, oldString, newString, replaceAll);
    } catch (error) {
      return { error: editErrorMessage(error, filePath) };
    }
  }

  async grep(pattern: string, path?: string | null, _glob?: string | null): Promise<GrepResult> {
    try {
      const result = await this.workspace.grep(pattern, path ?? "/", true);
      return { matches: toGrepMatches(result) };
    } catch (error) {
      return { error: filesystemErrorMessage(error) };
    }
  }

  async glob(pattern: string, path = "/"): Promise<GlobResult> {
    try {
      return { files: globToFileInfos(await this.workspace.glob(pattern, path)) };
    } catch (error) {
      return { error: filesystemErrorMessage(error) };
    }
  }

  async uploadFiles(files: readonly (readonly [string, Uint8Array])[]): Promise<FileUploadResponse[]> {
    const results: FileUploadResponse[] = [];
    for (const [path, data] of files) {
      try {
        await this.workspace.writeFile(path, data);
        results.push({ path, error: null });
      } catch (error) {
        results.push({ path, error: error instanceof UnsupportedFeatureError ? "permission_denied" : "invalid_path" });
      }
    }
    return results;
  }

  async downloadFiles(paths: readonly string[]): Promise<FileDownloadResponse[]> {
    const results: FileDownloadResponse[] = [];
    for (const path of paths) {
      try {
        results.push({ path, content: await this.workspace.readFileBytes(path), error: null });
      } catch (error) {
        const failure =
          error instanceof UnsupportedFeatureError
            ? "permission_denied"
            : isHttpNotFound(error)
              ? "file_not_found"
              : "invalid_path";
        results.push({ path, content: null, error: failure });
      }
    }
    return results;
  }
}

function filesystemErrorMessage(error: unknown): string {
  if (error instanceof UnsupportedFeatureError) return error.message;
  return "Stratum filesystem operation failed.";
}

function readErrorMessage(error: unknown, filePath: string): string {
  if (isHttpNotFound(error)) return `Error: file '${filePath}' not found`;
  if (error instanceof UnsupportedFeatureError) return error.message;
  return "Stratum read failed.";
}

function editErrorMessage(error: unknown, filePath: string): string {
  if (isHttpNotFound(error)) return `Error: file '${filePath}' not found`;
  if (error instanceof UnsupportedFeatureError) return error.message;
  if (error instanceof StratumAgentEditError) return error.message;
  return "Stratum edit failed.";
}
