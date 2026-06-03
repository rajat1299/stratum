import {
  StratumHttpError,
  StratumVolume,
  UnsupportedFeatureError,
  type CapabilityManifest,
  type CapabilityRouteOperation,
  type ExecuteRunResult,
  type StratumClient,
  type StratumGrepResult,
  type StratumStat,
} from "@stratum/sdk";
import picomatch from "picomatch";

export interface StratumAgentWorkspaceOptions {
  readonly client: StratumClient;
  readonly capabilities: CapabilityManifest;
  readonly volume?: StratumVolume;
}

export interface StratumWorkspaceEntry {
  readonly path: string;
  readonly is_dir: boolean;
}

export interface StratumAgentExecuteResult {
  readonly stdout: string;
  readonly stderr: string;
  readonly exitCode: number | null;
  readonly status: ExecuteRunResult["status"];
  readonly runId: string;
  readonly jobId: string;
  readonly stdoutTruncated: boolean;
  readonly stderrTruncated: boolean;
}

/**
 * A capability-gated facade over a mounted Stratum workspace.
 *
 * All file behavior goes through `@stratum/sdk` / {@link StratumVolume}. Execution
 * goes through the Stratum `/execute` route only and only when the capability
 * manifest reports it available; there is no host-shell fallback. Thrown errors
 * are bounded and never echo tokens, commands, output, or backing paths.
 */
export class StratumAgentWorkspace {
  readonly client: StratumClient;
  readonly capabilities: CapabilityManifest;
  readonly volume: StratumVolume;

  constructor(options: StratumAgentWorkspaceOptions) {
    this.client = options.client;
    this.capabilities = options.capabilities;
    this.volume = options.volume ?? options.client.mount();
  }

  canExecute(): boolean {
    return this.capabilities.routes.execute.available === true;
  }

  async readFileText(path: string): Promise<string> {
    this.requireRoute("filesystem.read", this.capabilities.routes.filesystem.read);
    return this.volume.readFile(path);
  }

  async readFileBytes(path: string): Promise<Uint8Array> {
    this.requireRoute("filesystem.read", this.capabilities.routes.filesystem.read);
    return this.volume.readFileBuffer(path);
  }

  async writeFile(path: string, content: string | Uint8Array): Promise<{ path: string }> {
    this.requireRoute("filesystem.write", this.capabilities.routes.filesystem.write);
    await this.ensureParent(path);
    await this.volume.writeFile(path, content);
    return { path };
  }

  async stat(path: string): Promise<StratumStat> {
    this.requireRoute("filesystem.stat", this.capabilities.routes.filesystem.stat);
    return this.volume.stat(path);
  }

  async exists(path: string): Promise<boolean> {
    try {
      await this.stat(path);
      return true;
    } catch (error) {
      if (isHttpNotFound(error)) return false;
      throw error;
    }
  }

  async isDirectory(path: string): Promise<boolean> {
    return (await this.stat(path)).kind === "directory";
  }

  async listFiles(path: string): Promise<readonly StratumWorkspaceEntry[]> {
    this.requireRoute("filesystem.list", this.capabilities.routes.filesystem.list);
    const listing = await this.volume.listDirectory(path);
    return listing.entries.map((entry) => ({
      path: joinWorkspacePath(path, entry.name),
      is_dir: entry.is_dir,
    }));
  }

  async editFile(
    path: string,
    oldString: string,
    newString: string,
    replaceAll = false,
  ): Promise<{ path: string; occurrences: number }> {
    const current = await this.readFileText(path);
    const count = current.split(oldString).length - 1;
    if (count === 0) throw new Error("string not found in file");
    if (count > 1 && replaceAll !== true) {
      throw new Error(`string appears ${count} times; set replaceAll=true`);
    }
    const next = replaceAll ? current.split(oldString).join(newString) : current.replace(oldString, newString);
    await this.writeFile(path, next);
    return { path, occurrences: replaceAll ? count : 1 };
  }

  async glob(pattern: string, path = "/"): Promise<readonly string[]> {
    this.requireRoute("filesystem.list", this.capabilities.routes.filesystem.list);
    const isMatch = picomatch(pattern, { dot: true });
    const matches: string[] = [];
    const walk = async (dir: string): Promise<void> => {
      const listing = await this.volume.listDirectory(dir);
      for (const entry of listing.entries) {
        const full = joinWorkspacePath(dir, entry.name);
        if (entry.is_dir) {
          await walk(full);
        } else if (isMatch(matchablePath(full))) {
          matches.push(full);
        }
      }
    };
    await walk(path);
    return matches;
  }

  async grep(pattern: string, path = "/", recursive = true): Promise<StratumGrepResult> {
    this.requireRoute("search.grep", this.capabilities.routes.search.grep);
    return this.volume.grep(pattern, path, recursive);
  }

  async execute(command: string, prompt?: string): Promise<StratumAgentExecuteResult> {
    if (!this.canExecute()) {
      throw new UnsupportedFeatureError("Stratum execution is unavailable for this workspace.");
    }
    const result = await this.client.execute.run({ command, prompt });
    return {
      stdout: result.stdout,
      stderr: result.stderr,
      exitCode: result.exit_code,
      status: result.status,
      runId: result.run_id,
      jobId: result.job_id,
      stdoutTruncated: result.stdout_truncated,
      stderrTruncated: result.stderr_truncated,
    };
  }

  private async ensureParent(path: string): Promise<void> {
    const parent = parentOf(path);
    if (parent === "/" || parent === "") return;
    if (await this.exists(parent)) return;
    await this.ensureParent(parent);
    try {
      await this.volume.mkdir(parent);
    } catch (error) {
      if (!(await this.exists(parent))) throw error;
    }
  }

  private requireRoute(name: string, route: CapabilityRouteOperation): void {
    if (route.available !== true) {
      throw new UnsupportedFeatureError(`${name} is unavailable for this Stratum workspace.`);
    }
  }
}

export function joinWorkspacePath(parent: string, name: string): string {
  const base = parent.replace(/\/+$/, "");
  const leaf = name.replace(/^\/+/, "");
  if (base === "" || base === "/") return `/${leaf}`;
  return `${base}/${leaf}`;
}

export function parentOf(path: string): string {
  const trimmed = path.replace(/\/+$/, "");
  const idx = trimmed.lastIndexOf("/");
  if (idx <= 0) return "/";
  return trimmed.slice(0, idx);
}

export function isHttpNotFound(error: unknown): boolean {
  return error instanceof StratumHttpError && error.status === 404;
}

function matchablePath(path: string): string {
  return path.replace(/^\/+/, "");
}
