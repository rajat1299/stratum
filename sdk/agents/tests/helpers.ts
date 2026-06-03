// Provider-free test helpers: an in-memory fake Stratum workspace.
//
// These build a real `StratumVolume` over an in-memory `StratumVolumeClient`
// plus a fake `StratumClient.execute` so adapter tests exercise the real path
// normalization and capability gating without any network, model, or provider.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  StratumHttpError,
  StratumVolume,
  type CapabilityManifest,
  type ExecuteRunResult,
  type StratumClient,
  type StratumDirectoryEntry,
  type StratumDirectoryListing,
  type StratumGrepResult,
  type StratumStat,
  type StratumVolumeClient,
} from "@stratum/sdk";
import { StratumAgentWorkspace } from "../src/index.js";

const baseManifest = JSON.parse(
  readFileSync(
    fileURLToPath(new URL("../../contracts/capabilities.v1.json", import.meta.url)),
    "utf8",
  ),
) as CapabilityManifest;

export interface FakeExecuteOutcome {
  readonly stdout?: string;
  readonly stderr?: string;
  readonly exitCode?: number | null;
  readonly status?: ExecuteRunResult["status"];
  readonly stdoutTruncated?: boolean;
  readonly stderrTruncated?: boolean;
}

export interface FakeWorkspaceOptions {
  readonly files?: Record<string, string | Uint8Array>;
  readonly executeAvailable?: boolean;
  readonly execute?: (command: string, prompt?: string) => FakeExecuteOutcome;
  readonly capabilityOverrides?: (manifest: CapabilityManifest) => CapabilityManifest;
  readonly readError?: Error;
  readonly statError?: Error;
  readonly writeError?: Error;
  readonly deleteError?: Error;
}

export interface FakeWorkspace {
  readonly workspace: StratumAgentWorkspace;
  readonly capabilities: CapabilityManifest;
  readonly executeCalls: { command: string; prompt?: string }[];
  read(path: string): string | undefined;
  has(path: string): boolean;
}

const encoder = new TextEncoder();

function normalizeKey(path: string): string {
  return path.replace(/^\/+/, "").replace(/\/+$/, "");
}

function toBytes(value: string | Uint8Array): Uint8Array {
  return typeof value === "string" ? encoder.encode(value) : value;
}

function parentKey(key: string): string {
  const idx = key.lastIndexOf("/");
  return idx < 0 ? "" : key.slice(0, idx);
}

function fileStat(key: string, bytes: Uint8Array): StratumStat {
  return {
    inode_id: 1,
    kind: "file",
    size: bytes.byteLength,
    mode: "100644",
    uid: 1000,
    gid: 1000,
    created: 1_700_000_000,
    modified: 1_700_000_500,
    mime_type: mimeHint(key),
    content_hash: null,
    custom_attrs: {},
  };
}

function dirStat(): StratumStat {
  return {
    inode_id: 2,
    kind: "directory",
    size: 0,
    mode: "40755",
    uid: 1000,
    gid: 1000,
    created: 1_700_000_000,
    modified: 1_700_000_500,
    mime_type: null,
    content_hash: null,
    custom_attrs: {},
  };
}

function mimeHint(key: string): string | null {
  if (key.endsWith(".png")) return "image/png";
  if (key.endsWith(".pdf")) return "application/pdf";
  if (key.endsWith(".bin")) return "application/octet-stream";
  return null;
}

class InMemoryVolumeClient implements StratumVolumeClient {
  private readonly files = new Map<string, Uint8Array>();
  private readonly dirs = new Set<string>([""]);
  private readonly options: FakeWorkspaceOptions;

  constructor(options: FakeWorkspaceOptions) {
    this.options = options;
    const files = options.files ?? {};
    for (const [path, content] of Object.entries(files)) {
      this.put(normalizeKey(path), toBytes(content));
    }
  }

  private put(key: string, bytes: Uint8Array): void {
    this.files.set(key, bytes);
    let dir = parentKey(key);
    for (;;) {
      this.dirs.add(dir);
      if (dir === "") break;
      dir = parentKey(dir);
    }
  }

  read(key: string): string | undefined {
    const bytes = this.files.get(normalizeKey(key));
    return bytes === undefined ? undefined : new TextDecoder().decode(bytes);
  }

  has(key: string): boolean {
    return this.files.has(normalizeKey(key));
  }

  private requireFile(path: string): Uint8Array {
    const bytes = this.files.get(normalizeKey(path));
    if (bytes === undefined) throw new StratumHttpError(404, "not found");
    return bytes;
  }

  readFile(path: string): Promise<string> {
    if (this.options.readError !== undefined) return Promise.reject(this.options.readError);
    return Promise.resolve(new TextDecoder().decode(this.requireFile(path)));
  }

  readFileBuffer(path: string): Promise<Uint8Array> {
    if (this.options.readError !== undefined) return Promise.reject(this.options.readError);
    return Promise.resolve(this.requireFile(path));
  }

  writeFile(path: string, content: unknown): Promise<{ written: string; size: number }> {
    if (this.options.writeError !== undefined) return Promise.reject(this.options.writeError);
    const key = normalizeKey(path);
    const bytes = content instanceof Uint8Array ? content : encoder.encode(String(content));
    this.put(key, bytes);
    return Promise.resolve({ written: `/${key}`, size: bytes.byteLength });
  }

  mkdir(path: string): Promise<{ created: string; type: "directory" }> {
    const key = normalizeKey(path);
    let dir: string = key;
    for (;;) {
      this.dirs.add(dir);
      if (dir === "") break;
      dir = parentKey(dir);
    }
    return Promise.resolve({ created: `/${key}`, type: "directory" });
  }

  listDirectory(path = ""): Promise<StratumDirectoryListing> {
    const key = normalizeKey(path);
    if (!this.dirs.has(key)) {
      return Promise.reject(new StratumHttpError(404, "not found"));
    }
    const names = new Set<string>();
    const entries: StratumDirectoryEntry[] = [];
    const prefix = key === "" ? "" : `${key}/`;
    for (const fileKey of this.files.keys()) {
      if (!fileKey.startsWith(prefix)) continue;
      const rest = fileKey.slice(prefix.length);
      if (rest.includes("/")) continue;
      if (names.has(rest)) continue;
      names.add(rest);
      entries.push(directoryEntry(rest, false));
    }
    for (const dirKey of this.dirs) {
      if (dirKey === "" || dirKey === key) continue;
      if (parentKey(dirKey) !== key) continue;
      const name = dirKey.slice(prefix.length);
      if (names.has(name)) continue;
      names.add(name);
      entries.push(directoryEntry(name, true));
    }
    return Promise.resolve({ path: `/${key}`, entries });
  }

  stat(path: string): Promise<StratumStat> {
    if (this.options.statError !== undefined) return Promise.reject(this.options.statError);
    const key = normalizeKey(path);
    const bytes = this.files.get(key);
    if (bytes !== undefined) return Promise.resolve(fileStat(key, bytes));
    if (this.dirs.has(key)) return Promise.resolve(dirStat());
    return Promise.reject(new StratumHttpError(404, "not found"));
  }

  deletePath(path: string): Promise<{ deleted: string }> {
    if (this.options.deleteError !== undefined) return Promise.reject(this.options.deleteError);
    const key = normalizeKey(path);
    this.files.delete(key);
    this.dirs.delete(key);
    for (const fileKey of [...this.files.keys()]) {
      if (fileKey.startsWith(`${key}/`)) this.files.delete(fileKey);
    }
    return Promise.resolve({ deleted: `/${key}` });
  }

  grep(pattern: string, path = "", recursive = true): Promise<StratumGrepResult> {
    const key = normalizeKey(path);
    const prefix = key === "" ? "" : `${key}/`;
    const regex = new RegExp(pattern);
    const results: { file: string; line_num: number; line: string }[] = [];
    for (const [fileKey, bytes] of this.files) {
      if (!fileKey.startsWith(prefix)) continue;
      if (!recursive && fileKey.slice(prefix.length).includes("/")) continue;
      const lines = new TextDecoder().decode(bytes).split("\n");
      lines.forEach((line, index) => {
        if (regex.test(line)) results.push({ file: `/${fileKey}`, line_num: index + 1, line });
      });
    }
    return Promise.resolve({ results, count: results.length });
  }

  copyPath(): Promise<never> {
    return Promise.reject(new Error("copyPath not supported in fake"));
  }

  movePath(): Promise<never> {
    return Promise.reject(new Error("movePath not supported in fake"));
  }

  find(): Promise<never> {
    return Promise.reject(new Error("find not supported in fake"));
  }

  tree(): Promise<never> {
    return Promise.reject(new Error("tree not supported in fake"));
  }

  status(): Promise<never> {
    return Promise.reject(new Error("status not supported in fake"));
  }

  diff(): Promise<never> {
    return Promise.reject(new Error("diff not supported in fake"));
  }

  commit(): Promise<never> {
    return Promise.reject(new Error("commit not supported in fake"));
  }
}

function directoryEntry(name: string, isDir: boolean): StratumDirectoryListing["entries"][number] {
  return {
    name,
    kind: isDir ? "directory" : "file",
    is_dir: isDir,
    is_symlink: false,
    size: 0,
    mode: isDir ? "40755" : "100644",
    uid: 1000,
    gid: 1000,
    modified: 1_700_000_500,
  };
}

export function fakeCapabilities(executeAvailable: boolean): CapabilityManifest {
  return {
    ...baseManifest,
    routes: {
      ...baseManifest.routes,
      execute: { ...baseManifest.routes.execute, available: executeAvailable, execution: executeAvailable },
    },
  };
}

export function createFakeWorkspace(options: FakeWorkspaceOptions = {}): FakeWorkspace {
  const fs = new InMemoryVolumeClient(options);
  const volume = new StratumVolume(fs);
  const capabilities = options.capabilityOverrides?.(fakeCapabilities(options.executeAvailable === true))
    ?? fakeCapabilities(options.executeAvailable === true);
  const executeCalls: { command: string; prompt?: string }[] = [];

  const runImpl = async (request: { command: string; prompt?: string }): Promise<ExecuteRunResult> => {
    executeCalls.push({ command: request.command, prompt: request.prompt });
    const outcome = options.execute?.(request.command, request.prompt) ?? {};
    const runId = "550e8400-e29b-41d4-a716-446655440002";
    const status = outcome.status ?? "succeeded";
    return {
      workspace_id: "550e8400-e29b-41d4-a716-446655440000",
      job_id: "550e8400-e29b-41d4-a716-446655440001",
      run_id: runId,
      status,
      run_paths: {
        root: `/runs/${runId}`,
        prompt: `/runs/${runId}/prompt.md`,
        command: `/runs/${runId}/command.md`,
        stdout: `/runs/${runId}/stdout.md`,
        stderr: `/runs/${runId}/stderr.md`,
        result: `/runs/${runId}/result.md`,
        metadata: `/runs/${runId}/metadata.md`,
        artifacts: `/runs/${runId}/artifacts`,
      },
      created_at: "2026-06-02T00:00:00Z",
      started_at: "2026-06-02T00:00:01Z",
      ended_at: "2026-06-02T00:00:02Z",
      exit_code: outcome.exitCode ?? 0,
      stdout_truncated: outcome.stdoutTruncated ?? false,
      stderr_truncated: outcome.stderrTruncated ?? false,
      stdout: outcome.stdout ?? "",
      stderr: outcome.stderr ?? "",
    };
  };

  const client = {
    execute: { run: runImpl },
    mount: () => volume,
  } as unknown as StratumClient;

  const workspace = new StratumAgentWorkspace({ client, capabilities, volume });

  return {
    workspace,
    capabilities,
    executeCalls,
    read: (path: string) => fs.read(path),
    has: (path: string) => fs.has(path),
  };
}
