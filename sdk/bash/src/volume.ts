import type {
  StratumCommitResult,
  StratumCopyResult,
  StratumDeleteResult,
  StratumDirectoryListing,
  StratumFindResult,
  StratumGrepResult,
  StratumMkdirResult,
  StratumMoveResult,
  StratumMutationOptions,
  StratumRequestBody,
  StratumStat,
  StratumWriteOptions,
  StratumWriteResult,
} from "@stratum/sdk";
import { dirname, normalizePath, PathIndex, toClientPath } from "./path-index.js";
import { SessionCache, type SessionCacheOptions } from "./session-cache.js";

export interface StratumVolumeClient {
  readFile(path: string): Promise<string>;
  readFileBuffer(path: string): Promise<Uint8Array>;
  writeFile(path: string, content: StratumRequestBody, options?: StratumWriteOptions): Promise<StratumWriteResult>;
  mkdir(path: string, options?: StratumMutationOptions): Promise<StratumMkdirResult>;
  listDirectory(path?: string): Promise<StratumDirectoryListing>;
  stat(path: string): Promise<StratumStat>;
  deletePath(
    path: string,
    recursive?: boolean,
    options?: StratumMutationOptions,
  ): Promise<StratumDeleteResult>;
  copyPath(
    source: string,
    destination: string,
    options?: StratumMutationOptions,
  ): Promise<StratumCopyResult>;
  movePath(
    source: string,
    destination: string,
    options?: StratumMutationOptions,
  ): Promise<StratumMoveResult>;
  grep(pattern: string, path?: string, recursive?: boolean): Promise<StratumGrepResult>;
  find(name: string, path?: string): Promise<StratumFindResult>;
  tree(path?: string): Promise<string>;
  status(): Promise<string>;
  diff(path?: string): Promise<string>;
  commit(message: string, options?: StratumMutationOptions): Promise<StratumCommitResult>;
}

export interface StratumVolumeOptions {
  readonly cwd?: string;
  readonly pathIndex?: PathIndex;
  readonly cache?: SessionCache;
  readonly cacheOptions?: SessionCacheOptions;
}

export class StratumVolume {
  readonly client: StratumVolumeClient;
  readonly pathIndex: PathIndex;
  readonly cache: SessionCache;
  private cwd: string;

  constructor(client: StratumVolumeClient, options: StratumVolumeOptions = {}) {
    this.client = client;
    this.cwd = normalizePath(options.cwd ?? "/");
    this.pathIndex = options.pathIndex ?? new PathIndex();
    this.cache = options.cache ?? new SessionCache(options.cacheOptions);
  }

  pwd(): string {
    return this.cwd;
  }

  async cd(path: string): Promise<string> {
    const target = this.absolute(path);
    const stat = await this.stat(target);
    if (stat.kind !== "directory") {
      throw new Error(`Not a directory: ${target}`);
    }
    this.pathIndex.recordDirectory(target);
    this.cwd = target;
    return this.cwd;
  }

  async ls(path = "."): Promise<StratumDirectoryListing> {
    const target = this.absolute(path);
    const cached = this.cache.getList(target);
    if (cached) return cached;

    const listing = await this.client.listDirectory(toClientPath(target));
    this.cache.setList(target, listing);
    this.pathIndex.recordListing({ ...listing, path: target });
    return listing;
  }

  async cat(path: string): Promise<string> {
    const target = this.absolute(path);
    const cached = this.cache.getRead(target);
    if (cached !== null) return readToString(cached);

    const content = await this.client.readFile(toClientPath(target));
    this.cache.setRead(target, content);
    this.pathIndex.recordFile(target, byteLength(content));
    return content;
  }

  async readFileBuffer(path: string): Promise<Uint8Array> {
    const target = this.absolute(path);
    const cached = this.cache.getRead(target);
    if (cached !== null) return readToBytes(cached);

    const content = await this.client.readFileBuffer(toClientPath(target));
    this.cache.setRead(target, content);
    this.pathIndex.recordFile(target, content.byteLength);
    return new Uint8Array(content);
  }

  async writeFile(
    path: string,
    content: string | Uint8Array,
    options?: StratumWriteOptions,
  ): Promise<StratumWriteResult> {
    const target = this.absolute(path);
    const result = await this.client.writeFile(toClientPath(target), content, options);
    this.invalidateMutation(target);
    this.cache.setRead(target, cloneRead(content));
    this.pathIndex.recordFile(target, result.size);
    return result;
  }

  async mkdir(path: string, options?: StratumMutationOptions): Promise<StratumMkdirResult> {
    const target = this.absolute(path);
    const result = await this.client.mkdir(toClientPath(target), options);
    this.invalidateMutation(target);
    this.pathIndex.recordDirectory(target);
    return result;
  }

  async rm(
    path: string,
    recursive = false,
    options?: StratumMutationOptions,
  ): Promise<StratumDeleteResult> {
    const target = this.absolute(path);
    const result = await this.client.deletePath(toClientPath(target), recursive, options);
    this.invalidateMutation(target);
    this.pathIndex.invalidateSubtree(target);
    return result;
  }

  async cp(
    source: string,
    destination: string,
    options?: StratumMutationOptions,
  ): Promise<StratumCopyResult> {
    const src = this.absolute(source);
    const dest = this.absolute(destination);
    const result = await this.client.copyPath(toClientPath(src), toClientPath(dest), options);
    this.invalidateMutation(dest);
    return result;
  }

  async mv(
    source: string,
    destination: string,
    options?: StratumMutationOptions,
  ): Promise<StratumMoveResult> {
    const src = this.absolute(source);
    const dest = this.absolute(destination);
    const result = await this.client.movePath(toClientPath(src), toClientPath(dest), options);
    this.invalidateMutation(src);
    this.invalidateMutation(dest);
    this.pathIndex.invalidateSubtree(src);
    return result;
  }

  async grep(pattern: string, path = ".", recursive = true): Promise<StratumGrepResult> {
    return this.client.grep(pattern, toClientPath(this.absolute(path)), recursive);
  }

  async find(name: string, path = "."): Promise<StratumFindResult> {
    return this.client.find(name, toClientPath(this.absolute(path)));
  }

  async tree(path = "."): Promise<string> {
    return this.client.tree(toClientPath(this.absolute(path)));
  }

  async status(): Promise<string> {
    return this.client.status();
  }

  async diff(path?: string): Promise<string> {
    return this.client.diff(path === undefined ? undefined : toClientPath(this.absolute(path)));
  }

  async commit(message: string, options?: StratumMutationOptions): Promise<StratumCommitResult> {
    return this.client.commit(message, options);
  }

  async stat(path: string): Promise<StratumStat> {
    const target = this.absolute(path);
    if (target === "/") {
      this.pathIndex.recordDirectory("/");
      return rootStat();
    }

    const cached = this.cache.getStat(target);
    if (cached) return cached;

    const stat = await this.client.stat(toClientPath(target));
    this.cache.setStat(target, stat);
    this.pathIndex.recordStat(target, stat);
    return stat;
  }

  private absolute(path: string): string {
    return normalizePath(path, this.cwd);
  }

  private invalidateMutation(path: string): void {
    this.cache.invalidatePath(path);
    this.invalidateAncestorLists(path);
    this.pathIndex.invalidateSubtree(path);
  }

  private invalidateAncestorLists(path: string): void {
    let current = dirname(path);
    while (true) {
      this.cache.invalidateExact(current, ["list", "stat"]);
      if (current === "/") break;
      current = dirname(current);
    }
  }
}

function rootStat(): StratumStat {
  return {
    inode_id: 0,
    kind: "directory",
    size: 0,
    mode: "0755",
    uid: 0,
    gid: 0,
    created: 0,
    modified: 0,
    mime_type: null,
    content_hash: null,
    custom_attrs: {},
  };
}

function byteLength(content: string): number {
  return new TextEncoder().encode(content).length;
}

function readToString(content: string | Uint8Array): string {
  return typeof content === "string" ? content : new TextDecoder().decode(content);
}

function readToBytes(content: string | Uint8Array): Uint8Array {
  if (typeof content === "string") return new TextEncoder().encode(content);
  return new Uint8Array(content);
}

function cloneRead(content: string | Uint8Array): string | Uint8Array {
  return typeof content === "string" ? content : new Uint8Array(content);
}
