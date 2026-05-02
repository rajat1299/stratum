import type {
  BufferEncoding,
  CpOptions,
  FileContent,
  FsStat,
  IFileSystem,
  MkdirOptions,
  RmOptions,
} from "just-bash";
import { normalizePath } from "./path-index.js";
import type { StratumVolume } from "./volume.js";
import { eexist, eisdir, enoent, enosys, enotdir, toFsError } from "./errors.js";

interface DirentEntry {
  name: string;
  isFile: boolean;
  isDirectory: boolean;
  isSymbolicLink: boolean;
}

interface ReadFileOptions {
  encoding?: BufferEncoding | null;
}

interface WriteFileOptions {
  encoding?: BufferEncoding;
}

export class StratumFs implements IFileSystem {
  constructor(public readonly volume: StratumVolume) {}

  resolvePath(base: string, path: string): string {
    return normalizePath(path, base);
  }

  async readFile(path: string, _options?: ReadFileOptions | BufferEncoding): Promise<string> {
    const normalized = normalizePath(path);
    const stat = await this.statRequired(normalized, "open");
    if (stat.kind === "directory") throw eisdir(normalized);

    try {
      return await this.volume.cat(normalized);
    } catch (error) {
      throw toFsError(error, normalized, "open");
    }
  }

  async readFileBuffer(path: string): Promise<Uint8Array> {
    return new TextEncoder().encode(await this.readFile(path));
  }

  async writeFile(
    path: string,
    content: FileContent,
    _options?: WriteFileOptions | BufferEncoding,
  ): Promise<void> {
    const normalized = normalizePath(path);
    const stat = await this.statIfExists(normalized, "write");
    if (stat?.kind === "directory") throw eisdir(normalized);

    try {
      await this.volume.writeFile(normalized, contentToString(content));
    } catch (error) {
      throw toFsError(error, normalized, "write");
    }
  }

  async appendFile(
    path: string,
    content: FileContent,
    _options?: WriteFileOptions | BufferEncoding,
  ): Promise<void> {
    const normalized = normalizePath(path);
    const stat = await this.statIfExists(normalized, "append");
    if (stat?.kind === "directory") throw eisdir(normalized);

    let head = "";
    try {
      head = await this.volume.cat(normalized);
    } catch (error) {
      const fsError = toFsError(error, normalized, "open");
      if (fsError.code !== "ENOENT") throw fsError;
    }

    try {
      await this.volume.writeFile(normalized, head + contentToString(content));
    } catch (error) {
      throw toFsError(error, normalized, "append");
    }
  }

  async exists(path: string): Promise<boolean> {
    try {
      await this.stat(path);
      return true;
    } catch {
      return false;
    }
  }

  async stat(path: string): Promise<FsStat> {
    const normalized = normalizePath(path);
    try {
      return statToFsStat(await this.volume.stat(normalized));
    } catch (error) {
      throw toFsError(error, normalized, "stat");
    }
  }

  async lstat(path: string): Promise<FsStat> {
    return this.stat(path);
  }

  async realpath(path: string): Promise<string> {
    const normalized = normalizePath(path);
    await this.stat(normalized);
    return normalized;
  }

  async mkdir(path: string, options?: MkdirOptions): Promise<void> {
    const normalized = normalizePath(path);
    if (normalized === "/") {
      if (options?.recursive) return;
      throw eexist(normalized);
    }

    if (!options?.recursive) {
      if (await this.statIfExists(normalized, "mkdir")) throw eexist(normalized);
      await this.mkdirRemote(normalized);
      return;
    }

    for (const segment of pathSegments(normalized)) {
      const stat = await this.statIfExists(segment, "mkdir");
      if (stat) {
        if (stat.kind !== "directory") throw enotdir(segment);
        continue;
      }
      await this.mkdirRemote(segment);
    }
  }

  async readdir(path: string): Promise<string[]> {
    return (await this.readdirWithFileTypes(path)).map((entry) => entry.name);
  }

  async readdirWithFileTypes(path: string): Promise<DirentEntry[]> {
    const normalized = normalizePath(path);
    const stat = await this.statRequired(normalized, "scandir");
    if (stat.kind !== "directory") throw enotdir(normalized);

    try {
      const listing = await this.volume.ls(normalized);
      return listing.entries
        .map((entry) => ({
          name: entry.name,
          isFile: !entry.is_dir && !entry.is_symlink,
          isDirectory: entry.is_dir,
          isSymbolicLink: entry.is_symlink,
        }))
        .sort((a, b) => a.name.localeCompare(b.name));
    } catch (error) {
      throw toFsError(error, normalized, "scandir");
    }
  }

  async rm(path: string, options?: RmOptions): Promise<void> {
    const normalized = normalizePath(path);
    try {
      await this.volume.rm(normalized, options?.recursive ?? false);
    } catch (error) {
      const fsError = toFsError(error, normalized, "rm");
      if (options?.force && fsError.code === "ENOENT") return;
      throw fsError;
    }
  }

  async cp(src: string, dest: string, options?: CpOptions): Promise<void> {
    const source = normalizePath(src);
    const destination = normalizePath(dest);
    const stat = await this.statRequired(source, "cp");
    if (stat.kind === "directory" && !options?.recursive) {
      throw eisdir(source);
    }

    try {
      await this.volume.cp(source, destination);
    } catch (error) {
      throw toFsError(error, source, "cp");
    }
  }

  async mv(src: string, dest: string): Promise<void> {
    const source = normalizePath(src);
    const destination = normalizePath(dest);

    try {
      await this.volume.mv(source, destination);
    } catch (error) {
      throw toFsError(error, source, "mv");
    }
  }

  getAllPaths(): string[] {
    return Array.from(new Set(["/", ...this.volume.pathIndex.paths()])).sort();
  }

  async chmod(_path: string, _mode: number): Promise<void> {
    throw enosys("chmod");
  }

  async symlink(_target: string, _linkPath: string): Promise<void> {
    throw enosys("symlink");
  }

  async link(_existingPath: string, _newPath: string): Promise<void> {
    throw enosys("link");
  }

  async readlink(_path: string): Promise<string> {
    throw enosys("readlink");
  }

  async utimes(_path: string, _atime: Date, _mtime: Date): Promise<void> {
    throw enosys("utimes");
  }

  private async mkdirRemote(path: string): Promise<void> {
    try {
      await this.volume.mkdir(path);
    } catch (error) {
      throw toFsError(error, path, "mkdir");
    }
  }

  private async statIfExists(
    path: string,
    operation: string,
  ): Promise<Awaited<ReturnType<StratumVolume["stat"]>> | null> {
    try {
      return await this.volume.stat(path);
    } catch (error) {
      const fsError = toFsError(error, path, operation);
      if (fsError.code === "ENOENT") return null;
      throw fsError;
    }
  }

  private async statRequired(path: string, operation: string): Promise<Awaited<ReturnType<StratumVolume["stat"]>>> {
    const stat = await this.statIfExists(path, operation);
    if (!stat) throw enoent(path);
    return stat;
  }
}

function contentToString(content: FileContent): string {
  return typeof content === "string" ? content : new TextDecoder().decode(content);
}

function statToFsStat(stat: Awaited<ReturnType<StratumVolume["stat"]>>): FsStat {
  const isDirectory = stat.kind === "directory";
  const isSymbolicLink = stat.kind === "symlink";
  const mode = parseInt(stat.mode, 8);
  return {
    isFile: stat.kind === "file",
    isDirectory,
    isSymbolicLink,
    mode: Number.isFinite(mode) ? mode : isDirectory ? 0o755 : 0o644,
    size: stat.size,
    mtime: stat.modified > 0 ? new Date(stat.modified * 1000) : new Date(0),
  };
}

function pathSegments(path: string): string[] {
  const segments = normalizePath(path).split("/").filter(Boolean);
  const paths: string[] = [];
  let current = "";
  for (const segment of segments) {
    current += `/${segment}`;
    paths.push(current);
  }
  return paths;
}
