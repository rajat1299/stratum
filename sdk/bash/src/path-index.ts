import type { StratumDirectoryEntry, StratumDirectoryListing, StratumStat } from "./client.js";

export interface IndexedPathEntry extends StratumDirectoryEntry {
  readonly path: string;
}

export function normalizePath(input: string, cwd = "/"): string {
  const base = cwd.startsWith("/") ? cwd : `/${cwd}`;
  const raw = input === "" ? "." : input;
  const combined = raw.startsWith("/") ? raw : `${base}/${raw}`;
  const parts: string[] = [];

  for (const part of combined.split("/")) {
    if (part === "" || part === ".") continue;
    if (part === "..") {
      parts.pop();
      continue;
    }
    parts.push(part);
  }

  return parts.length === 0 ? "/" : `/${parts.join("/")}`;
}

export function toClientPath(input: string, cwd = "/"): string {
  return normalizePath(input, cwd).replace(/^\/+/, "");
}

export function dirname(path: string): string {
  const normalized = normalizePath(path);
  if (normalized === "/") return "/";
  const index = normalized.lastIndexOf("/");
  return index <= 0 ? "/" : normalized.slice(0, index);
}

export class PathIndex {
  private readonly entries = new Map<string, IndexedPathEntry>();

  recordListing(listing: StratumDirectoryListing, cwd = "/"): void {
    const directoryPath = normalizePath(listing.path, cwd);
    this.entries.set(directoryPath, directoryEntry(directoryPath));

    for (const entry of listing.entries) {
      const childPath = normalizePath(entry.name, directoryPath);
      this.entries.set(childPath, { ...entry, path: childPath });
      this.ensureParentDirectories(childPath);
    }
  }

  recordFile(path: string, size: number): void {
    const normalized = normalizePath(path);
    this.entries.set(normalized, {
      name: basename(normalized),
      path: normalized,
      is_dir: false,
      is_symlink: false,
      size,
      mode: "0644",
      uid: 0,
      gid: 0,
      modified: 0,
    });
    this.ensureParentDirectories(normalized);
  }

  recordStat(path: string, stat: StratumStat): void {
    const normalized = normalizePath(path);
    this.entries.set(normalized, {
      name: basename(normalized),
      path: normalized,
      is_dir: stat.kind === "directory",
      is_symlink: stat.kind === "symlink",
      size: stat.size,
      mode: stat.mode,
      uid: stat.uid,
      gid: stat.gid,
      modified: stat.modified,
    });
    this.ensureParentDirectories(normalized);
  }

  recordDirectory(path: string): void {
    const normalized = normalizePath(path);
    this.entries.set(normalized, directoryEntry(normalized));
    this.ensureParentDirectories(normalized);
  }

  entry(path: string): IndexedPathEntry | undefined {
    return this.entries.get(normalizePath(path));
  }

  isFile(path: string): boolean {
    return this.entry(path)?.is_dir === false;
  }

  isDirectory(path: string): boolean {
    const normalized = normalizePath(path);
    if (normalized === "/") return true;
    return this.entries.get(normalized)?.is_dir === true;
  }

  invalidateSubtree(path: string): void {
    const normalized = normalizePath(path);
    for (const entryPath of Array.from(this.entries.keys())) {
      if (isSubtreePath(entryPath, normalized)) {
        this.entries.delete(entryPath);
      }
    }
  }

  clear(): void {
    this.entries.clear();
  }

  paths(): string[] {
    return Array.from(this.entries.keys()).sort();
  }

  private ensureParentDirectories(path: string): void {
    let current = dirname(path);
    while (current !== "/") {
      if (!this.entries.has(current)) {
        this.entries.set(current, directoryEntry(current));
      }
      current = dirname(current);
    }
  }
}

function basename(path: string): string {
  const normalized = normalizePath(path);
  if (normalized === "/") return "/";
  return normalized.slice(normalized.lastIndexOf("/") + 1);
}

function directoryEntry(path: string): IndexedPathEntry {
  return {
    name: basename(path),
    path,
    is_dir: true,
    is_symlink: false,
    size: 0,
    mode: "0755",
    uid: 0,
    gid: 0,
    modified: 0,
  };
}

function isSubtreePath(path: string, root: string): boolean {
  if (root === "/") return true;
  return path === root || path.startsWith(`${root}/`);
}
