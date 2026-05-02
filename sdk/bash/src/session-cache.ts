import type { StratumDirectoryListing, StratumStat } from "./client.js";
import { normalizePath } from "./path-index.js";

export interface SessionCacheOptions {
  readonly ttlMs?: number | null;
  readonly maxBytes?: number;
  readonly now?: () => number;
}

export type SessionCacheKind = "read" | "stat" | "list";

interface CacheEntry<T> {
  readonly kind: SessionCacheKind;
  readonly path: string;
  readonly value: T;
  readonly expiresAt: number;
  readonly bytes: number;
}

const DEFAULT_TTL_MS = 150_000;
const DEFAULT_MAX_BYTES = 50 * 1024 * 1024;

export class SessionCache {
  private readonly ttlMs: number | null;
  private readonly maxBytes: number;
  private readonly now: () => number;
  private readonly entries = new Map<string, CacheEntry<unknown>>();
  private currentBytes = 0;

  constructor(options: SessionCacheOptions = {}) {
    this.ttlMs = options.ttlMs === undefined ? DEFAULT_TTL_MS : options.ttlMs;
    this.maxBytes = options.maxBytes ?? DEFAULT_MAX_BYTES;
    this.now = options.now ?? Date.now;
  }

  getRead(path: string): string | null {
    return this.get<string>("read", path);
  }

  setRead(path: string, content: string): void {
    this.set("read", path, content, byteLength(content));
  }

  getStat(path: string): StratumStat | null {
    return this.get<StratumStat>("stat", path);
  }

  setStat(path: string, stat: StratumStat): void {
    this.set("stat", path, stat, byteLength(JSON.stringify(stat)));
  }

  getList(path: string): StratumDirectoryListing | null {
    return this.get<StratumDirectoryListing>("list", path);
  }

  setList(path: string, listing: StratumDirectoryListing): void {
    this.set("list", path, listing, byteLength(JSON.stringify(listing)));
  }

  invalidatePath(path: string): void {
    const normalized = normalizePath(path);
    for (const [key, entry] of Array.from(this.entries.entries())) {
      if (isSubtreePath(entry.path, normalized)) {
        this.deleteKey(key);
      }
    }
  }

  invalidateExact(path: string, kinds: readonly SessionCacheKind[] = ["read", "stat", "list"]): void {
    const normalized = normalizePath(path);
    for (const kind of kinds) {
      this.deleteKey(cacheKey(kind, normalized));
    }
  }

  clear(): void {
    this.entries.clear();
    this.currentBytes = 0;
  }

  size(): number {
    this.pruneExpired();
    return this.entries.size;
  }

  totalBytes(): number {
    this.pruneExpired();
    return this.currentBytes;
  }

  private get<T>(kind: SessionCacheKind, path: string): T | null {
    const key = cacheKey(kind, path);
    const entry = this.entries.get(key);
    if (!entry) return null;
    if (this.isExpired(entry)) {
      this.deleteKey(key);
      return null;
    }

    this.entries.delete(key);
    this.entries.set(key, entry);
    return entry.value as T;
  }

  private set<T>(kind: SessionCacheKind, path: string, value: T, bytes: number): void {
    const normalized = normalizePath(path);
    const key = cacheKey(kind, normalized);
    this.deleteKey(key);

    const entry: CacheEntry<T> = {
      kind,
      path: normalized,
      value,
      expiresAt: this.expiresAt(),
      bytes,
    };
    this.entries.set(key, entry);
    this.currentBytes += bytes;
    this.enforceMaxBytes();
  }

  private expiresAt(): number {
    if (this.ttlMs === null) return Number.POSITIVE_INFINITY;
    return this.now() + this.ttlMs;
  }

  private isExpired(entry: CacheEntry<unknown>): boolean {
    return this.now() >= entry.expiresAt;
  }

  private pruneExpired(): void {
    for (const [key, entry] of Array.from(this.entries.entries())) {
      if (this.isExpired(entry)) {
        this.deleteKey(key);
      }
    }
  }

  private enforceMaxBytes(): void {
    while (this.currentBytes > this.maxBytes && this.entries.size > 0) {
      const oldestKey = this.entries.keys().next().value as string | undefined;
      if (oldestKey === undefined) break;
      this.deleteKey(oldestKey);
    }
  }

  private deleteKey(key: string): void {
    const entry = this.entries.get(key);
    if (!entry) return;
    this.entries.delete(key);
    this.currentBytes -= entry.bytes;
  }
}

function cacheKey(kind: SessionCacheKind, path: string): string {
  return `${kind}:${normalizePath(path)}`;
}

function byteLength(value: string): number {
  return new TextEncoder().encode(value).length;
}

function isSubtreePath(path: string, root: string): boolean {
  if (root === "/") return true;
  return path === root || path.startsWith(`${root}/`);
}
