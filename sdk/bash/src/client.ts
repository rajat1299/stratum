export interface StratumClientOptions {
  readonly baseUrl: string;
  readonly workspaceId: string;
  readonly workspaceToken: string;
  readonly fetch?: typeof fetch;
}

export interface StratumDirectoryEntry {
  readonly name: string;
  readonly is_dir: boolean;
  readonly is_symlink: boolean;
  readonly size: number;
  readonly mode: string;
  readonly uid: number;
  readonly gid: number;
  readonly modified: number;
}

export interface StratumDirectoryListing {
  readonly path: string;
  readonly entries: readonly StratumDirectoryEntry[];
}

export interface StratumStat {
  readonly inode_id: number;
  readonly kind: string;
  readonly size: number;
  readonly mode: string;
  readonly uid: number;
  readonly gid: number;
  readonly created: number;
  readonly modified: number;
  readonly mime_type: string | null;
  readonly content_hash: string | null;
  readonly custom_attrs: Record<string, string>;
}

export interface StratumWriteResult {
  readonly written: string;
  readonly size: number;
}

export interface StratumMkdirResult {
  readonly created: string;
  readonly type: "directory";
}

export interface StratumDeleteResult {
  readonly deleted: string;
}

export interface StratumCopyResult {
  readonly copied: string;
  readonly to: string;
}

export interface StratumMoveResult {
  readonly moved: string;
  readonly to: string;
}

export interface StratumGrepResult {
  readonly results: readonly StratumGrepMatch[];
  readonly count: number;
}

export interface StratumGrepMatch {
  readonly file: string;
  readonly line_num: number;
  readonly line: string;
}

export interface StratumFindResult {
  readonly results: readonly string[];
  readonly count: number;
}

export interface StratumCommitResult {
  readonly hash: string;
  readonly message: string;
  readonly author: string;
}

export interface StratumMutationOptions {
  readonly idempotencyKey?: string;
}

export interface StratumWriteOptions extends StratumMutationOptions {
  readonly mimeType?: string;
}

type ResponseKind = "json" | "text";

interface RequestOptions {
  readonly method: string;
  readonly query?: readonly (readonly [string, string])[];
  readonly headers?: HeadersInit;
  readonly body?: BodyInit;
  readonly responseKind: ResponseKind;
  readonly idempotencyKey?: string;
}

export class StratumHttpError extends Error {
  readonly status: number;
  readonly body: string;

  constructor(status: number, body: string, message: string) {
    super(message);
    this.name = "StratumHttpError";
    this.status = status;
    this.body = body;
  }
}

export class StratumClient {
  private readonly baseUrl: string;
  private readonly workspaceId: string;
  private readonly workspaceToken: string;
  private readonly fetchImpl: typeof fetch;

  constructor(options: StratumClientOptions) {
    this.baseUrl = ensureTrailingSlash(options.baseUrl);
    this.workspaceId = options.workspaceId;
    this.workspaceToken = options.workspaceToken;
    this.fetchImpl = options.fetch ?? globalThis.fetch;

    if (!this.fetchImpl) {
      throw new Error("StratumClient requires fetch");
    }
  }

  async readFile(path: string): Promise<string> {
    return this.requestText(this.fsRoute(path), { method: "GET" });
  }

  async writeFile(path: string, content: BodyInit, options: StratumWriteOptions = {}): Promise<StratumWriteResult> {
    return this.requestJson(this.fsRoute(path), {
      method: "PUT",
      body: content,
      headers: options.mimeType ? { "X-Stratum-Mime-Type": options.mimeType } : undefined,
      idempotencyKey: options.idempotencyKey,
    });
  }

  async mkdir(path: string, options: StratumMutationOptions = {}): Promise<StratumMkdirResult> {
    return this.requestJson(this.fsRoute(path), {
      method: "PUT",
      headers: { "X-Stratum-Type": "directory" },
      idempotencyKey: options.idempotencyKey,
    });
  }

  async listDirectory(path = ""): Promise<StratumDirectoryListing> {
    return this.requestJson(this.fsRoute(path), { method: "GET" });
  }

  async stat(path: string): Promise<StratumStat> {
    const routePath = normalizeRoutePath(path);
    if (routePath === "") {
      throw new Error("StratumClient.stat does not support the workspace root; use listDirectory instead");
    }

    return this.requestJson(this.fsRoute(routePath), {
      method: "GET",
      query: [["stat", "true"]],
    });
  }

  async deletePath(path: string, recursive = false, options: StratumMutationOptions = {}): Promise<StratumDeleteResult> {
    return this.requestJson(this.fsRoute(path), {
      method: "DELETE",
      query: [["recursive", String(recursive)]],
      idempotencyKey: options.idempotencyKey,
    });
  }

  async copyPath(
    source: string,
    destination: string,
    options: StratumMutationOptions = {},
  ): Promise<StratumCopyResult> {
    return this.requestJson(this.fsRoute(source), {
      method: "POST",
      query: [
        ["op", "copy"],
        ["dst", destination],
      ],
      idempotencyKey: options.idempotencyKey,
    });
  }

  async movePath(
    source: string,
    destination: string,
    options: StratumMutationOptions = {},
  ): Promise<StratumMoveResult> {
    return this.requestJson(this.fsRoute(source), {
      method: "POST",
      query: [
        ["op", "move"],
        ["dst", destination],
      ],
      idempotencyKey: options.idempotencyKey,
    });
  }

  async grep(pattern: string, path = "", recursive = true): Promise<StratumGrepResult> {
    return this.requestJson("search/grep", {
      method: "GET",
      query: [
        ["pattern", pattern],
        ["path", path],
        ["recursive", String(recursive)],
      ],
    });
  }

  async find(name: string, path = ""): Promise<StratumFindResult> {
    return this.requestJson("search/find", {
      method: "GET",
      query: [
        ["path", path],
        ["name", name],
      ],
    });
  }

  async tree(path = ""): Promise<string> {
    return this.requestText(this.treeRoute(path), { method: "GET" });
  }

  async status(): Promise<string> {
    return this.requestText("vcs/status", { method: "GET" });
  }

  async diff(path?: string): Promise<string> {
    const query = path === undefined ? undefined : [["path", path] as const];
    return this.requestText("vcs/diff", { method: "GET", query });
  }

  async commit(message: string, options: StratumMutationOptions = {}): Promise<StratumCommitResult> {
    return this.requestJson("vcs/commit", {
      method: "POST",
      body: JSON.stringify({ message }),
      headers: { "Content-Type": "application/json" },
      idempotencyKey: options.idempotencyKey,
    });
  }

  private fsRoute(path: string): string {
    return pathRoute("fs", path);
  }

  private treeRoute(path: string): string {
    return pathRoute("tree", path);
  }

  private async requestJson<T>(route: string, options: Omit<RequestOptions, "responseKind">): Promise<T> {
    return this.request<T>(route, { ...options, responseKind: "json" });
  }

  private async requestText(route: string, options: Omit<RequestOptions, "responseKind">): Promise<string> {
    return this.request<string>(route, { ...options, responseKind: "text" });
  }

  private async request<T>(route: string, options: RequestOptions): Promise<T> {
    const url = this.buildUrl(route, options.query);
    const headers = new Headers(options.headers);
    headers.set("Authorization", `Bearer ${this.workspaceToken}`);
    headers.set("X-Stratum-Workspace", this.workspaceId);

    if (isMutatingMethod(options.method)) {
      headers.set("Idempotency-Key", options.idempotencyKey ?? generateIdempotencyKey());
    }

    const response = await this.fetchImpl(url, {
      method: options.method,
      headers,
      body: options.body,
    });

    if (!response.ok) {
      const body = await response.text();
      throw new StratumHttpError(response.status, body, errorMessageFromBody(response.status, body));
    }

    if (options.responseKind === "text") {
      return response.text() as Promise<T>;
    }

    const body = await response.text();
    return (body === "" ? undefined : JSON.parse(body)) as T;
  }

  private buildUrl(route: string, query?: readonly (readonly [string, string])[]): string {
    const url = new URL(stripLeadingSlash(route), this.baseUrl);
    for (const [key, value] of query ?? []) {
      url.searchParams.set(key, value);
    }
    return url.toString();
  }
}

function ensureTrailingSlash(baseUrl: string): string {
  return baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
}

function pathRoute(prefix: string, path: string): string {
  const routePath = normalizeRoutePath(path);
  if (routePath === "") {
    return prefix;
  }

  return `${prefix}/${routePath.split("/").map(encodeURIComponent).join("/")}`;
}

function normalizeRoutePath(path: string): string {
  const parts: string[] = [];
  for (const part of stripLeadingSlash(path).split("/")) {
    if (part === "" || part === ".") {
      continue;
    }
    if (part === "..") {
      parts.pop();
      continue;
    }
    parts.push(part);
  }
  return parts.join("/");
}

function stripLeadingSlash(value: string): string {
  return value.replace(/^\/+/, "");
}

function isMutatingMethod(method: string): boolean {
  return method === "PUT" || method === "POST" || method === "DELETE";
}

function generateIdempotencyKey(): string {
  const crypto = globalThis.crypto;
  if (crypto?.randomUUID) {
    return `stratum-bash-${crypto.randomUUID()}`;
  }

  return `stratum-bash-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

function errorMessageFromBody(status: number, body: string): string {
  try {
    const parsed = JSON.parse(body) as { error?: unknown };
    if (typeof parsed.error === "string" && parsed.error !== "") {
      return parsed.error;
    }
  } catch {
    // Preserve the raw body on StratumHttpError when the server returns non-JSON text.
  }

  return body === "" ? `Stratum request failed with status ${status}` : body;
}
