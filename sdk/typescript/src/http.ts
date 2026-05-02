import { StratumHttpError } from "./errors.js";
import type { StratumAuth, StratumRequestBody } from "./types.js";

export type ResponseKind = "bytes" | "json" | "text";

export interface RequestOptions {
  readonly method: string;
  readonly query?: readonly (readonly [string, string])[];
  readonly headers?: HeadersInit;
  readonly body?: StratumRequestBody | unknown;
  readonly responseKind: ResponseKind;
  readonly idempotencyKey?: string;
  readonly autoIdempotency?: boolean;
}

export interface HttpClientOptions {
  readonly baseUrl: string;
  readonly fetchImpl: typeof fetch;
  readonly auth?: StratumAuth;
  readonly idempotencyKeyPrefix: string;
}

export class StratumHttpClient {
  private readonly baseUrl: string;
  private readonly fetchImpl: typeof fetch;
  private readonly auth?: StratumAuth;
  private readonly idempotencyKeyPrefix: string;

  constructor(options: HttpClientOptions) {
    this.baseUrl = ensureTrailingSlash(options.baseUrl);
    this.fetchImpl = options.fetchImpl;
    this.auth = options.auth;
    this.idempotencyKeyPrefix = options.idempotencyKeyPrefix;
  }

  async json<T>(route: string, options: Omit<RequestOptions, "responseKind">): Promise<T> {
    return this.request<T>(route, { ...options, responseKind: "json" });
  }

  async text(route: string, options: Omit<RequestOptions, "responseKind">): Promise<string> {
    return this.request<string>(route, { ...options, responseKind: "text" });
  }

  async bytes(route: string, options: Omit<RequestOptions, "responseKind">): Promise<Uint8Array> {
    return this.request<Uint8Array>(route, { ...options, responseKind: "bytes" });
  }

  async request<T>(route: string, options: RequestOptions): Promise<T> {
    const url = this.buildUrl(route, options.query);
    const headers = new Headers(options.headers);
    applyHeaders(headers, buildAuthHeaders(this.auth));

    if (options.idempotencyKey !== undefined) {
      headers.set("Idempotency-Key", options.idempotencyKey);
    } else if (options.autoIdempotency === true) {
      headers.set("Idempotency-Key", generateIdempotencyKey(this.idempotencyKeyPrefix));
    }

    const body = toRequestBody(options.body, headers);
    const response = await this.fetchImpl(url, {
      method: options.method,
      headers,
      body,
    });

    if (!response.ok) {
      const errorBody = await response.text();
      throw new StratumHttpError(response.status, errorBody);
    }

    if (options.responseKind === "text") {
      return response.text() as Promise<T>;
    }

    if (options.responseKind === "bytes") {
      return response.arrayBuffer().then((buffer) => new Uint8Array(buffer)) as Promise<T>;
    }

    const responseBody = await response.text();
    return (responseBody === "" ? undefined : JSON.parse(responseBody)) as T;
  }

  private buildUrl(route: string, query?: readonly (readonly [string, string])[]): string {
    const url = new URL(stripLeadingSlash(route), this.baseUrl);
    for (const [key, value] of query ?? []) {
      url.searchParams.set(key, value);
    }
    return url.toString();
  }
}

export function buildAuthHeaders(auth: StratumAuth | undefined): Headers {
  const headers = new Headers();
  if (auth?.type === "user") {
    headers.set("Authorization", `User ${auth.username}`);
  } else if (auth?.type === "bearer") {
    headers.set("Authorization", `Bearer ${auth.token}`);
  } else if (auth?.type === "workspace") {
    headers.set("Authorization", `Bearer ${auth.workspaceToken}`);
    headers.set("X-Stratum-Workspace", auth.workspaceId);
  }

  return headers;
}

export function generateIdempotencyKey(prefix = "stratum-sdk"): string {
  const safePrefix = prefix.replace(/[^\x21-\x7e]/g, "-").slice(0, 160) || "stratum-sdk";
  const crypto = globalThis.crypto;
  const suffix = crypto?.randomUUID
    ? crypto.randomUUID()
    : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  return `${safePrefix}-${suffix}`.slice(0, 255);
}

function ensureTrailingSlash(baseUrl: string): string {
  return baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
}

function stripLeadingSlash(value: string): string {
  return value.replace(/^\/+/, "");
}

function applyHeaders(target: Headers, source: Headers): void {
  source.forEach((value, key) => target.set(key, value));
}

function toRequestBody(body: StratumRequestBody | unknown | undefined, headers: Headers): BodyInit | undefined {
  if (body === undefined) {
    return undefined;
  }

  if (body instanceof Uint8Array) {
    const bytes = new Uint8Array(body.byteLength);
    bytes.set(body);
    return bytes.buffer as ArrayBuffer;
  }

  if (typeof body === "string" || body instanceof Blob || body instanceof ArrayBuffer || body instanceof FormData) {
    return body;
  }

  if (body instanceof URLSearchParams || body instanceof ReadableStream) {
    return body;
  }

  if (!headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  return JSON.stringify(body);
}
