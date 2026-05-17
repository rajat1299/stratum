/**
 * useStratumClient — the single read path for the SDK in components.
 *
 *   - Memoized per-auth: a new StratumClient instance is constructed only
 *     when auth credentials actually change. TanStack Query keys can rely
 *     on referential stability of the client itself when needed.
 *
 *   - baseUrl is empty: same-origin in prod, Vite dev proxy in dev (see
 *     web/vite.config.ts).
 *
 *   - During the initial "loading" state we still return a usable client
 *     (just unauth'd). This lets components that don't need auth — like
 *     /v1/capabilities or /health — fire off requests immediately and not
 *     wait for storage hydration.
 *
 *   - The hook does not touch idempotency keys, retry policy, or query
 *     caching. Those live in the TanStack Query layer (Phase A4).
 */

import { useMemo } from "react";
import { StratumClient, type StratumClientOptions } from "@stratum/sdk";
import { useAuth } from "./auth.tsx";

/** Optional override for tests + storybook spikes. */
export interface UseStratumClientOptions {
  /** Base URL override; defaults to empty string (same-origin / Vite proxy). */
  readonly baseUrl?: string;
  /** Inject a custom fetch — for MSW, undici, or a test stub. */
  readonly fetch?: typeof fetch;
}

export function useStratumClient(options: UseStratumClientOptions = {}): StratumClient {
  const { state } = useAuth();
  const { baseUrl: explicitBaseUrl, fetch: fetchImpl } = options;

  // Default to same-origin when running in a browser context. The SDK's URL
  // builder needs an absolute base — an empty string would throw on the
  // `new URL("change-requests", "/")` line. In prod that origin is the
  // deployed host; in dev Vite's proxy hands the request to the local
  // stratum-server on :3000; in happy-dom tests it's whatever happy-dom
  // exposes as window.location.origin.
  const baseUrl =
    explicitBaseUrl ?? (typeof window !== "undefined" ? window.location.origin : "");

  // Pull only the bytes that actually drive client identity. This keeps the
  // memo key stable across no-op re-renders.
  const credentials = state.status === "authed" ? state.credentials : undefined;

  return useMemo(() => {
    const init: StratumClientOptions = {
      baseUrl,
      ...(credentials ? { auth: credentials } : {}),
      ...(fetchImpl ? { fetch: fetchImpl } : {}),
    };
    return new StratumClient(init);
  }, [baseUrl, credentials, fetchImpl]);
}
