/**
 * Capabilities — the single read path for `GET /v1/capabilities`.
 *
 *   - Types come from `@stratum/sdk` (`CapabilityManifest`). Backend owns
 *     the contract; we import it. If backend drifts the shape under v1,
 *     the typecheck breaks loudly — by design (see
 *     docs/plans/2026-05-15-capability-manifest-v1-lock.md).
 *
 *   - Mock data mirrors `sdk/contracts/capabilities.v1.{json,durable-cloud.json}`
 *     verbatim. Copies live under `__fixtures__/` so Vite's project-root
 *     constraint doesn't have to be relaxed. A repo-level CI check enforces
 *     byte-equality between the SDK fixture and ours (see roadmap §F4).
 *
 *   - `loadCapabilities()` is the public read. It hits the real endpoint
 *     in production, falls back to the local mock in dev when the server
 *     isn't reachable, and routes `hints.banner` through `parseBanner()`
 *     so untyped JSON never leaks into components.
 */

import type {
  CapabilityHints,
  CapabilityManifest,
} from "@stratum/sdk";
import { useQuery, type UseQueryResult } from "@tanstack/react-query";
import { parseBanner, type Banner } from "./banner-parser.ts";
import localFixture from "./__fixtures__/capabilities.v1.json";
import durableCloudFixture from "./__fixtures__/capabilities.v1.durable-cloud.json";

/**
 * The application-side view of `hints`. Identical to the SDK's
 * `CapabilityHints` except `banner` is the validated `Banner | null`,
 * not the wire `unknown | null`.
 */
export interface SafeHints extends Omit<CapabilityHints, "banner"> {
  readonly banner: Banner | null;
}

/** The application-side view of the manifest with validated hints. */
export interface SafeCapabilities extends Omit<CapabilityManifest, "hints"> {
  readonly hints: SafeHints;
}

/** Cast helper — the SDK type currently lacks per-server-mode discrimination. */
export function isDurableCloud(manifest: SafeCapabilities): boolean {
  return manifest.server.core_runtime === "durable-cloud";
}

/**
 * Load the manifest. In production this hits the real `/v1/capabilities`.
 * In dev, when the server isn't running, we fall back to the appropriate
 * mock so the UI keeps working.
 *
 * The optional `fetcher` arg is a test seam — pass `() => Promise.reject()`
 * to force the fallback, or `() => Promise.resolve(customManifest)` to inject
 * a fixture from a Playwright test.
 */
export async function loadCapabilities(
  fetcher: () => Promise<CapabilityManifest> = defaultFetch,
): Promise<SafeCapabilities> {
  let raw: CapabilityManifest;
  try {
    raw = await fetcher();
  } catch (error) {
    if (import.meta.env.DEV) {
      // eslint-disable-next-line no-console
      console.warn(
        "[capabilities] /v1/capabilities unreachable; falling back to local fixture",
        error,
      );
      raw = localFixture as unknown as CapabilityManifest;
    } else {
      throw error;
    }
  }
  return sanitise(raw);
}

/** Pull the local-mode fixture without hitting the network. Used by tests + storybook-style spikes. */
export function loadLocalFixture(): SafeCapabilities {
  return sanitise(localFixture as unknown as CapabilityManifest);
}

/** Pull the durable-cloud-mode fixture. Used to render the "what's missing in durable-cloud" UI in dev. */
export function loadDurableCloudFixture(): SafeCapabilities {
  return sanitise(durableCloudFixture as unknown as CapabilityManifest);
}

// ─────────────────────────────────────────────────────────────────────────────
// Internals
// ─────────────────────────────────────────────────────────────────────────────

function sanitise(raw: CapabilityManifest): SafeCapabilities {
  const banner = parseBanner(raw.hints.banner);
  // Build the SafeHints explicitly so we never leak unvalidated `banner` through.
  const hints: SafeHints = {
    banner,
    branding: raw.hints.branding,
    support_url: raw.hints.support_url,
  };
  return { ...raw, hints };
}

async function defaultFetch(): Promise<CapabilityManifest> {
  const res = await fetch("/v1/capabilities", { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`/v1/capabilities ${res.status} ${res.statusText}`);
  return (await res.json()) as CapabilityManifest;
}

// ─────────────────────────────────────────────────────────────────────────────
// React hook — useCapabilities()
// ─────────────────────────────────────────────────────────────────────────────

/** Stable query key so any callsite reading the manifest hits the same cache. */
export const capabilitiesKey = ["capabilities"] as const;

/**
 * Read the manifest from cache. The fetch is cheap, but the response is
 * stable for ~minutes (revision bumps are infrequent) — we cache it for
 * the session and let the regular invalidation cadence pick up changes.
 *
 * Per Cache-Control: max-age=60 on /v1/capabilities, a 5-minute staleTime
 * keeps the manifest warm without thrashing.
 */
export function useCapabilities(): UseQueryResult<SafeCapabilities, Error> {
  return useQuery({
    queryKey: capabilitiesKey,
    queryFn: () => loadCapabilities(),
    staleTime: 5 * 60_000,
    gcTime: 30 * 60_000,
    refetchOnWindowFocus: false, // manifest doesn't drift mid-session
  });
}
