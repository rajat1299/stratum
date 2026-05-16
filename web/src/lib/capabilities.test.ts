import type { CapabilityManifest } from "@stratum/sdk";
import { describe, expect, it, vi } from "vitest";
import {
  isDurableCloud,
  loadCapabilities,
  loadDurableCloudFixture,
  loadLocalFixture,
} from "./capabilities.ts";

/**
 * Date-counter format the manifest v1 contract guarantees:
 *   YYYY-MM-DD-N  (e.g. "2026-05-16-2")
 * Matching the shape rather than a literal means routine regens don't
 * trip this assertion. The drift check in __fixtures__/capabilities-sync
 * .test.ts is what proves the mirrored fixture matches the SDK one.
 */
const REVISION_SHAPE = /^\d{4}-\d{2}-\d{2}-\d+$/;

describe("loadLocalFixture — sanity check against backend's contract", () => {
  const cap = loadLocalFixture();

  it("declares a well-formed revision (YYYY-MM-DD-N)", () => {
    expect(cap.revision).toMatch(REVISION_SHAPE);
  });

  it("declares local backend + local-state core runtime", () => {
    expect(cap.server.backend_mode).toBe("local");
    expect(cap.server.core_runtime).toBe("local-state");
  });

  it("declares the diff text/v1 format with all six fragment kinds we render", () => {
    expect(cap.diff.format).toBe("text/v1");
    // Backend declares five wire kinds; "unknown" is the FE-internal fail-soft.
    expect(cap.diff.supported_fragment_kinds).toEqual([
      "text-unified",
      "metadata-only",
      "binary",
      "too-large",
      "kind-changed",
    ]);
  });

  it("declares review surface available + admin-gated (the daily driver)", () => {
    expect(cap.routes.review.change_requests.available).toBe(true);
    expect(cap.routes.review.change_requests.admin).toBe(true);
    expect(cap.routes.review.merge.idempotent).toBe(true);
  });

  it("declares semantic search unavailable with a tracking ref (graceful empty state)", () => {
    expect(cap.routes.search.semantic.available).toBe(false);
    expect(cap.routes.search.semantic.reason).toBeTruthy();
    expect(cap.routes.search.semantic.tracking_ref).toBeTruthy();
  });

  it("declares issue_token as non-idempotent (secret-bearing response)", () => {
    expect(cap.routes.workspaces.issue_token.idempotent).toBe(false);
    expect(cap.routes.workspaces.issue_token.reason).toMatch(/secret-bearing/i);
  });

  it("sanitises hints.banner — null in the local fixture stays null", () => {
    expect(cap.hints.banner).toBeNull();
  });
});

describe("loadDurableCloudFixture — the locked-down mode", () => {
  const cap = loadDurableCloudFixture();

  it("declares durable-cloud core runtime", () => {
    expect(cap.server.core_runtime).toBe("durable-cloud");
    expect(isDurableCloud(cap)).toBe(true);
  });
});

describe("loadCapabilities — network fetch with dev fallback", () => {
  it("returns the parsed manifest when fetch succeeds", async () => {
    const sample = loadLocalFixture() as unknown as CapabilityManifest;
    const customFetch = async (): Promise<CapabilityManifest> => sample;
    const result = await loadCapabilities(customFetch);
    expect(result.revision).toBe(sample.revision);
  });

  it("falls back to the local fixture in dev when fetch fails", async () => {
    // `import.meta.env.DEV` is true under vitest because vitest sets DEV by default.
    const result = await loadCapabilities(async () => {
      throw new Error("boom");
    });
    expect(result.revision).toMatch(REVISION_SHAPE);
    expect(result.server.backend_mode).toBe("local");
  });

  it("re-throws in production when fetch fails", async () => {
    const original = import.meta.env.DEV;
    // Hack: vitest exposes import.meta.env as a writable object on the proxy.
    (import.meta.env as { DEV: boolean }).DEV = false;
    try {
      await expect(
        loadCapabilities(async () => {
          throw new Error("network down");
        }),
      ).rejects.toThrow("network down");
    } finally {
      (import.meta.env as { DEV: boolean }).DEV = original;
    }
  });

  it("invokes the default fetcher against /v1/capabilities when none is provided", async () => {
    const fetchSpy = vi.fn(async () => new Response(JSON.stringify(loadLocalFixture()), { status: 200 }));
    const original = globalThis.fetch;
    globalThis.fetch = fetchSpy as typeof fetch;
    try {
      const result = await loadCapabilities();
      expect(fetchSpy).toHaveBeenCalledWith("/v1/capabilities", expect.any(Object));
      expect(result.revision).toMatch(REVISION_SHAPE);
    } finally {
      globalThis.fetch = original;
    }
  });
});
