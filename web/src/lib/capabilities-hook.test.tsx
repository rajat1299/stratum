import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { capabilitiesKey, useCapabilities } from "./capabilities.ts";

/**
 * Hook tests live in a separate .tsx file because the rest of
 * capabilities.test.ts is pure-function tests and stays .ts. The
 * QueryClientProvider here is the one wiring difference; everything
 * else mirrors the other hook test setup.
 */

function okJson(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

let originalFetch: typeof globalThis.fetch | undefined;
beforeEach(() => {
  originalFetch = globalThis.fetch;
});
afterEach(() => {
  if (originalFetch) globalThis.fetch = originalFetch;
});

function wrapWithQueryClient(client = new QueryClient({
  defaultOptions: { queries: { retry: false, gcTime: 0 } },
})) {
  function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  }
  return { Wrapper, client };
}

describe("useCapabilities", () => {
  it("returns the parsed local fixture in dev when fetch fails (loadCapabilities fallback)", async () => {
    globalThis.fetch = vi.fn<typeof fetch>(async () => {
      throw new Error("network down");
    });
    const { Wrapper } = wrapWithQueryClient();
    const { result } = renderHook(() => useCapabilities(), { wrapper: Wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.server.backend_mode).toBe("local");
    expect(result.current.data?.revision).toMatch(/^\d{4}-\d{2}-\d{2}-\d+$/);
  });

  it("returns the live manifest when /v1/capabilities responds 200", async () => {
    const sample = {
      revision: "2026-05-17-1",
      server: { name: "stratum", version: "1.0.0", build: null, backend_mode: "durable-cloud", core_runtime: "durable-cloud", build_features: [] },
      auth: { modes: [], providers: [] },
      routes: {},
      diff: { format: "text/v1", max_text_diff_bytes: 0, max_text_diff_cells: 0, context_lines: 3, supported_fragment_kinds: [], json_format_available: false },
      protection: { ref_rules: { available: true, required_approvals_max: 16, require_all_files_viewed_default: true }, path_rules: { available: true, required_approvals_max: 16, target_ref_optional: true, require_all_files_viewed_default: true } },
      idempotency: { header: "Idempotency-Key", max_key_bytes: 255, stale_pending_seconds: 60, completed_retention_seconds: 86400, endpoints_supported: [] },
      recovery: { available: false, phases: [], destructive_cleanup_enabled: false, scheduler_present: false },
      limits: { max_file_size_bytes: 0, max_inodes: 0, max_depth: 0, audit_default_limit: 0, audit_max_limit: 0, log_max_limit: 0 },
      hints: { banner: null, branding: null, support_url: null },
    };
    globalThis.fetch = vi.fn<typeof fetch>(async () => okJson(sample));
    const { Wrapper } = wrapWithQueryClient();
    const { result } = renderHook(() => useCapabilities(), { wrapper: Wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.server.core_runtime).toBe("durable-cloud");
    // hints.banner runs through parseBanner — null in stays null out.
    expect(result.current.data?.hints.banner).toBeNull();
  });

  it("shares cache across hook invocations (single fetch for many consumers)", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => {
      throw new Error("network down — fall back to fixture");
    });
    globalThis.fetch = fetchSpy;
    const { Wrapper } = wrapWithQueryClient();
    function useTwo() {
      const a = useCapabilities();
      const b = useCapabilities();
      return { a, b };
    }
    const { result } = renderHook(useTwo, { wrapper: Wrapper });
    await waitFor(() => expect(result.current.a.isSuccess).toBe(true));
    expect(result.current.b.isSuccess).toBe(true);
    // Both hooks return the same underlying query result — only one fetch.
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });

  it("exposes a stable query key for cross-callsite invalidation", () => {
    expect(capabilitiesKey).toEqual(["capabilities"]);
  });
});
