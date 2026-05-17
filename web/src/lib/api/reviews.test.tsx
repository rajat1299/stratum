import type { ChangeRequestListResponse } from "@stratum/sdk";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, memoryAuthStorage } from "../auth.tsx";
import { reviewKeys, useChangeRequest, useChangeRequestList, useChangeRequests } from "./reviews.ts";

// ─────────────────────────────────────────────────────────────────────────────
// Test plumbing
// ─────────────────────────────────────────────────────────────────────────────

const SAMPLE: ChangeRequestListResponse = {
  change_requests: [
    {
      change_request: {
        id: "cr-1",
        title: "redline §3.2 indemnification — narrow carve-out per policy",
        description: null,
        source_ref: "agent/redline/cr-1",
        target_ref: "main",
        base_commit: "0".repeat(64),
        head_commit: "a4f9c1b2" + "0".repeat(56),
        status: "open",
        created_by: 2,
        version: 1,
      },
      approval_state: {
        change_request_id: "cr-1",
        required_approvals: 1,
        approval_count: 0,
        approved_by: [],
        required_reviewers: [],
        approved_required_reviewers: [],
        missing_required_reviewers: [],
        approved: false,
        matched_ref_rules: ["rule-main"],
        matched_path_rules: [],
      },
    },
  ],
};

function wrapAuthed(fetchImpl: typeof globalThis.fetch) {
  const storage = memoryAuthStorage({ type: "user", username: "alice" });
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  // The SDK reads from globalThis.fetch, so we swap that for the test.
  globalThis.fetch = fetchImpl;
  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <AuthProvider storage={storage}>
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
      </AuthProvider>
    );
  }
  return { Wrapper, queryClient };
}

let originalFetch: typeof fetch | undefined;
beforeEach(() => {
  originalFetch = globalThis.fetch;
});
afterEach(() => {
  if (originalFetch) globalThis.fetch = originalFetch;
});

function okJson(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

function httpError(status: number, body: unknown = { error: "boom" }): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

describe("reviewKeys — stable factory", () => {
  it("list and detail keys all start with the 'change-requests' root", () => {
    expect(reviewKeys.all).toEqual(["change-requests"]);
    expect(reviewKeys.list()).toEqual(["change-requests", "list"]);
    expect(reviewKeys.detail("cr-42")).toEqual(["change-requests", "detail", "cr-42"]);
  });
});

describe("useChangeRequests", () => {
  it("calls GET /change-requests through the SDK and returns the parsed response", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(SAMPLE));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useChangeRequests(), { wrapper: Wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual(SAMPLE);

    // Sanity-check the request the SDK issued.
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const call = fetchSpy.mock.calls[0];
    if (!call) throw new Error("fetch was not called");
    const [url, init] = call;
    expect(String(url)).toContain("change-requests");
    expect(init?.method).toBe("GET");
  });

  it("surfaces HTTP errors so components can render an error state", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => httpError(403));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useChangeRequests(), { wrapper: Wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error).toBeTruthy();
  });
});

describe("useChangeRequest — detail fetch", () => {
  const DETAIL = SAMPLE.change_requests[0]!;

  it("calls GET /change-requests/:id through the SDK and returns the parsed response", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(DETAIL));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useChangeRequest("cr-1"), { wrapper: Wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual(DETAIL);

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const call = fetchSpy.mock.calls[0];
    if (!call) throw new Error("fetch was not called");
    const [url] = call;
    expect(String(url)).toContain("change-requests/cr-1");
  });

  it("surfaces 404 as a query error (terminal — no retry)", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => httpError(404));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useChangeRequest("cr-missing"), { wrapper: Wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));
    // Detail screen renders a "not found" card when this happens.
    expect(result.current.error).toBeTruthy();
    // Terminal → exactly one call, no retry.
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });

  it("surfaces 403 as a query error (terminal — no retry)", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => httpError(403));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useChangeRequest("cr-forbidden"), { wrapper: Wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });

  it("URL-encodes funky ids (defense — backend ids are safe but be sure)", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(DETAIL));
    const { Wrapper } = wrapAuthed(fetchSpy);
    renderHook(() => useChangeRequest("cr with space"), { wrapper: Wrapper });
    await waitFor(() => expect(fetchSpy).toHaveBeenCalledTimes(1));
    const call = fetchSpy.mock.calls[0];
    if (!call) throw new Error("fetch was not called");
    const url = String(call[0]);
    // The SDK's encodeRouteSegment handles this — we assert it's encoded.
    expect(url).toMatch(/change-requests\/cr%20with%20space|change-requests\/cr\+with\+space/);
  });
});

describe("useChangeRequestList — convenience", () => {
  it("returns an empty array while loading (no `data?.change_requests ?? []` in components)", async () => {
    let resolveFetch: (r: Response) => void = () => undefined;
    const fetchSpy = vi.fn<typeof fetch>(
      () =>
        new Promise<Response>((res) => {
          resolveFetch = res;
        }),
    );
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useChangeRequestList(), { wrapper: Wrapper });

    // While the fetch is pending: isLoading is true, items is an empty array.
    expect(result.current.isLoading).toBe(true);
    expect(result.current.items).toEqual([]);

    resolveFetch(okJson(SAMPLE));
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.items).toEqual(SAMPLE.change_requests);
  });

  it("exposes refetch as a fire-and-forget callable", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(SAMPLE));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useChangeRequestList(), { wrapper: Wrapper });
    await waitFor(() => expect(result.current.items.length).toBeGreaterThan(0));
    fetchSpy.mockClear();
    result.current.refetch();
    await waitFor(() => expect(fetchSpy).toHaveBeenCalledTimes(1));
  });
});
