import type { ChangeRequestListResponse } from "@stratum/sdk";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, memoryAuthStorage } from "../auth.tsx";
import {
  reviewKeys,
  useApproveChangeRequest,
  useChangeRequest,
  useChangeRequestList,
  useChangeRequests,
  useDismissApproval,
  useMergeChangeRequest,
  useRejectChangeRequest,
} from "./reviews.ts";
import { act } from "@testing-library/react";

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

// ─────────────────────────────────────────────────────────────────────────────
// D3 — Mutation hook tests
//
// Each hook is tested for:
//   - The right HTTP method + URL is hit
//   - An Idempotency-Key header is attached (one per mutate() call,
//     not auto-generated by the SDK on every render)
//   - Both detail + list query caches are invalidated on success
//   - Errors surface on the mutation (no auto-retry)
// ─────────────────────────────────────────────────────────────────────────────

function headerOf(init: RequestInit | undefined, name: string): string | null {
  const headers = init?.headers;
  if (!headers) return null;
  if (headers instanceof Headers) return headers.get(name);
  if (Array.isArray(headers)) {
    const found = headers.find(([k]) => k.toLowerCase() === name.toLowerCase());
    return found ? (found[1] ?? null) : null;
  }
  const rec = headers as Record<string, string>;
  for (const k of Object.keys(rec)) {
    if (k.toLowerCase() === name.toLowerCase()) return rec[k] ?? null;
  }
  return null;
}

const APPROVAL_RESPONSE = {
  approval: {
    id: "appr-1",
    change_request_id: "cr-1",
    head_commit: "a4f9c1b2" + "0".repeat(56),
    approved_by: 1,
    comment: null,
    active: true,
    version: 1,
  },
  created: true,
  approval_state: {
    change_request_id: "cr-1",
    required_approvals: 1,
    approval_count: 1,
    approved_by: [1],
    required_reviewers: [],
    approved_required_reviewers: [],
    missing_required_reviewers: [],
    approved: true,
    matched_ref_rules: ["rule-main"],
    matched_path_rules: [],
  },
};

describe("useApproveChangeRequest", () => {
  it("POSTs to /change-requests/:id/approvals with an Idempotency-Key header", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(APPROVAL_RESPONSE));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useApproveChangeRequest(), { wrapper: Wrapper });

    await act(async () => {
      await result.current.mutateAsync({ id: "cr-1", comment: "lgtm" });
    });

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const call = fetchSpy.mock.calls[0];
    if (!call) throw new Error("fetch was not called");
    const [url, init] = call;
    expect(String(url)).toContain("change-requests/cr-1/approvals");
    expect(init?.method).toBe("POST");
    expect(headerOf(init, "Idempotency-Key")).toMatch(/^[0-9a-f-]{20,}$/i);
  });

  it("invalidates the affected detail + list queries on success", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(APPROVAL_RESPONSE));
    const { Wrapper, queryClient } = wrapAuthed(fetchSpy);
    // Spy on invalidateQueries — more reliable than asserting on
    // QueryState.isInvalidated, which flips back to false fast once
    // TanStack Query's refetch-after-invalidate cycle completes (or is
    // skipped when there are no observers).
    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
    const { result } = renderHook(() => useApproveChangeRequest(), { wrapper: Wrapper });
    await act(async () => {
      await result.current.mutateAsync({ id: "cr-1" });
    });
    const calledKeys = invalidateSpy.mock.calls.map((c) => c[0]?.queryKey);
    expect(calledKeys).toContainEqual(reviewKeys.detail("cr-1"));
    expect(calledKeys).toContainEqual(reviewKeys.list());
  });

  it("surfaces HTTP errors on the mutation (no auto-retry)", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => httpError(403));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useApproveChangeRequest(), { wrapper: Wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync({ id: "cr-1" })).rejects.toBeTruthy();
    });
    // Mutation defaults are retry: 0 (set on QueryProvider) — exactly one call.
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });
});

describe("useRejectChangeRequest", () => {
  it("POSTs to /change-requests/:id/reject with an Idempotency-Key", async () => {
    const rejectedCr = { change_request: SAMPLE.change_requests[0]!.change_request, approval_state: SAMPLE.change_requests[0]!.approval_state };
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(rejectedCr));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useRejectChangeRequest(), { wrapper: Wrapper });
    await act(async () => {
      await result.current.mutateAsync({ id: "cr-1" });
    });
    const call = fetchSpy.mock.calls[0];
    if (!call) throw new Error("fetch was not called");
    expect(String(call[0])).toContain("change-requests/cr-1/reject");
    expect(call[1]?.method).toBe("POST");
    expect(headerOf(call[1], "Idempotency-Key")).toMatch(/^[0-9a-f-]{20,}$/i);
  });
});

describe("useMergeChangeRequest", () => {
  it("POSTs to /change-requests/:id/merge", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(SAMPLE.change_requests[0]!));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useMergeChangeRequest(), { wrapper: Wrapper });
    await act(async () => {
      await result.current.mutateAsync({ id: "cr-1" });
    });
    expect(String(fetchSpy.mock.calls[0]?.[0])).toContain("change-requests/cr-1/merge");
  });

  it("each mutate() call gets a distinct idempotency key (one key per user action)", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(SAMPLE.change_requests[0]!));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useMergeChangeRequest(), { wrapper: Wrapper });
    await act(async () => {
      await result.current.mutateAsync({ id: "cr-1" });
      await result.current.mutateAsync({ id: "cr-1" });
    });
    const k1 = headerOf(fetchSpy.mock.calls[0]?.[1], "Idempotency-Key");
    const k2 = headerOf(fetchSpy.mock.calls[1]?.[1], "Idempotency-Key");
    expect(k1).toBeTruthy();
    expect(k2).toBeTruthy();
    expect(k1).not.toBe(k2);
  });
});

describe("useDismissApproval", () => {
  it("POSTs to /change-requests/:id/approvals/:aid/dismiss with the reason in body", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(APPROVAL_RESPONSE));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useDismissApproval(), { wrapper: Wrapper });
    await act(async () => {
      await result.current.mutateAsync({ id: "cr-1", approvalId: "appr-1", reason: "stale head" });
    });
    const call = fetchSpy.mock.calls[0];
    if (!call) throw new Error("fetch was not called");
    expect(String(call[0])).toContain("change-requests/cr-1/approvals/appr-1/dismiss");
    expect(call[1]?.method).toBe("POST");
    // Body should carry the reason.
    expect(String(call[1]?.body)).toContain("stale head");
  });
});
