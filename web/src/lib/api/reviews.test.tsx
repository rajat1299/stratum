import type { ChangeRequestListResponse } from "@stratum/sdk";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, memoryAuthStorage } from "../auth.tsx";
import {
  reviewKeys,
  useApprovals,
  useApproveChangeRequest,
  useChangeRequest,
  useChangeRequestList,
  useChangeRequests,
  useComments,
  useCreateComment,
  useDismissApproval,
  useAssignReviewer,
  useMergeChangeRequest,
  useReviewers,
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
        require_all_files_viewed: true,
      },
      require_all_files_viewed: true,
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
  it("list, detail, approvals, reviewers, and comments keys all start with the 'change-requests' root", () => {
    expect(reviewKeys.all).toEqual(["change-requests"]);
    expect(reviewKeys.list()).toEqual(["change-requests", "list"]);
    expect(reviewKeys.detail("cr-42")).toEqual(["change-requests", "detail", "cr-42"]);
    expect(reviewKeys.approvals("cr-42")).toEqual(["change-requests", "approvals", "cr-42"]);
    expect(reviewKeys.reviewers("cr-42")).toEqual(["change-requests", "reviewers", "cr-42"]);
    expect(reviewKeys.comments("cr-42")).toEqual(["change-requests", "comments", "cr-42"]);
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

  it("invalidates the approvals list (so the dismissed row re-renders inactive)", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(APPROVAL_RESPONSE));
    const { Wrapper, queryClient } = wrapAuthed(fetchSpy);
    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
    const { result } = renderHook(() => useDismissApproval(), { wrapper: Wrapper });
    await act(async () => {
      await result.current.mutateAsync({ id: "cr-1", approvalId: "appr-1" });
    });
    const calledKeys = invalidateSpy.mock.calls.map((c) => c[0]?.queryKey);
    expect(calledKeys).toContainEqual(reviewKeys.approvals("cr-1"));
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// D3.4 — useApprovals (list)
// ─────────────────────────────────────────────────────────────────────────────

const APPROVALS_LIST_RESPONSE = {
  approvals: [
    {
      id: "appr-1",
      change_request_id: "cr-1",
      head_commit: "a4f9c1b2" + "0".repeat(56),
      approved_by: 42,
      comment: "lgtm",
      active: true,
      version: 1,
    },
    {
      id: "appr-old",
      change_request_id: "cr-1",
      head_commit: "0".repeat(64),
      approved_by: 17,
      comment: null,
      active: false,
      dismissed_by: 0,
      dismissal_reason: "stale head",
      version: 2,
    },
  ],
};

describe("useApprovals", () => {
  it("GETs /change-requests/:id/approvals and returns the parsed list", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(APPROVALS_LIST_RESPONSE));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useApprovals("cr-1"), { wrapper: Wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.approvals).toHaveLength(2);
    const call = fetchSpy.mock.calls[0];
    if (!call) throw new Error("fetch was not called");
    expect(String(call[0])).toContain("change-requests/cr-1/approvals");
    expect(call[1]?.method).toBe("GET");
  });

  it("returns both active and inactive approvals in source order", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(APPROVALS_LIST_RESPONSE));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useApprovals("cr-1"), { wrapper: Wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    const approvals = result.current.data?.approvals ?? [];
    expect(approvals[0]?.active).toBe(true);
    expect(approvals[1]?.active).toBe(false);
    expect(approvals[1]?.dismissal_reason).toBe("stale head");
  });

  it("surfaces 4xx as terminal (no retry)", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => httpError(403));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useApprovals("cr-forbidden"), { wrapper: Wrapper });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });
});

describe("useApproveChangeRequest — also invalidates the approvals list", () => {
  it("a successful approve mutation invalidates reviewKeys.approvals(id)", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(APPROVAL_RESPONSE));
    const { Wrapper, queryClient } = wrapAuthed(fetchSpy);
    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
    const { result } = renderHook(() => useApproveChangeRequest(), { wrapper: Wrapper });
    await act(async () => {
      await result.current.mutateAsync({ id: "cr-1" });
    });
    const calledKeys = invalidateSpy.mock.calls.map((c) => c[0]?.queryKey);
    expect(calledKeys).toContainEqual(reviewKeys.approvals("cr-1"));
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// D5 — Reviewers
// ─────────────────────────────────────────────────────────────────────────────

const REVIEWER_LIST_RESPONSE = {
  assignments: [
    {
      id: "rev-1",
      change_request_id: "cr-1",
      reviewer: 42,
      assigned_by: 1,
      required: true,
      active: true,
      version: 1,
    },
    {
      id: "rev-2",
      change_request_id: "cr-1",
      reviewer: 7,
      assigned_by: 1,
      required: false,
      active: true,
      version: 1,
    },
  ],
  approval_state: SAMPLE.change_requests[0]!.approval_state,
  require_all_files_viewed: true,
};

const REVIEWER_ASSIGN_RESPONSE = {
  assignment: {
    id: "rev-3",
    change_request_id: "cr-1",
    reviewer: 88,
    assigned_by: 1,
    required: true,
    active: true,
    version: 1,
  },
  created: true,
  updated: false,
  approval_state: SAMPLE.change_requests[0]!.approval_state,
  require_all_files_viewed: true,
};

describe("useReviewers", () => {
  it("GETs /change-requests/:id/reviewers and returns assignment rows", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(REVIEWER_LIST_RESPONSE));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useReviewers("cr-1"), { wrapper: Wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.assignments).toHaveLength(2);
    const call = fetchSpy.mock.calls[0];
    if (!call) throw new Error("fetch was not called");
    expect(String(call[0])).toContain("change-requests/cr-1/reviewers");
    expect(call[1]?.method).toBe("GET");
  });
});

describe("useAssignReviewer", () => {
  it("POSTs reviewer_uid and required to /change-requests/:id/reviewers", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(REVIEWER_ASSIGN_RESPONSE));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useAssignReviewer(), { wrapper: Wrapper });

    await act(async () => {
      await result.current.mutateAsync({ id: "cr-1", reviewerUid: 88, required: true });
    });

    const call = fetchSpy.mock.calls[0];
    if (!call) throw new Error("fetch was not called");
    expect(String(call[0])).toContain("change-requests/cr-1/reviewers");
    expect(call[1]?.method).toBe("POST");
    expect(headerOf(call[1], "Idempotency-Key")).toMatch(/^[0-9a-f-]{20,}$/i);
    const body = String(call[1]?.body);
    expect(body).toContain('"reviewer_uid":88');
    expect(body).toContain('"required":true');
  });

  it("invalidates reviewers, detail, approvals, and list after assignment", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(REVIEWER_ASSIGN_RESPONSE));
    const { Wrapper, queryClient } = wrapAuthed(fetchSpy);
    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
    const { result } = renderHook(() => useAssignReviewer(), { wrapper: Wrapper });
    await act(async () => {
      await result.current.mutateAsync({ id: "cr-1", reviewerUid: 88, required: false });
    });
    const calledKeys = invalidateSpy.mock.calls.map((c) => c[0]?.queryKey);
    expect(calledKeys).toContainEqual(reviewKeys.reviewers("cr-1"));
    expect(calledKeys).toContainEqual(reviewKeys.detail("cr-1"));
    expect(calledKeys).toContainEqual(reviewKeys.approvals("cr-1"));
    expect(calledKeys).toContainEqual(reviewKeys.list());
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// D4 — Comments
// ─────────────────────────────────────────────────────────────────────────────

const COMMENT_LIST_RESPONSE = {
  comments: [
    {
      id: "cmt-1",
      change_request_id: "cr-1",
      author: 42,
      body: "Looks good — one nit on §3.2.",
      path: null,
      kind: "general" as const,
      active: true,
      version: 1,
    },
    {
      id: "cmt-2",
      change_request_id: "cr-1",
      author: 7,
      body: "Please update the summary before merge.",
      path: "/contracts/loi-acme.docx",
      kind: "changes_requested" as const,
      active: true,
      version: 1,
    },
  ],
};

const COMMENT_CREATE_RESPONSE = {
  comment: {
    id: "cmt-new",
    change_request_id: "cr-1",
    author: 1,
    body: "ack",
    path: null,
    kind: "general" as const,
    active: true,
    version: 1,
  },
  created: true,
};

describe("useComments", () => {
  it("GETs /change-requests/:id/comments and returns the parsed list", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(COMMENT_LIST_RESPONSE));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useComments("cr-1"), { wrapper: Wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.comments).toHaveLength(2);
    const call = fetchSpy.mock.calls[0];
    if (!call) throw new Error("fetch was not called");
    expect(String(call[0])).toContain("change-requests/cr-1/comments");
    expect(call[1]?.method).toBe("GET");
  });

  it("preserves the kind distinction so the thread can render badges", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(COMMENT_LIST_RESPONSE));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useComments("cr-1"), { wrapper: Wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    const comments = result.current.data?.comments ?? [];
    expect(comments[0]?.kind).toBe("general");
    expect(comments[1]?.kind).toBe("changes_requested");
    expect(comments[1]?.path).toBe("/contracts/loi-acme.docx");
  });
});

describe("useCreateComment", () => {
  it("POSTs to /change-requests/:id/comments with an Idempotency-Key + body", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(COMMENT_CREATE_RESPONSE));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useCreateComment(), { wrapper: Wrapper });

    await act(async () => {
      await result.current.mutateAsync({
        id: "cr-1",
        body: "Approving subject to §3.2 revision",
        kind: "changes_requested",
      });
    });

    const call = fetchSpy.mock.calls[0];
    if (!call) throw new Error("fetch was not called");
    expect(String(call[0])).toContain("change-requests/cr-1/comments");
    expect(call[1]?.method).toBe("POST");
    expect(headerOf(call[1], "Idempotency-Key")).toMatch(/^[0-9a-f-]{20,}$/i);
    const body = String(call[1]?.body);
    expect(body).toContain("Approving subject to §3.2");
    expect(body).toContain("changes_requested");
  });

  it("omits optional fields (path, kind) from the request body when not supplied", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(COMMENT_CREATE_RESPONSE));
    const { Wrapper } = wrapAuthed(fetchSpy);
    const { result } = renderHook(() => useCreateComment(), { wrapper: Wrapper });
    await act(async () => {
      await result.current.mutateAsync({ id: "cr-1", body: "ack" });
    });
    const body = JSON.parse(String(fetchSpy.mock.calls[0]?.[1]?.body)) as Record<string, unknown>;
    expect(body).toEqual({ body: "ack" });
  });

  it("invalidates the comments list on success (so the new comment appears)", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(COMMENT_CREATE_RESPONSE));
    const { Wrapper, queryClient } = wrapAuthed(fetchSpy);
    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
    const { result } = renderHook(() => useCreateComment(), { wrapper: Wrapper });
    await act(async () => {
      await result.current.mutateAsync({ id: "cr-1", body: "ack" });
    });
    const calledKeys = invalidateSpy.mock.calls.map((c) => c[0]?.queryKey);
    expect(calledKeys).toContainEqual(reviewKeys.comments("cr-1"));
  });

  it("does NOT invalidate the detail cache (comments don't change approval_state)", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => okJson(COMMENT_CREATE_RESPONSE));
    const { Wrapper, queryClient } = wrapAuthed(fetchSpy);
    const invalidateSpy = vi.spyOn(queryClient, "invalidateQueries");
    const { result } = renderHook(() => useCreateComment(), { wrapper: Wrapper });
    await act(async () => {
      await result.current.mutateAsync({ id: "cr-1", body: "ack" });
    });
    const calledKeys = invalidateSpy.mock.calls.map((c) => c[0]?.queryKey);
    expect(calledKeys).not.toContainEqual(reviewKeys.detail("cr-1"));
  });
});
