import type { ChangeRequestResponse } from "@stratum/sdk";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, memoryAuthStorage } from "../lib/auth.tsx";
import { loadLocalFixture } from "../lib/capabilities.ts";
import { ChangeRequestDetail } from "./ChangeRequestDetail.tsx";

// ─────────────────────────────────────────────────────────────────────────────
// Fixtures
// ─────────────────────────────────────────────────────────────────────────────

const OPEN_PENDING: ChangeRequestResponse = {
  change_request: {
    id: "cr-1",
    title: "redline §3.2 indemnification — narrow carve-out per policy",
    description: "Counterparty's draft mutual indemnity is broader than redline-policy.md §4.b.",
    source_ref: "agent/redline/cr-1",
    target_ref: "main",
    base_commit: "0".repeat(64),
    head_commit: "a4f9c1b2" + "0".repeat(56),
    status: "open",
    created_by: 101,
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
};

const OPEN_APPROVED: ChangeRequestResponse = {
  ...OPEN_PENDING,
  approval_state: {
    ...OPEN_PENDING.approval_state,
    approved: true,
    approval_count: 1,
    approved_by: [42],
  },
};

const MERGED: ChangeRequestResponse = {
  ...OPEN_PENDING,
  change_request: { ...OPEN_PENDING.change_request, status: "merged" },
};

// ─────────────────────────────────────────────────────────────────────────────
// Plumbing
// ─────────────────────────────────────────────────────────────────────────────

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

/** Default policy in the test fixture: ref_rules require all files
 *  viewed → merge is gated. Override via the `requireAllViewed` arg. */
function buildCapabilitiesResponse(requireAllViewed = true): Response {
  const fixture = loadLocalFixture();
  // The fixture mirrors sdk/contracts/capabilities.v1.json; override only
  // the field the action row reads.
  const patched = {
    ...fixture,
    protection: {
      ...fixture.protection,
      ref_rules: {
        ...fixture.protection.ref_rules,
        require_all_files_viewed_default: requireAllViewed,
      },
    },
  };
  return new Response(JSON.stringify(patched), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

interface RenderOptions {
  readonly id?: string;
  readonly onBack?: () => void;
  /** Manifest's require_all_files_viewed_default for the ref_rules group. */
  readonly requireAllViewed?: boolean;
  /** Override the response for GET /change-requests/:id/approvals (D3.4). */
  readonly approvalsResponse?: Response | (() => Response | Promise<Response>);
  /** Override the response for GET /change-requests/:id/reviewers (D5). */
  readonly reviewersResponse?: Response | (() => Response | Promise<Response>);
  /** Override the response for GET /change-requests/:id/comments (D4). */
  readonly commentsResponse?: Response | (() => Response | Promise<Response>);
}

const EMPTY_APPROVALS = () =>
  new Response(JSON.stringify({ approvals: [] }), {
    status: 200,
    headers: { "content-type": "application/json" },
  });

const EMPTY_COMMENTS = () =>
  new Response(JSON.stringify({ comments: [], approval_state: OPEN_PENDING.approval_state }), {
    status: 200,
    headers: { "content-type": "application/json" },
  });

const EMPTY_REVIEWERS = () =>
  new Response(
    JSON.stringify({
      assignments: [],
      approval_state: OPEN_PENDING.approval_state,
      require_all_files_viewed: OPEN_PENDING.require_all_files_viewed,
    }),
    {
      status: 200,
      headers: { "content-type": "application/json" },
    },
  );

function renderDetail(
  primary: typeof globalThis.fetch | Response | (() => Response | Promise<Response>),
  opts: RenderOptions = {},
) {
  const {
    id = "cr-1",
    onBack = vi.fn(),
    requireAllViewed = false,
    approvalsResponse,
    reviewersResponse,
    commentsResponse,
  } = opts;

  // URL-aware fetch:
  //   /v1/capabilities          → manifest stub (configurable via opts)
  //   /change-requests/:id/approvals → approvals stub (default: empty list)
  //   everything else            → primary (the test's CR-detail stub)
  const primaryFn: typeof globalThis.fetch =
    typeof primary === "function"
      ? (primary as typeof globalThis.fetch)
      : async () => (primary instanceof Response ? primary.clone() : primary);

  const approvalsFn = approvalsResponse
    ? typeof approvalsResponse === "function"
      ? approvalsResponse
      : async () => (approvalsResponse instanceof Response ? approvalsResponse.clone() : approvalsResponse)
    : EMPTY_APPROVALS;
  const commentsFn = commentsResponse
    ? typeof commentsResponse === "function"
      ? commentsResponse
      : async () => (commentsResponse instanceof Response ? commentsResponse.clone() : commentsResponse)
    : EMPTY_COMMENTS;
  const reviewersFn = reviewersResponse
    ? typeof reviewersResponse === "function"
      ? reviewersResponse
      : async () => (reviewersResponse instanceof Response ? reviewersResponse.clone() : reviewersResponse)
    : EMPTY_REVIEWERS;

  globalThis.fetch = (async (input, init) => {
    const url = String(typeof input === "string" || input instanceof URL ? input : input.url);
    const method = (init?.method ?? "GET").toUpperCase();
    if (url.includes("/v1/capabilities")) return buildCapabilitiesResponse(requireAllViewed);
    // Only GET /approvals (list query) gets the empty-list stub.
    // POST /approvals (approve / dismiss mutations) routes to `primary`
    // so existing mutation tests' stubs still drive that response.
    if (method === "GET" && url.includes("/approvals")) return approvalsFn();
    if (method === "GET" && url.includes("/reviewers")) return reviewersFn();
    if (method === "GET" && url.includes("/comments")) return commentsFn();
    return primaryFn(input, init);
  }) as typeof globalThis.fetch;

  const storage = memoryAuthStorage({ type: "user", username: "alice" });
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0, retryDelay: 0 }, mutations: { retry: false } },
  });
  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <AuthProvider storage={storage}>
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
      </AuthProvider>
    );
  }
  return {
    onBack,
    queryClient,
    ...render(<ChangeRequestDetail id={id} onBack={onBack} />, { wrapper: Wrapper }),
  };
}

let originalFetch: typeof globalThis.fetch | undefined;
beforeEach(() => {
  originalFetch = globalThis.fetch;
});
afterEach(() => {
  if (originalFetch) globalThis.fetch = originalFetch;
});

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

describe("ChangeRequestDetail — back link", () => {
  it("always renders Back to Reviews and routes onBack on click", async () => {
    const { onBack } = renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_PENDING)));
    const back = screen.getByRole("button", { name: /back to reviews/i });
    fireEvent.click(back);
    expect(onBack).toHaveBeenCalledTimes(1);
  });
});

describe("ChangeRequestDetail — loading", () => {
  it("renders a skeleton while the fetch is pending", () => {
    renderDetail(vi.fn<typeof fetch>(() => new Promise<Response>(() => undefined)));
    expect(screen.getByLabelText("Loading change request")).toBeTruthy();
  });
});

describe("ChangeRequestDetail — 404", () => {
  it("shows a not-found card mentioning the id and a Return button", async () => {
    const { onBack } = renderDetail(vi.fn<typeof fetch>(async () => httpError(404)), {
      id: "cr-missing",
    });
    expect(
      await screen.findByRole("heading", { name: /change request not found/i }),
    ).toBeTruthy();
    expect(screen.getByText("cr-missing")).toBeTruthy();
    // The header carries "Back to Reviews"; the not-found card carries
    // "Return to Reviews" — distinct labels so each can be targeted.
    fireEvent.click(screen.getByRole("button", { name: /^return to reviews$/i }));
    expect(onBack).toHaveBeenCalledTimes(1);
  });
});

describe("ChangeRequestDetail — 403", () => {
  it("shows a forbidden alert", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => httpError(403)));
    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toMatch(/don't have access/i);
  });
});

describe("ChangeRequestDetail — generic error", () => {
  it("renders an alert + Retry that fires another fetch", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => httpError(503));
    renderDetail(fetchSpy);
    await screen.findByRole("alert");
    // 5xx is non-terminal per isTerminalHttpError → the hook retries
    // up to 2 times before surfacing the error. 1 initial + 2 retries
    // = 3 fetch calls. retryDelay: 0 keeps this fast.
    expect(fetchSpy).toHaveBeenCalledTimes(3);
    fetchSpy.mockClear();
    fireEvent.click(screen.getByRole("button", { name: /retry/i }));
    // Click refetch → 1 fresh attempt + 2 retries = 3 more calls.
    await waitFor(() => expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1));
  });
});

describe("ChangeRequestDetail — populated", () => {
  it("renders title, byline, ref pair, and id prefix", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_PENDING)));
    expect(
      await screen.findByRole("heading", { name: /redline §3.2 indemnification/i }),
    ).toBeTruthy();
    expect(screen.getByText("agent/redline/cr-1")).toBeTruthy();
    expect(screen.getByText("main")).toBeTruthy();
    // First 8 chars of the id.
    expect(screen.getByText("cr-1")).toBeTruthy();
  });

  it("renders the description when present", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_PENDING)));
    expect(
      await screen.findByText(/counterparty's draft mutual indemnity/i),
    ).toBeTruthy();
  });

  it("renders the approval-state breakdown for a pending CR", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_PENDING)));
    await screen.findByRole("heading", { name: /^approval state$/i });
    expect(screen.getByText(/0 \/ 1 approvals/)).toBeTruthy();
    expect(screen.getByText(/rule-main/)).toBeTruthy();
  });

  it("shows 'approved' in the breakdown when the CR is approved", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_APPROVED)));
    await screen.findByRole("heading", { name: /^approval state$/i });
    expect(screen.getByText("approved")).toBeTruthy();
    expect(screen.getByText(/uid:42/)).toBeTruthy();
  });

  it("renders the diff placeholder with the recorded base + head commits", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_PENDING)));
    await screen.findByRole("heading", { name: /^diff$/i });
    expect(screen.getByText(/diff display unblocks/i)).toBeTruthy();
    expect(screen.getByText(OPEN_PENDING.change_request.base_commit)).toBeTruthy();
    expect(screen.getByText(OPEN_PENDING.change_request.head_commit)).toBeTruthy();
  });
});

describe("ChangeRequestDetail — action row (D3 wired)", () => {
  it("on an open + unapproved CR: Approve, Reject, and Request changes enabled; Merge disabled (no approval)", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_PENDING)));
    await screen.findByRole("heading", { name: /redline §3.2 indemnification/i });
    const approve = screen.getByRole("button", { name: /^approve$/i }) as HTMLButtonElement;
    expect(approve.disabled).toBe(false);
    const reject = screen.getByRole("button", { name: /^reject$/i }) as HTMLButtonElement;
    expect(reject.disabled).toBe(false);
    const merge = screen.getByRole("button", { name: /^merge$/i }) as HTMLButtonElement;
    expect(merge.disabled).toBe(true);
    expect(merge.title).toMatch(/approval requirements/i);
    const requestChanges = screen.getByRole("button", { name: /request changes/i }) as HTMLButtonElement;
    expect(requestChanges.disabled).toBe(false);
  });

  it("on an approved CR with require_all_files_viewed_default=true (manifest default): Merge gated with viewing tooltip", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_APPROVED)), {
      requireAllViewed: true,
    });
    await screen.findByRole("heading", { name: /redline §3.2 indemnification/i });
    // Wait for capabilities to load so the gating kicks in.
    await waitFor(() => {
      const merge = screen.getByRole("button", { name: /^merge$/i }) as HTMLButtonElement;
      expect(merge.disabled).toBe(true);
      expect(merge.title).toMatch(/viewed-file tracking/i);
    });
  });

  it("on an approved CR with require_all_files_viewed_default=false: Merge enabled", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_APPROVED)), {
      requireAllViewed: false,
    });
    await screen.findByRole("heading", { name: /redline §3.2 indemnification/i });
    await waitFor(() => {
      const merge = screen.getByRole("button", { name: /^merge$/i }) as HTMLButtonElement;
      expect(merge.disabled).toBe(false);
    });
  });

  it("on a terminal CR (merged): every action disabled with a 'CR is merged' tooltip", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(MERGED)));
    await screen.findByRole("heading", { name: /redline §3.2 indemnification/i });
    const approve = screen.getByRole("button", { name: /^approve$/i }) as HTMLButtonElement;
    expect(approve.disabled).toBe(true);
    expect(approve.title).toMatch(/merged/i);
    const reject = screen.getByRole("button", { name: /^reject$/i }) as HTMLButtonElement;
    expect(reject.disabled).toBe(true);
    const merge = screen.getByRole("button", { name: /^merge$/i }) as HTMLButtonElement;
    expect(merge.disabled).toBe(true);
  });

  it("clicking Approve fires POST /change-requests/:id/approvals", async () => {
    const detailFetch = vi.fn<typeof fetch>(async (input) => {
      const url = String(typeof input === "string" || input instanceof URL ? input : input.url);
      if (url.includes("/approvals")) return okJson({ approval: {}, created: true, approval_state: OPEN_PENDING.approval_state });
      return okJson(OPEN_PENDING);
    });
    renderDetail(detailFetch);
    await screen.findByRole("button", { name: /^approve$/i });
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: /^approve$/i }));
    });
    await waitFor(() => {
      const approvalsCall = detailFetch.mock.calls.find(([u]) =>
        String(u).includes("/change-requests/cr-1/approvals"),
      );
      expect(approvalsCall).toBeTruthy();
      expect(approvalsCall?.[1]?.method).toBe("POST");
    });
  });

  it("surfaces a mutation error inline as a role=alert below the buttons", async () => {
    const detailFetch = vi.fn<typeof fetch>(async (input) => {
      const url = String(typeof input === "string" || input instanceof URL ? input : input.url);
      if (url.includes("/approvals")) return httpError(403, { error: "policy denied" });
      return okJson(OPEN_PENDING);
    });
    renderDetail(detailFetch);
    await screen.findByRole("button", { name: /^approve$/i });
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: /^approve$/i }));
    });
    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toBeTruthy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// D3.4 — Approvals list + inline dismiss
// ─────────────────────────────────────────────────────────────────────────────

const APPROVALS_LIST = {
  approvals: [
    {
      id: "appr-active",
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

function approvalsListResponse(): Response {
  return new Response(JSON.stringify(APPROVALS_LIST), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

describe("ChangeRequestDetail — approvals list (D3.4)", () => {
  it("renders nothing for the Approvals section when the list is empty (default fixture)", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_PENDING)));
    // Wait for the CR detail to render so the approvals list has had its
    // chance to fetch + render-or-skip.
    await screen.findByRole("heading", { name: /^approval state$/i });
    expect(screen.queryByText(/^Approvals$/)).toBeNull();
  });

  it("renders both active + dismissed approvals when present", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_PENDING)), {
      approvalsResponse: approvalsListResponse,
    });
    // Section header
    expect(await screen.findByText(/^Approvals$/)).toBeTruthy();
    // Active approval: uid + comment visible, Dismiss button present
    expect(screen.getByText(/uid:42/)).toBeTruthy();
    expect(screen.getByText(/lgtm/)).toBeTruthy();
    expect(screen.getByRole("button", { name: /^dismiss$/i })).toBeTruthy();
    // Dismissed approval: dismissal trail visible, no Dismiss button on it
    expect(screen.getByText(/uid:17/)).toBeTruthy();
    expect(screen.getByText(/dismissed by uid:0/)).toBeTruthy();
    expect(screen.getByText(/stale head/)).toBeTruthy();
  });

  it("clicking Dismiss reveals the inline reason form (input + Confirm + Cancel)", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_PENDING)), {
      approvalsResponse: approvalsListResponse,
    });
    await screen.findByText(/^Approvals$/);
    fireEvent.click(screen.getByRole("button", { name: /^dismiss$/i }));
    expect(screen.getByRole("textbox", { name: /dismissal reason/i })).toBeTruthy();
    expect(screen.getByRole("button", { name: /confirm dismiss/i })).toBeTruthy();
    expect(screen.getByRole("button", { name: /^cancel$/i })).toBeTruthy();
  });

  it("Cancel collapses the form back to the Dismiss button without firing the mutation", async () => {
    const detailFetch = vi.fn<typeof fetch>(async (input) => {
      const url = String(typeof input === "string" || input instanceof URL ? input : input.url);
      if (url.includes("/dismiss")) {
        throw new Error("Cancel should not call dismiss");
      }
      return okJson(OPEN_PENDING);
    });
    renderDetail(detailFetch, { approvalsResponse: approvalsListResponse });
    await screen.findByText(/^Approvals$/);
    fireEvent.click(screen.getByRole("button", { name: /^dismiss$/i }));
    fireEvent.click(screen.getByRole("button", { name: /^cancel$/i }));
    expect(screen.queryByRole("textbox", { name: /dismissal reason/i })).toBeNull();
    expect(screen.getByRole("button", { name: /^dismiss$/i })).toBeTruthy();
  });

  it("Confirm dismiss with a reason POSTs to /approvals/:aid/dismiss with the reason in body", async () => {
    const detailFetch = vi.fn<typeof fetch>(async (input) => {
      const url = String(typeof input === "string" || input instanceof URL ? input : input.url);
      if (url.includes("/dismiss")) {
        return okJson({ approval: { ...APPROVALS_LIST.approvals[0]!, active: false }, dismissed: true, approval_state: OPEN_PENDING.approval_state });
      }
      return okJson(OPEN_PENDING);
    });
    renderDetail(detailFetch, { approvalsResponse: approvalsListResponse });
    await screen.findByText(/^Approvals$/);
    fireEvent.click(screen.getByRole("button", { name: /^dismiss$/i }));
    const input = screen.getByRole("textbox", { name: /dismissal reason/i });
    fireEvent.change(input, { target: { value: "stale head" } });
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: /confirm dismiss/i }));
    });
    await waitFor(() => {
      const call = detailFetch.mock.calls.find(([u]) =>
        String(u).includes("/approvals/appr-active/dismiss"),
      );
      expect(call).toBeTruthy();
      expect(call?.[1]?.method).toBe("POST");
      expect(String(call?.[1]?.body)).toContain("stale head");
    });
  });

  it("a dismiss 4xx surfaces inline as role=alert without collapsing the form", async () => {
    const detailFetch = vi.fn<typeof fetch>(async (input) => {
      const url = String(typeof input === "string" || input instanceof URL ? input : input.url);
      if (url.includes("/dismiss")) return httpError(403, { error: "not permitted" });
      return okJson(OPEN_PENDING);
    });
    renderDetail(detailFetch, { approvalsResponse: approvalsListResponse });
    await screen.findByText(/^Approvals$/);
    fireEvent.click(screen.getByRole("button", { name: /^dismiss$/i }));
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: /confirm dismiss/i }));
    });
    // Form stays expanded so the user can retry; alert appears below it.
    expect(await screen.findByRole("alert")).toBeTruthy();
    expect(screen.getByRole("textbox", { name: /dismissal reason/i })).toBeTruthy();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// D5 — Reviewer assignment
// ─────────────────────────────────────────────────────────────────────────────

const REVIEWER_LIST = {
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
  approval_state: OPEN_PENDING.approval_state,
  require_all_files_viewed: true,
};

function reviewersListResponse(): Response {
  return new Response(JSON.stringify(REVIEWER_LIST), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

describe("ChangeRequestDetail — reviewers (D5)", () => {
  it("renders assigned reviewers with required and optional markers", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_PENDING)), {
      reviewersResponse: reviewersListResponse,
    });
    expect(await screen.findByRole("heading", { name: /^reviewers$/i })).toBeTruthy();
    expect(screen.getByText("Reviewer 42")).toBeTruthy();
    expect(screen.getByText("Reviewer 7")).toBeTruthy();
    expect(screen.getByText(/^required$/i)).toBeTruthy();
    expect(screen.getByText(/^optional$/i)).toBeTruthy();
  });

  it("assigns a required reviewer through POST /reviewers", async () => {
    const detailFetch = vi.fn<typeof fetch>(async (input, init) => {
      const url = String(typeof input === "string" || input instanceof URL ? input : input.url);
      if (url.includes("/reviewers") && init?.method === "POST") {
        return okJson({
          assignment: { ...REVIEWER_LIST.assignments[0]!, id: "rev-new", reviewer: 88 },
          created: true,
          updated: false,
          approval_state: OPEN_PENDING.approval_state,
          require_all_files_viewed: true,
        });
      }
      return okJson(OPEN_PENDING);
    });
    renderDetail(detailFetch);
    await screen.findByRole("heading", { name: /redline §3.2 indemnification/i });

    fireEvent.change(screen.getByLabelText(/^reviewer$/i), { target: { value: "88" } });
    fireEvent.click(screen.getByRole("checkbox", { name: /required approval/i }));

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: /^assign$/i }));
    });

    await waitFor(() => {
      const call = detailFetch.mock.calls.find(([u]) => String(u).includes("/reviewers"));
      expect(call).toBeTruthy();
      expect(call?.[1]?.method).toBe("POST");
      expect(String(call?.[1]?.body)).toContain('"reviewer_uid":88');
      expect(String(call?.[1]?.body)).toContain('"required":true');
    });
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// D4 — Comments thread + request changes
// ─────────────────────────────────────────────────────────────────────────────

const COMMENT_LIST = {
  comments: [
    {
      id: "cmt-1",
      change_request_id: "cr-1",
      author: 42,
      body: "Please align the cap with the insurance schedule.",
      path: "/contracts/acme.md",
      kind: "changes_requested" as const,
      active: true,
      version: 1,
    },
    {
      id: "cmt-2",
      change_request_id: "cr-1",
      author: 101,
      body: "Confirmed. I updated the cap language.",
      path: null,
      kind: "general" as const,
      active: true,
      version: 1,
    },
  ],
  approval_state: OPEN_PENDING.approval_state,
};

function commentsListResponse(): Response {
  return new Response(JSON.stringify(COMMENT_LIST), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

describe("ChangeRequestDetail — comments (D4)", () => {
  it("renders the review thread with changes-requested and path markers", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_PENDING)), {
      commentsResponse: commentsListResponse,
    });
    expect(await screen.findByRole("heading", { name: /^comments$/i })).toBeTruthy();
    expect(screen.getByText(/align the cap/i)).toBeTruthy();
    expect(screen.getByText(/changes requested/i)).toBeTruthy();
    expect(screen.getByText("/contracts/acme.md")).toBeTruthy();
    expect(screen.getByText(/updated the cap language/i)).toBeTruthy();
  });

  it("Request changes opens the composer and posts a changes_requested comment", async () => {
    const detailFetch = vi.fn<typeof fetch>(async (input, init) => {
      const url = String(typeof input === "string" || input instanceof URL ? input : input.url);
      if (url.includes("/comments") && init?.method === "POST") {
        return okJson({
          comment: { ...COMMENT_LIST.comments[0]!, id: "cmt-new", body: "Please narrow the carve-out." },
          created: true,
          approval_state: OPEN_PENDING.approval_state,
        });
      }
      return okJson(OPEN_PENDING);
    });
    renderDetail(detailFetch);
    await screen.findByRole("heading", { name: /redline §3.2 indemnification/i });

    fireEvent.click(screen.getByRole("button", { name: /request changes/i }));
    const composer = screen.getByRole("textbox", { name: /comment/i });
    fireEvent.change(composer, { target: { value: "Please narrow the carve-out." } });

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: /^send$/i }));
    });

    await waitFor(() => {
      const call = detailFetch.mock.calls.find(([u]) => String(u).includes("/comments"));
      expect(call).toBeTruthy();
      expect(call?.[1]?.method).toBe("POST");
      expect(String(call?.[1]?.body)).toContain("Please narrow the carve-out.");
      expect(String(call?.[1]?.body)).toContain("changes_requested");
    });
  });
});
