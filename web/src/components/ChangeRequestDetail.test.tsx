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
  },
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
}

function renderDetail(
  primary: typeof globalThis.fetch | Response | (() => Response | Promise<Response>),
  opts: RenderOptions = {},
) {
  const { id = "cr-1", onBack = vi.fn(), requireAllViewed = false } = opts;

  // URL-aware fetch — capabilities calls get the manifest response, every
  // other URL goes through `primary` (the test's CR-detail stub).
  const primaryFn: typeof globalThis.fetch =
    typeof primary === "function"
      ? (primary as typeof globalThis.fetch)
      : async () => (primary instanceof Response ? primary.clone() : primary);

  globalThis.fetch = (async (input, init) => {
    const url = String(typeof input === "string" || input instanceof URL ? input : input.url);
    if (url.includes("/v1/capabilities")) return buildCapabilitiesResponse(requireAllViewed);
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
  it("on an open + unapproved CR: Approve + Reject enabled, Merge disabled (no approval), Request changes still D4", async () => {
    renderDetail(vi.fn<typeof fetch>(async () => okJson(OPEN_PENDING)));
    await screen.findByRole("heading", { name: /redline §3.2 indemnification/i });
    const approve = screen.getByRole("button", { name: /^approve$/i }) as HTMLButtonElement;
    expect(approve.disabled).toBe(false);
    const reject = screen.getByRole("button", { name: /^reject$/i }) as HTMLButtonElement;
    expect(reject.disabled).toBe(false);
    const merge = screen.getByRole("button", { name: /^merge$/i }) as HTMLButtonElement;
    expect(merge.disabled).toBe(true);
    expect(merge.title).toMatch(/approval requirements/i);
    // Request changes still pending D4 — visible but disabled.
    const requestChanges = screen.getByRole("button", { name: /request changes/i }) as HTMLButtonElement;
    expect(requestChanges.disabled).toBe(true);
    expect(requestChanges.title).toMatch(/D4/);
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
