import type { ChangeRequestListResponse, ChangeRequestResponse } from "@stratum/sdk";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, memoryAuthStorage } from "../lib/auth.tsx";
import { ReviewsScreen } from "./ReviewsScreen.tsx";

// ─────────────────────────────────────────────────────────────────────────────
// Fixtures
// ─────────────────────────────────────────────────────────────────────────────

const HUMAN_CR: ChangeRequestResponse = {
  change_request: {
    id: "cr-1",
    title: "Update redline policy v3 — narrow indemnification ladder",
    description: null,
    source_ref: "review/cr-1",
    target_ref: "main",
    base_commit: "0".repeat(64),
    head_commit: "a4f9c1b2" + "0".repeat(56),
    status: "open",
    created_by: 1,
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

const AGENT_CR: ChangeRequestResponse = {
  ...HUMAN_CR,
  change_request: {
    ...HUMAN_CR.change_request,
    id: "cr-2",
    title: "redline §3.2 indemnification — narrow carve-out per policy",
    created_by: 101, // agentish — uid >= 100
  },
  approval_state: { ...HUMAN_CR.approval_state, change_request_id: "cr-2" },
};

const MERGED_CR: ChangeRequestResponse = {
  ...HUMAN_CR,
  change_request: { ...HUMAN_CR.change_request, id: "cr-3", title: "Earlier merged CR", status: "merged" },
  approval_state: {
    ...HUMAN_CR.approval_state,
    change_request_id: "cr-3",
    approved: true,
    approval_count: 1,
  },
};

const POPULATED: ChangeRequestListResponse = {
  change_requests: [AGENT_CR, HUMAN_CR, MERGED_CR],
};

const EMPTY: ChangeRequestListResponse = { change_requests: [] };

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

function renderWith(fetchImpl: typeof globalThis.fetch) {
  globalThis.fetch = fetchImpl;
  const storage = memoryAuthStorage({ type: "user", username: "alice" });
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <AuthProvider storage={storage}>
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
      </AuthProvider>
    );
  }
  return render(<ReviewsScreen />, { wrapper: Wrapper });
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

describe("ReviewsScreen — chrome", () => {
  it("always renders the title + phase label", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(EMPTY)));
    expect(await screen.findByRole("heading", { name: "Reviews" })).toBeTruthy();
    expect(screen.getByText(/phase d/i)).toBeTruthy();
  });
});

describe("ReviewsScreen — loading state", () => {
  it("renders skeleton rows while the query is pending", () => {
    // Never-resolving fetch keeps the query in flight.
    const fetchSpy = vi.fn<typeof fetch>(() => new Promise<Response>(() => undefined));
    renderWith(fetchSpy);
    expect(screen.getByLabelText("Loading change requests")).toBeTruthy();
  });
});

describe("ReviewsScreen — empty state", () => {
  it("shows the editorial empty card when there are no change requests", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(EMPTY)));
    expect(
      await screen.findByRole("heading", { name: /no change requests yet/i }),
    ).toBeTruthy();
  });
});

describe("ReviewsScreen — error state", () => {
  it("renders an alert with the error message and a Retry button", async () => {
    renderWith(vi.fn<typeof fetch>(async () => httpError(403)));
    const alert = await screen.findByRole("alert");
    expect(alert).toBeTruthy();
    expect(screen.getByRole("button", { name: /retry/i })).toBeTruthy();
  });

  it("clicking Retry fires another fetch", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => httpError(503));
    renderWith(fetchSpy);
    await screen.findByRole("alert");
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole("button", { name: /retry/i }));
    await waitFor(() => expect(fetchSpy).toHaveBeenCalledTimes(2));
  });
});

describe("ReviewsScreen — populated state", () => {
  it("renders one card per change request", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    // Each card is an <article>; articles have an implicit ARIA role.
    // We query that rather than <li> because Tailwind's Preflight resets
    // `list-style: none` which downgrades the listitem role on <ul>/<li>.
    const articles = await screen.findAllByRole("article");
    expect(articles).toHaveLength(3);
  });

  it("renders the change-request titles", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    expect(
      await screen.findByRole("heading", { name: /redline §3.2 indemnification/i }),
    ).toBeTruthy();
    expect(screen.getByRole("heading", { name: /update redline policy v3/i })).toBeTruthy();
  });

  it("renders the right status badge per CR", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    await screen.findByRole("heading", { name: /update redline policy v3/i });
    // Open CR → "open" badge
    expect(screen.getAllByText("open").length).toBeGreaterThan(0);
    // Merged CR → "merged" badge
    expect(screen.getByText("merged")).toBeTruthy();
  });

  it("distinguishes agent vs human authors by mark", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    await screen.findByRole("heading", { name: /redline §3.2 indemnification/i });
    expect(screen.getAllByTitle(/agent-authored/i)).toHaveLength(1);
    expect(screen.getAllByTitle(/human-authored/i).length).toBeGreaterThan(0);
  });

  it("shows the summary row in the header (merged + rejected counts)", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    await screen.findByRole("heading", { name: /redline §3.2 indemnification/i });
    // "open" appears both in the summary and on every open card's status
    // badge — ambiguous. The merged / rejected counts are unique to the
    // summary row, so we assert those.
    expect(screen.getByText(/1 merged/)).toBeTruthy();
    expect(screen.getByText(/0 rejected/)).toBeTruthy();
  });
});
