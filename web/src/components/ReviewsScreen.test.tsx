import type { ChangeRequestListResponse, ChangeRequestResponse } from "@stratum/sdk";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, memoryAuthStorage } from "../lib/auth.tsx";
import { ReviewsScreen, type FilterController } from "./ReviewsScreen.tsx";

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
    source_ref: "agent/redline/cr-2",
    created_by: 101, // agentish — uid >= 100
  },
  approval_state: { ...HUMAN_CR.approval_state, change_request_id: "cr-2" },
};

const MERGED_CR: ChangeRequestResponse = {
  ...HUMAN_CR,
  change_request: {
    ...HUMAN_CR.change_request,
    id: "cr-3",
    title: "Earlier merged CR",
    source_ref: "review/cr-3-merged",
    status: "merged",
  },
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

function renderWith(fetchImpl: typeof globalThis.fetch, controller?: FilterController) {
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
  return render(
    <ReviewsScreen {...(controller ? { controller } : {})} />,
    { wrapper: Wrapper },
  );
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
    // Wait for the populated state — articles imply the data has resolved.
    const articles = await screen.findAllByRole("article");
    // Scope badge assertions inside the article cards so the filter-chip
    // labels ("merged", etc.) don't collide with the status badges.
    const badges = articles.map((a) => within(a).getAllByText(/^(open|merged|rejected|ready)$/i));
    // Each article has exactly one status badge.
    expect(badges.flat()).toHaveLength(3);
    // Two open + one merged, in source order: agent CR, human CR, merged CR.
    expect(badges[0]![0]!.textContent).toBe("open");
    expect(badges[1]![0]!.textContent).toBe("open");
    expect(badges[2]![0]!.textContent).toBe("merged");
  });

  it("distinguishes agent vs human authors by mark", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    await screen.findByRole("heading", { name: /redline §3.2 indemnification/i });
    expect(screen.getAllByTitle(/agent-authored/i)).toHaveLength(1);
    expect(screen.getAllByTitle(/human-authored/i).length).toBeGreaterThan(0);
  });

  it("renders the four filter chips with per-status counts", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    const group = await screen.findByRole("radiogroup", { name: /filter by status/i });
    expect(group).toBeTruthy();
    const chips = within(group).getAllByRole("radio");
    expect(chips.map((c) => c.textContent?.trim())).toEqual([
      "all3",
      "open2",
      "merged1",
      "rejected0",
    ]);
  });

  it("'all' is the default filter (aria-checked) on first load", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    const group = await screen.findByRole("radiogroup", { name: /filter by status/i });
    const all = within(group).getByRole("radio", { name: /^all/i });
    expect(all.getAttribute("aria-checked")).toBe("true");
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// D1.2 — filter + search
// ─────────────────────────────────────────────────────────────────────────────

describe("ReviewsScreen — filter chips", () => {
  it("clicking 'merged' narrows the visible cards to merged CRs", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    await screen.findAllByRole("article");
    const group = screen.getByRole("radiogroup", { name: /filter by status/i });
    fireEvent.click(within(group).getByRole("radio", { name: /^merged/i }));
    const articles = screen.getAllByRole("article");
    expect(articles).toHaveLength(1);
    expect(within(articles[0]!).getByRole("heading").textContent).toMatch(/earlier merged/i);
  });

  it("clicking 'rejected' shows the no-matches state (none in the fixture)", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    await screen.findAllByRole("article");
    const group = screen.getByRole("radiogroup", { name: /filter by status/i });
    fireEvent.click(within(group).getByRole("radio", { name: /^rejected/i }));
    expect(screen.queryAllByRole("article")).toHaveLength(0);
    expect(screen.getByRole("heading", { name: /no matches/i })).toBeTruthy();
  });

  it("toggles aria-checked when the user picks a different filter", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    await screen.findAllByRole("article");
    const group = screen.getByRole("radiogroup", { name: /filter by status/i });
    const open = within(group).getByRole("radio", { name: /^open/i });
    fireEvent.click(open);
    expect(open.getAttribute("aria-checked")).toBe("true");
    expect(within(group).getByRole("radio", { name: /^all/i }).getAttribute("aria-checked")).toBe("false");
  });
});

describe("ReviewsScreen — search input", () => {
  it("filters cards by case-insensitive title substring", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    await screen.findAllByRole("article");
    const search = screen.getByRole("searchbox");
    fireEvent.change(search, { target: { value: "INDEMNIFICATION" } });
    const articles = screen.getAllByRole("article");
    expect(articles).toHaveLength(2);
  });

  it("matches by source ref too", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    await screen.findAllByRole("article");
    // agent/redline/cr-2 is unique to AGENT_CR's source ref.
    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "agent/redline" } });
    expect(screen.getAllByRole("article")).toHaveLength(1);
  });

  it("whitespace-only query keeps everything visible", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    await screen.findAllByRole("article");
    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "    " } });
    expect(screen.getAllByRole("article")).toHaveLength(3);
  });
});

describe("ReviewsScreen — no-matches state", () => {
  it("appears when filter + search would empty the list", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    await screen.findAllByRole("article");
    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "zzzzz" } });
    expect(screen.getByRole("heading", { name: /no matches/i })).toBeTruthy();
  });

  it("'Clear filters' restores the full list and resets the chip to 'all'", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)));
    await screen.findAllByRole("article");
    const group = screen.getByRole("radiogroup", { name: /filter by status/i });
    fireEvent.click(within(group).getByRole("radio", { name: /^rejected/i }));
    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "zzzzz" } });
    fireEvent.click(screen.getByRole("button", { name: /clear filters/i }));
    expect(screen.getAllByRole("article")).toHaveLength(3);
    expect((screen.getByRole("searchbox") as HTMLInputElement).value).toBe("");
    expect(within(group).getByRole("radio", { name: /^all/i }).getAttribute("aria-checked")).toBe("true");
  });
});

describe("ReviewsScreen — external controller (URL-state bridge)", () => {
  it("honors the controller's filter + query instead of local state", async () => {
    const setFilter = vi.fn();
    const setQuery = vi.fn();
    const clear = vi.fn();
    const controller: FilterController = {
      filter: "merged",
      query: "",
      setFilter,
      setQuery,
      clear,
    };
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)), controller);

    // Filter from the controller is applied: only the merged CR renders.
    const articles = await screen.findAllByRole("article");
    expect(articles).toHaveLength(1);
    expect(within(articles[0]!).getByRole("heading").textContent).toMatch(/earlier merged/i);

    // The merged chip is aria-checked because the controller's filter is 'merged'.
    const group = screen.getByRole("radiogroup", { name: /filter by status/i });
    expect(within(group).getByRole("radio", { name: /^merged/i }).getAttribute("aria-checked")).toBe("true");
  });

  it("delegates filter clicks to the controller (no local state change)", async () => {
    const setFilter = vi.fn();
    const controller: FilterController = {
      filter: "all",
      query: "",
      setFilter,
      setQuery: vi.fn(),
      clear: vi.fn(),
    };
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)), controller);

    await screen.findAllByRole("article");
    const group = screen.getByRole("radiogroup", { name: /filter by status/i });
    fireEvent.click(within(group).getByRole("radio", { name: /^open/i }));
    expect(setFilter).toHaveBeenCalledWith("open");

    // Controller is the source of truth; the chip's checked state reflects
    // the controller value (still 'all'), not the click target.
    expect(within(group).getByRole("radio", { name: /^all/i }).getAttribute("aria-checked")).toBe("true");
  });

  it("delegates search typing to the controller", async () => {
    const setQuery = vi.fn();
    const controller: FilterController = {
      filter: "all",
      query: "",
      setFilter: vi.fn(),
      setQuery,
      clear: vi.fn(),
    };
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)), controller);

    await screen.findAllByRole("article");
    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "tokenizer" } });
    expect(setQuery).toHaveBeenCalledWith("tokenizer");
  });

  it("Clear filters delegates to controller.clear()", async () => {
    const clear = vi.fn();
    const controller: FilterController = {
      filter: "rejected", // no rejected CRs in fixture → no-matches state
      query: "",
      setFilter: vi.fn(),
      setQuery: vi.fn(),
      clear,
    };
    renderWith(vi.fn<typeof fetch>(async () => okJson(POPULATED)), controller);

    fireEvent.click(await screen.findByRole("button", { name: /clear filters/i }));
    expect(clear).toHaveBeenCalled();
  });
});

describe("ReviewsScreen — toolbar gating", () => {
  it("does not render the filter toolbar in the empty state (no data to filter)", async () => {
    renderWith(vi.fn<typeof fetch>(async () => okJson(EMPTY)));
    await screen.findByRole("heading", { name: /no change requests yet/i });
    expect(screen.queryByRole("radiogroup")).toBeNull();
    expect(screen.queryByRole("searchbox")).toBeNull();
  });

  it("does not render the toolbar in the error state", async () => {
    renderWith(vi.fn<typeof fetch>(async () => httpError(503)));
    await screen.findByRole("alert");
    expect(screen.queryByRole("radiogroup")).toBeNull();
  });
});
