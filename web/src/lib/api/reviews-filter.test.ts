import type { ChangeRequestResponse } from "@stratum/sdk";
import { describe, expect, it } from "vitest";
import {
  ALL_FILTERS,
  countByFilter,
  filterAndSearch,
  matchesFilter,
  matchesQuery,
} from "./reviews-filter.ts";

// ─────────────────────────────────────────────────────────────────────────────
// Fixtures
// ─────────────────────────────────────────────────────────────────────────────

function row(opts: {
  id: string;
  title: string;
  status: "open" | "merged" | "rejected";
  source?: string;
  target?: string;
  description?: string | null;
}): ChangeRequestResponse {
  return {
    change_request: {
      id: opts.id,
      title: opts.title,
      description: opts.description ?? null,
      source_ref: opts.source ?? "review/" + opts.id,
      target_ref: opts.target ?? "main",
      base_commit: "0".repeat(64),
      head_commit: opts.id.padEnd(64, "0"),
      status: opts.status,
      created_by: 1,
      version: 1,
    },
    approval_state: {
      change_request_id: opts.id,
      required_approvals: 1,
      approval_count: 0,
      approved_by: [],
      required_reviewers: [],
      approved_required_reviewers: [],
      missing_required_reviewers: [],
      approved: false,
      matched_ref_rules: [],
      matched_path_rules: [],
      require_all_files_viewed: true,
    },
    require_all_files_viewed: true,
  };
}

const ITEMS = [
  row({ id: "a4f9c1b2", title: "redline §3.2 indemnification — narrow carve-out", status: "open", source: "agent/redline/cr-1" }),
  row({ id: "b1234567", title: "extend cure period 15 → 30 days", status: "open", source: "agent/redline/cr-2" }),
  row({ id: "c89abcde", title: "claim-7714 evidence merged", status: "merged" }),
  row({ id: "d00ffeed", title: "rejected schema-rewrite proposal", status: "rejected", source: "schema-rewrite" }),
];

// ─────────────────────────────────────────────────────────────────────────────
// Filter type surface
// ─────────────────────────────────────────────────────────────────────────────

describe("ALL_FILTERS", () => {
  it("exposes the four supported filter keys in display order", () => {
    expect(ALL_FILTERS).toEqual(["all", "open", "merged", "rejected"]);
  });
});

describe("matchesFilter", () => {
  it("'all' matches every status", () => {
    for (const it of ITEMS) expect(matchesFilter(it, "all")).toBe(true);
  });

  it.each(["open", "merged", "rejected"] as const)("%s filter matches only that status", (f) => {
    for (const it of ITEMS) {
      expect(matchesFilter(it, f)).toBe(it.change_request.status === f);
    }
  });
});

describe("matchesQuery — corpus + whitespace", () => {
  const target = ITEMS[0]!;

  it("matches on title (case-insensitive)", () => {
    expect(matchesQuery(target, "INDEMNIFICATION")).toBe(true);
    expect(matchesQuery(target, "indemnification")).toBe(true);
  });

  it("matches on source ref", () => {
    expect(matchesQuery(target, "agent/redline")).toBe(true);
  });

  it("matches on target ref", () => {
    expect(matchesQuery(target, "main")).toBe(true);
  });

  it("matches on id prefix (first 8 chars)", () => {
    expect(matchesQuery(target, "a4f9c1b2")).toBe(true);
    expect(matchesQuery(target, "a4f9")).toBe(true);
  });

  it("returns true for empty / whitespace queries (don't blank the list while the user is typing)", () => {
    expect(matchesQuery(target, "")).toBe(true);
    expect(matchesQuery(target, "   ")).toBe(true);
  });

  it("returns false when no field contains the substring", () => {
    expect(matchesQuery(target, "zzzzz")).toBe(false);
  });
});

describe("filterAndSearch — combination", () => {
  it("preserves source order when no filter / no query are applied", () => {
    const result = filterAndSearch(ITEMS, "all", "");
    expect(result.map((r) => r.change_request.id)).toEqual(ITEMS.map((r) => r.change_request.id));
  });

  it("narrows by filter first, then query", () => {
    const result = filterAndSearch(ITEMS, "open", "cure period");
    expect(result).toHaveLength(1);
    expect(result[0]!.change_request.title).toMatch(/cure period/);
  });

  it("empty result when filter rules everything out", () => {
    expect(filterAndSearch([], "open", "")).toEqual([]);
    expect(filterAndSearch(ITEMS, "merged", "zzzzz")).toEqual([]);
  });
});

describe("countByFilter", () => {
  it("counts each status + total under 'all'", () => {
    const c = countByFilter(ITEMS);
    expect(c).toEqual({ all: 4, open: 2, merged: 1, rejected: 1 });
  });

  it("returns zeros for the empty list", () => {
    expect(countByFilter([])).toEqual({ all: 0, open: 0, merged: 0, rejected: 0 });
  });
});
