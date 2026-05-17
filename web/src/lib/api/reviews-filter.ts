/**
 * reviews-filter — pure filter + search over change-request rows.
 *
 * Kept out of the React component so:
 *   - the rules (which fields a search matches, what "open" means)
 *     are one tested file rather than scattered through JSX,
 *   - the component can stay focused on layout + state plumbing,
 *   - the same predicates can drive a future server-side filter once
 *     backend ships GET /change-requests?status=…&q=… params.
 *
 * Phase D1.2 today is purely client-side: we receive the full list
 * from the API and prune in the browser. That's fine while the API
 * returns the bounded recent set; we revisit when pagination or
 * server-side query params land.
 */

import type { ChangeRequestResponse } from "@stratum/sdk";

export type Filter = "all" | "open" | "merged" | "rejected";

export const ALL_FILTERS: readonly Filter[] = ["all", "open", "merged", "rejected"] as const;

export interface FilterCounts {
  readonly all: number;
  readonly open: number;
  readonly merged: number;
  readonly rejected: number;
}

/** Match this filter against a CR row. */
export function matchesFilter(item: ChangeRequestResponse, filter: Filter): boolean {
  if (filter === "all") return true;
  return item.change_request.status === filter;
}

/**
 * Case-insensitive substring search across the user-facing fields a
 * reviewer is likely to recall:
 *
 *   - title              the headline they'd skim for
 *   - description        when present (currently always null in v1)
 *   - source_ref         "loi-redline-04"
 *   - target_ref         "main"
 *   - id prefix          first 8 chars — feels git-like
 *
 * Whitespace-only queries match everything (so the visible list doesn't
 * empty out the moment the reviewer's cursor lands in the field).
 */
export function matchesQuery(item: ChangeRequestResponse, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (q.length === 0) return true;
  const cr = item.change_request;
  const corpus = [
    cr.title,
    cr.description ?? "",
    cr.source_ref,
    cr.target_ref,
    cr.id.slice(0, 8),
  ]
    .join(" ")
    .toLowerCase();
  return corpus.includes(q);
}

/** Combined filter + search; preserves source order. */
export function filterAndSearch(
  items: readonly ChangeRequestResponse[],
  filter: Filter,
  query: string,
): readonly ChangeRequestResponse[] {
  return items.filter((it) => matchesFilter(it, filter) && matchesQuery(it, query));
}

/** Per-filter counts over the unfiltered set. Drives the chip badges. */
export function countByFilter(items: readonly ChangeRequestResponse[]): FilterCounts {
  const counts: FilterCounts = { all: items.length, open: 0, merged: 0, rejected: 0 };
  // Build a mutable mirror so the public type stays readonly.
  const tally = { ...counts };
  for (const it of items) {
    if (it.change_request.status === "open") tally.open++;
    else if (it.change_request.status === "merged") tally.merged++;
    else if (it.change_request.status === "rejected") tally.rejected++;
  }
  return tally;
}
