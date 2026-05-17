/**
 * ReviewsScreen — the daily-driver list of pending change requests.
 *
 * D1 (the API wire-up) shipped in 545bd09. D1.2 (this slice) adds
 * client-side filter chips + a search input. Both work on the data the
 * useChangeRequestList query already returned — backend slice 3 doesn't
 * yet take query params on GET /change-requests, so we prune in the
 * browser. The pure logic lives in `lib/api/reviews-filter.ts` so it's
 * trivially testable; the same predicates port to a server-side filter
 * the day backend adds the params.
 *
 * Filter state is component-local for now (`useState`). URL search
 * params are the next obvious upgrade (shareable, back-button-safe,
 * refresh-resilient) and queued as a follow-up — orthogonal to the
 * D1.2 ask.
 *
 * Five render states:
 *
 *   Loading                 3 skeleton cards.
 *   Error                   alert + Retry.
 *   No data at all          editorial "no change requests yet" card.
 *   No matches after filter distinct "no matches" card with a Clear
 *                           filters button — different intent from the
 *                           genuine empty state, surfaced as such.
 *   Populated               filtered card list.
 */

import type { ChangeRequestResponse } from "@stratum/sdk";
import { useDeferredValue, useId, useMemo, useState } from "react";
import { useChangeRequestList } from "../lib/api/reviews.ts";
import {
  ALL_FILTERS,
  countByFilter,
  filterAndSearch,
  type Filter,
} from "../lib/api/reviews-filter.ts";

// ─────────────────────────────────────────────────────────────────────────────
// Screen
// ─────────────────────────────────────────────────────────────────────────────

export function ReviewsScreen() {
  const { items, isLoading, isError, error, refetch } = useChangeRequestList();
  const [filter, setFilter] = useState<Filter>("all");
  const [query, setQuery] = useState("");

  // Defer the search query for the filter computation so a fast typist
  // doesn't block paint when the list grows. Counts use the raw items
  // (filter chips never lie about how many CRs exist).
  const deferredQuery = useDeferredValue(query);
  const filtered = useMemo(
    () => filterAndSearch(items, filter, deferredQuery),
    [items, filter, deferredQuery],
  );
  const counts = useMemo(() => countByFilter(items), [items]);

  const hasData = !isLoading && !isError && items.length > 0;
  const hasNoData = !isLoading && !isError && items.length === 0;
  const hasNoMatches = hasData && filtered.length === 0;

  return (
    <div className="mx-auto max-w-3xl px-8 py-10">
      <header className="mb-6">
        <div className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
          Phase D — the daily driver
        </div>
        <h1 className="mt-1 text-[22px] font-medium leading-tight tracking-tight text-stone-900">
          Reviews
        </h1>
      </header>

      {hasData && (
        <FilterToolbar
          filter={filter}
          counts={counts}
          query={query}
          onFilterChange={setFilter}
          onQueryChange={setQuery}
        />
      )}

      {isLoading && <LoadingState />}
      {isError && <ErrorState error={error} onRetry={refetch} />}
      {hasNoData && <EmptyState />}
      {hasNoMatches && (
        <NoMatchesState
          onClear={() => {
            setFilter("all");
            setQuery("");
          }}
        />
      )}
      {hasData && filtered.length > 0 && (
        <ul aria-label="Change requests" className="flex flex-col gap-2">
          {filtered.map((cr) => (
            <li key={cr.change_request.id}>
              <ChangeRequestCard item={cr} />
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Filter toolbar
// ─────────────────────────────────────────────────────────────────────────────

function FilterToolbar({
  filter,
  counts,
  query,
  onFilterChange,
  onQueryChange,
}: {
  readonly filter: Filter;
  readonly counts: { readonly all: number; readonly open: number; readonly merged: number; readonly rejected: number };
  readonly query: string;
  readonly onFilterChange: (next: Filter) => void;
  readonly onQueryChange: (next: string) => void;
}) {
  const searchId = useId();
  return (
    <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
      <div
        role="radiogroup"
        aria-label="Filter by status"
        className="inline-flex rounded-md bg-stone-100 p-0.5"
      >
        {ALL_FILTERS.map((f) => (
          <FilterChip
            key={f}
            filter={f}
            count={counts[f]}
            active={filter === f}
            onClick={() => onFilterChange(f)}
          />
        ))}
      </div>
      <label htmlFor={searchId} className="relative inline-flex items-center">
        <span className="sr-only">Search change requests</span>
        <svg
          aria-hidden
          width="13"
          height="13"
          viewBox="0 0 16 16"
          fill="none"
          className="pointer-events-none absolute left-2.5 text-stone-400"
        >
          <circle cx="7" cy="7" r="4.5" stroke="currentColor" strokeWidth="1.5" />
          <path
            d="M10.5 10.5 13.5 13.5"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
          />
        </svg>
        <input
          id={searchId}
          type="search"
          value={query}
          onChange={(e) => onQueryChange(e.currentTarget.value)}
          placeholder="Search by title, ref, or id…"
          autoComplete="off"
          spellCheck={false}
          className="w-64 rounded-md border border-stone-200 bg-white py-1.5 pl-7 pr-2 text-[12.5px] text-stone-900 outline-none transition placeholder:text-stone-400 focus:border-stone-400 focus:ring-2 focus:ring-stone-200"
        />
      </label>
    </div>
  );
}

function FilterChip({
  filter,
  count,
  active,
  onClick,
}: {
  readonly filter: Filter;
  readonly count: number;
  readonly active: boolean;
  readonly onClick: () => void;
}) {
  return (
    <button
      type="button"
      role="radio"
      aria-checked={active}
      onClick={onClick}
      className={`flex items-center gap-1.5 rounded-[5px] px-2.5 py-1 text-[12px] font-medium transition ${
        active
          ? "bg-white text-stone-900 shadow-sm ring-1 ring-stone-200"
          : "text-stone-600 hover:text-stone-900"
      }`}
    >
      <span className="capitalize">{filter}</span>
      <span
        className={`font-mono text-[10.5px] tabular-nums ${
          active ? "text-stone-500" : "text-stone-400"
        }`}
      >
        {count}
      </span>
    </button>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// States
// ─────────────────────────────────────────────────────────────────────────────

function LoadingState() {
  return (
    <ul aria-label="Loading change requests" aria-busy="true" className="flex flex-col gap-2">
      {[0, 1, 2].map((i) => (
        <li
          key={i}
          aria-hidden
          className="animate-pulse rounded-md border border-stone-200 bg-white p-4 shadow-sm"
        >
          <div className="mb-3 h-3 w-3/4 rounded bg-stone-200" />
          <div className="h-3 w-1/2 rounded bg-stone-100" />
        </li>
      ))}
    </ul>
  );
}

function EmptyState() {
  return (
    <div className="rounded-md border border-dashed border-stone-300 bg-white px-6 py-12 text-center shadow-sm">
      <h2 className="text-[15px] font-medium text-stone-900">No change requests yet.</h2>
      <p className="mx-auto mt-1 max-w-sm font-serif text-[14px] italic text-stone-500">
        When agents commit to a branch you review, they'll show up here with a diff, a rationale,
        and one-click approve.
      </p>
    </div>
  );
}

function NoMatchesState({ onClear }: { readonly onClear: () => void }) {
  return (
    <div className="rounded-md border border-stone-200 bg-white px-6 py-10 text-center shadow-sm">
      <h2 className="text-[15px] font-medium text-stone-900">No matches.</h2>
      <p className="mx-auto mt-1 max-w-sm font-serif text-[14px] italic text-stone-500">
        Nothing in this view matches your filter and search. Loosen one to see more.
      </p>
      <button
        type="button"
        onClick={onClear}
        className="mt-4 rounded-md border border-stone-300 bg-white px-3 py-1 text-[12px] font-medium text-stone-700 transition hover:border-stone-500 hover:text-stone-900"
      >
        Clear filters
      </button>
    </div>
  );
}

function ErrorState({ error, onRetry }: { readonly error: Error | null; readonly onRetry: () => void }) {
  return (
    <div role="alert" className="rounded-md border border-rose-200 bg-rose-50 px-5 py-4 shadow-sm">
      <div className="font-mono text-[10.5px] uppercase tracking-wider text-rose-700">
        Couldn't load change requests
      </div>
      <p className="mt-1 font-mono text-[12px] text-rose-800">{error?.message ?? "Unknown error."}</p>
      <button
        type="button"
        onClick={onRetry}
        className="mt-3 rounded-md border border-rose-300 bg-white px-3 py-1 text-[12px] font-medium text-rose-800 transition hover:border-rose-500 hover:bg-rose-50"
      >
        Retry
      </button>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Card
// ─────────────────────────────────────────────────────────────────────────────

function ChangeRequestCard({ item }: { readonly item: ChangeRequestResponse }) {
  const cr = item.change_request;
  const approval = item.approval_state;
  const agentish = isLikelyAgent(cr.created_by);

  return (
    <article
      className="rounded-md border border-stone-200 bg-white p-4 shadow-sm transition hover:border-stone-300"
      aria-labelledby={`cr-title-${cr.id}`}
    >
      <div className="flex items-start gap-3">
        <ActorMark agentish={agentish} />
        <div className="min-w-0 flex-1">
          <h3 id={`cr-title-${cr.id}`} className="text-[14px] font-medium leading-snug text-stone-900">
            {cr.title}
          </h3>
          <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 font-mono text-[11px] text-stone-500">
            <span>{cr.source_ref}</span>
            <span aria-hidden>→</span>
            <span>{cr.target_ref}</span>
            <span aria-hidden className="text-stone-300">·</span>
            <ApprovalSummary item={item} />
          </div>
        </div>
        <StatusBadge status={cr.status} approved={"approved" in approval && approval.approved} />
      </div>
    </article>
  );
}

function ApprovalSummary({ item }: { readonly item: ChangeRequestResponse }) {
  const a = item.approval_state;
  if (!("approved" in a)) {
    return <span className="text-amber-700">approval state unavailable</span>;
  }
  if (a.approved) {
    return (
      <span className="text-emerald-700">
        {a.approval_count} / {a.required_approvals} approved
      </span>
    );
  }
  if (a.required_approvals === 0) {
    return <span>no approvals required</span>;
  }
  const missing = a.required_approvals - a.approval_count;
  return (
    <span>
      {a.approval_count} / {a.required_approvals} approvals · {missing} pending
    </span>
  );
}

function StatusBadge({
  status,
  approved,
}: {
  readonly status: "open" | "merged" | "rejected";
  readonly approved: boolean;
}) {
  const { label, color } = badgeFor(status, approved);
  return (
    <span className={`shrink-0 rounded-md px-2 py-0.5 font-mono text-[10.5px] uppercase tracking-wider ${color}`}>
      {label}
    </span>
  );
}

function badgeFor(
  status: "open" | "merged" | "rejected",
  approved: boolean,
): { label: string; color: string } {
  if (status === "merged") return { label: "merged", color: "bg-emerald-100 text-emerald-800" };
  if (status === "rejected") return { label: "rejected", color: "bg-stone-200 text-stone-700" };
  if (approved) return { label: "ready", color: "bg-orange-100 text-orange-800" };
  return { label: "open", color: "bg-amber-100 text-amber-800" };
}

function ActorMark({ agentish }: { readonly agentish: boolean }) {
  if (agentish) {
    return (
      <span
        aria-hidden
        className="grid h-6 w-6 shrink-0 place-items-center rounded-full bg-orange-100 font-mono text-[9px] font-semibold text-orange-700"
        title="Agent-authored change request"
      >
        ag
      </span>
    );
  }
  return (
    <span
      aria-hidden
      className="grid h-6 w-6 shrink-0 place-items-center rounded-full bg-stone-200 font-mono text-[9px] font-semibold text-stone-700"
      title="Human-authored change request"
    >
      hu
    </span>
  );
}

/** Heuristic: agent UIDs are >= 100 by convention in seeded fixtures. The
 *  real check lands when backend ships a `principal_kind` field on the
 *  change request (queued for Slice 4 prep). For now this is a safe
 *  ish-default for the local-state fixtures. */
function isLikelyAgent(uid: number): boolean {
  return uid >= 100;
}
