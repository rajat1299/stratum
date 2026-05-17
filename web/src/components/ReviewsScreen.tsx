/**
 * ReviewsScreen — the daily-driver list of pending change requests.
 *
 * D1 slice. Reads from /change-requests via useChangeRequestList; renders
 * loading / error / empty / populated. Detail view + approve flow land in
 * D2–D6 — for now the cards are listed but not clickable to a detail
 * route (none exists yet).
 *
 * Surfaces three states with intent:
 *
 *   Loading   3 skeleton cards. We know roughly what the layout looks like
 *             so we render the rough shape, not a generic spinner. Reviewer
 *             gets a sense of pending content without scrolling jump.
 *
 *   Error     Single card with the error message and a Retry button. Uses
 *             the query's own refetch — no manual fetch plumbing.
 *
 *   Empty     Editorial: "No change requests yet." The reviewer's most
 *             common state (especially first paint). Worth designing for.
 *
 *   Populated A card per change request. Title + agent mark + source→target
 *             ref pair + approval-state summary + status badge.
 */

import type { ChangeRequestResponse } from "@stratum/sdk";
import { useChangeRequestList } from "../lib/api/reviews.ts";

// ─────────────────────────────────────────────────────────────────────────────
// Screen
// ─────────────────────────────────────────────────────────────────────────────

export function ReviewsScreen() {
  const { items, isLoading, isError, error, refetch } = useChangeRequestList();

  return (
    <div className="mx-auto max-w-3xl px-8 py-10">
      <header className="mb-6 flex items-end justify-between">
        <div>
          <div className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
            Phase D — the daily driver
          </div>
          <h1 className="mt-1 text-[22px] font-medium leading-tight tracking-tight text-stone-900">
            Reviews
          </h1>
        </div>
        {!isLoading && !isError && items.length > 0 && <ReviewsSummary items={items} />}
      </header>

      {isLoading && <LoadingState />}
      {isError && <ErrorState error={error} onRetry={refetch} />}
      {!isLoading && !isError && items.length === 0 && <EmptyState />}
      {!isLoading && !isError && items.length > 0 && (
        <ul aria-label="Change requests" className="flex flex-col gap-2">
          {items.map((cr) => (
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

  // Detail route lands in D2 — cards aren't clickable yet, so we don't
  // dress them as links and lie about the cursor.
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

// ─────────────────────────────────────────────────────────────────────────────
// Summary row
// ─────────────────────────────────────────────────────────────────────────────

function ReviewsSummary({ items }: { readonly items: readonly ChangeRequestResponse[] }) {
  const counts = items.reduce(
    (acc, it) => {
      acc[it.change_request.status]++;
      return acc;
    },
    { open: 0, merged: 0, rejected: 0 } as Record<"open" | "merged" | "rejected", number>,
  );
  return (
    <div className="font-mono text-[11px] tabular-nums text-stone-500">
      <span>
        <strong className="text-stone-800">{counts.open}</strong> open
      </span>
      <span aria-hidden> · </span>
      <span>{counts.merged} merged</span>
      <span aria-hidden> · </span>
      <span>{counts.rejected} rejected</span>
    </div>
  );
}
