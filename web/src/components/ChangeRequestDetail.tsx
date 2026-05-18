/**
 * ChangeRequestDetail — the per-CR review surface at /reviews/$id.
 *
 * D2.2 lands the layout + data wiring for everything that exists today:
 * title, byline, description, approval-state breakdown, and a clearly-
 * marked placeholder for the diff. The action row (Approve & merge /
 * Request changes / Reject) renders disabled — those mutations land in
 * D3 with idempotency-aware useMutation hooks.
 *
 * Diff display blocked on backend: GET /vcs/diff today accepts only
 * `?path=` (working-tree against HEAD). Rendering a CR-scoped diff needs
 * `?base=&head=` so we can compare cr.base_commit ↔ cr.head_commit.
 * Surfaced as a placeholder card with the commit hashes visible (so a
 * reviewer can still curl them by hand) and a one-line backend ask.
 *
 * Five render states:
 *
 *   Loading        skeletons matching the eventual layout
 *   404 (not found)  "CR not found" card + Back to Reviews
 *   403 (no access) "You don't have access" card
 *   Other error    alert + Retry
 *   Populated      full layout
 */

import type { ApprovalRecord, ChangeRequest, ChangeRequestResponse } from "@stratum/sdk";
import { useState } from "react";
import {
  useApprovals,
  useApproveChangeRequest,
  useChangeRequest,
  useDismissApproval,
  useMergeChangeRequest,
  useRejectChangeRequest,
} from "../lib/api/reviews.ts";
import { useCapabilities } from "../lib/capabilities.ts";

// ─────────────────────────────────────────────────────────────────────────────
// Screen
// ─────────────────────────────────────────────────────────────────────────────

export interface ChangeRequestDetailProps {
  readonly id: string;
  /** Called when the user clicks "Back to Reviews". Router passes a real
   *  navigator; tests pass a vi.fn. */
  readonly onBack: () => void;
}

export function ChangeRequestDetail({ id, onBack }: ChangeRequestDetailProps) {
  const query = useChangeRequest(id);
  const status = httpStatusFromError(query.error);

  return (
    <div className="mx-auto max-w-3xl px-8 py-10">
      <BackLink onClick={onBack} />

      {query.isLoading && <LoadingDetail />}
      {query.isError && status === 404 && <NotFoundCard id={id} onBack={onBack} />}
      {query.isError && status === 403 && <ForbiddenCard />}
      {query.isError && status !== 404 && status !== 403 && (
        <GenericErrorCard error={query.error} onRetry={() => query.refetch()} />
      )}
      {query.isSuccess && <PopulatedDetail item={query.data} />}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Populated layout
// ─────────────────────────────────────────────────────────────────────────────

function PopulatedDetail({ item }: { readonly item: ChangeRequestResponse }) {
  const cr = item.change_request;
  const approval = item.approval_state;
  const approved = "approved" in approval && approval.approved;

  return (
    <article aria-labelledby="cr-detail-title">
      <header className="mb-5 flex items-start gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 font-mono text-[11px] text-stone-500">
            <span>{cr.source_ref}</span>
            <span aria-hidden>→</span>
            <span>{cr.target_ref}</span>
            <span aria-hidden className="text-stone-300">·</span>
            <span className="truncate" title={cr.id}>
              {cr.id.slice(0, 8)}
            </span>
          </div>
          <h1
            id="cr-detail-title"
            className="mt-2 text-[24px] font-medium leading-snug tracking-tight text-stone-900"
          >
            {cr.title}
          </h1>
        </div>
        <StatusBadges status={cr.status} approved={approved} />
      </header>

      <ActionRow id={cr.id} status={cr.status} approved={approved} />

      {cr.description !== null && cr.description.length > 0 && (
        <section aria-labelledby="cr-detail-desc" className="mt-8">
          <h2
            id="cr-detail-desc"
            className="mb-2 font-mono text-[10.5px] uppercase tracking-wider text-stone-500"
          >
            Description
          </h2>
          <p className="whitespace-pre-wrap text-[14px] leading-relaxed text-stone-800">
            {cr.description}
          </p>
        </section>
      )}

      <ApprovalDetail item={item} />

      <DiffPlaceholder cr={cr} />
    </article>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Action row (D3 wires the mutations)
// ─────────────────────────────────────────────────────────────────────────────

function ActionRow({
  id,
  status,
  approved,
}: {
  readonly id: string;
  readonly status: "open" | "merged" | "rejected";
  readonly approved: boolean;
}) {
  const isTerminal = status !== "open";
  const approve = useApproveChangeRequest();
  const reject = useRejectChangeRequest();
  const merge = useMergeChangeRequest();
  const capabilities = useCapabilities();

  // Merge gating — driven by the manifest's default for now.
  //
  // TODO(coordination): when backend ships the resolved per-CR
  // `require_all_files_viewed: bool` on GET /change-requests/:id (see
  // .worktrees/v2-foundation/docs/plans/2026-05-17-pre-slice45-review-
  // contract-coordination.md), swap this read for the CR-scoped value.
  // The manifest default is correct policy posture for protected refs;
  // the per-CR value will let path-rule overrides surface accurately.
  const requireAllViewed =
    capabilities.data?.protection.ref_rules.require_all_files_viewed_default ?? true;
  // The detail screen doesn't yet track per-file viewed state (that
  // lives in the C4 spike). When the protection rule requires it, we
  // gate merge with an explanatory tooltip rather than offer a button
  // the policy will reject.
  const mergeBlockedByViewing = approved && requireAllViewed;
  const canMerge = approved && !mergeBlockedByViewing && !isTerminal;

  const anyPending = approve.isPending || reject.isPending || merge.isPending;
  const firstError = approve.error ?? reject.error ?? merge.error;

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-2">
        <button
          type="button"
          onClick={() => approve.mutate({ id })}
          disabled={isTerminal || anyPending}
          title={isTerminal ? `This CR is ${status} — actions are read-only.` : undefined}
          className="rounded-md border border-stone-300 bg-stone-900 px-3 py-1.5 text-[13px] font-medium text-stone-50 transition enabled:hover:bg-stone-700 disabled:cursor-not-allowed disabled:opacity-40"
        >
          {approve.isPending ? "Approving…" : approved ? "Approve (recorded)" : "Approve"}
        </button>
        <button
          type="button"
          onClick={() => merge.mutate({ id })}
          disabled={!canMerge || anyPending}
          title={
            isTerminal
              ? `This CR is ${status} — actions are read-only.`
              : !approved
                ? "Merge unlocks once approval requirements are satisfied."
                : mergeBlockedByViewing
                  ? "Merge gated: the protected rule requires all files to be viewed. Viewed-file tracking on the detail screen ships with the diff display (waiting on GET /vcs/diff base+head params)."
                  : undefined
          }
          className="rounded-md border border-orange-300 bg-orange-500 px-3 py-1.5 text-[13px] font-medium text-white transition enabled:hover:bg-orange-600 disabled:cursor-not-allowed disabled:border-stone-300 disabled:bg-stone-300 disabled:text-stone-50 disabled:opacity-60"
        >
          {merge.isPending ? "Merging…" : "Merge"}
        </button>
        <button
          type="button"
          disabled
          title="Review comments (including 'Request changes') ship in slice D4."
          className="rounded-md border border-stone-300 px-3 py-1.5 text-[13px] font-medium text-stone-700 transition disabled:cursor-not-allowed disabled:opacity-40"
        >
          Request changes
          <span className="ml-1.5 font-mono text-[9.5px] uppercase tracking-wider text-stone-500">
            D4
          </span>
        </button>
        <button
          type="button"
          onClick={() => reject.mutate({ id })}
          disabled={isTerminal || anyPending}
          title={isTerminal ? `This CR is ${status} — actions are read-only.` : undefined}
          className="rounded-md border border-stone-300 px-3 py-1.5 text-[13px] font-medium text-stone-700 transition enabled:hover:border-rose-400 enabled:hover:text-rose-700 disabled:cursor-not-allowed disabled:opacity-40"
        >
          {reject.isPending ? "Rejecting…" : "Reject"}
        </button>
      </div>

      {firstError && <ActionError error={firstError} />}
    </div>
  );
}

function ActionError({ error }: { readonly error: Error }) {
  return (
    <p
      role="alert"
      className="rounded-md border border-rose-200 bg-rose-50 px-3 py-2 font-mono text-[11.5px] text-rose-800"
    >
      {error.message}
    </p>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Approval state detail
// ─────────────────────────────────────────────────────────────────────────────

function ApprovalDetail({ item }: { readonly item: ChangeRequestResponse }) {
  const a = item.approval_state;
  if (!("approved" in a)) {
    return (
      <section aria-labelledby="cr-detail-approval" className="mt-8">
        <h2
          id="cr-detail-approval"
          className="mb-2 font-mono text-[10.5px] uppercase tracking-wider text-stone-500"
        >
          Approval state
        </h2>
        <p className="rounded-md border border-amber-200 bg-amber-50 px-4 py-3 text-[13px] text-amber-900">
          Approval state unavailable from the server. ({a.error})
        </p>
      </section>
    );
  }

  const missing = Math.max(a.required_approvals - a.approval_count, 0);
  return (
    <section aria-labelledby="cr-detail-approval" className="mt-8">
      <h2
        id="cr-detail-approval"
        className="mb-2 font-mono text-[10.5px] uppercase tracking-wider text-stone-500"
      >
        Approval state
      </h2>
      <div className="overflow-hidden rounded-md border border-stone-200 bg-white shadow-sm">
        <dl className="divide-y divide-stone-100">
          <Row k="Status">
            {a.approved ? (
              <span className="text-emerald-700">approved</span>
            ) : a.required_approvals === 0 ? (
              <span>no approvals required</span>
            ) : (
              <span>
                {a.approval_count} / {a.required_approvals} approvals · {missing} pending
              </span>
            )}
          </Row>
          <Row k="Approved by">
            {a.approved_by.length === 0 ? (
              <span className="text-stone-400">—</span>
            ) : (
              <span className="font-mono">{a.approved_by.map((u) => `uid:${u}`).join(", ")}</span>
            )}
          </Row>
          {a.required_reviewers.length > 0 && (
            <Row k="Required reviewers">
              <span className="font-mono">
                {a.required_reviewers
                  .map((u) => `uid:${u}${a.approved_required_reviewers.includes(u) ? " ✓" : ""}`)
                  .join(", ")}
              </span>
            </Row>
          )}
          {a.missing_required_reviewers.length > 0 && (
            <Row k="Missing reviewers">
              <span className="font-mono text-amber-700">
                {a.missing_required_reviewers.map((u) => `uid:${u}`).join(", ")}
              </span>
            </Row>
          )}
          {a.matched_ref_rules.length > 0 && (
            <Row k="Matched ref rules">
              <span className="font-mono">{a.matched_ref_rules.join(", ")}</span>
            </Row>
          )}
          {a.matched_path_rules.length > 0 && (
            <Row k="Matched path rules">
              <span className="font-mono">{a.matched_path_rules.join(", ")}</span>
            </Row>
          )}
        </dl>
      </div>

      <ApprovalsList crId={item.change_request.id} />
    </section>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Approvals list + inline dismiss
// ─────────────────────────────────────────────────────────────────────────────

function ApprovalsList({ crId }: { readonly crId: string }) {
  const q = useApprovals(crId);
  // Don't render anything for the most common case (no approvals yet on
  // an open CR) — saves vertical space and avoids an empty card.
  if (q.isSuccess && q.data.approvals.length === 0) return null;

  return (
    <div className="mt-3">
      <div className="mb-1.5 ml-1 font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
        Approvals
      </div>
      {q.isLoading && (
        <div
          aria-busy="true"
          aria-label="Loading approvals"
          className="h-[34px] animate-pulse rounded-md border border-stone-200 bg-stone-50"
        />
      )}
      {q.isError && (
        <p
          role="alert"
          className="rounded-md border border-rose-200 bg-rose-50 px-3 py-2 font-mono text-[11.5px] text-rose-800"
        >
          Couldn't load approvals: {q.error?.message ?? "unknown error"}
        </p>
      )}
      {q.isSuccess && q.data.approvals.length > 0 && (
        <ul className="overflow-hidden rounded-md border border-stone-200 bg-white shadow-sm">
          {q.data.approvals.map((approval) => (
            <li
              key={approval.id}
              className="border-b border-stone-100 last:border-b-0"
            >
              <ApprovalRow approval={approval} crId={crId} />
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function ApprovalRow({
  approval,
  crId,
}: {
  readonly approval: ApprovalRecord;
  readonly crId: string;
}) {
  const dismiss = useDismissApproval();
  const [showForm, setShowForm] = useState(false);
  const [reason, setReason] = useState("");

  if (!approval.active) {
    // Inactive — render the historical trail. No actions.
    return (
      <div className="grid grid-cols-[60px_1fr_auto] items-center gap-3 px-4 py-2 text-[12.5px]">
        <span aria-hidden className="text-stone-400">
          ✗
        </span>
        <div className="min-w-0">
          <div className="font-mono text-stone-500 line-through">
            uid:{approval.approved_by}
            {approval.comment ? ` — "${approval.comment}"` : ""}
          </div>
          <div className="font-mono text-[11px] text-stone-500">
            dismissed by uid:{approval.dismissed_by ?? "?"}
            {approval.dismissal_reason ? ` · "${approval.dismissal_reason}"` : ""}
          </div>
        </div>
        <span className="font-mono text-[10px] uppercase tracking-wider text-stone-400">
          dismissed
        </span>
      </div>
    );
  }

  return (
    <div className="px-4 py-2">
      <div className="grid grid-cols-[60px_1fr_auto] items-center gap-3 text-[12.5px]">
        <span aria-hidden className="text-emerald-600">
          ✓
        </span>
        <div className="min-w-0 font-mono text-stone-800">
          uid:{approval.approved_by}
          {approval.comment ? (
            <span className="text-stone-500"> — "{approval.comment}"</span>
          ) : null}
        </div>
        {!showForm && (
          <button
            type="button"
            onClick={() => setShowForm(true)}
            disabled={dismiss.isPending}
            className="rounded-md border border-stone-300 px-2 py-0.5 font-mono text-[11px] text-stone-600 transition enabled:hover:border-rose-400 enabled:hover:text-rose-700 disabled:cursor-not-allowed disabled:opacity-40"
          >
            Dismiss
          </button>
        )}
      </div>

      {showForm && (
        <form
          onSubmit={(e) => {
            e.preventDefault();
            dismiss.mutate(
              { id: crId, approvalId: approval.id, ...(reason.trim() ? { reason: reason.trim() } : {}) },
              {
                onSuccess: () => {
                  setShowForm(false);
                  setReason("");
                },
              },
            );
          }}
          className="mt-2 flex flex-wrap items-center gap-2"
        >
          <input
            type="text"
            autoFocus
            value={reason}
            onChange={(e) => setReason(e.currentTarget.value)}
            placeholder="Reason (optional, e.g. 'stale head')"
            aria-label="Dismissal reason"
            maxLength={280}
            disabled={dismiss.isPending}
            className="flex-1 rounded-md border border-stone-300 px-2 py-1 font-mono text-[11.5px] text-stone-900 outline-none transition focus:border-stone-500 focus:ring-2 focus:ring-stone-200 disabled:opacity-50"
          />
          <button
            type="submit"
            disabled={dismiss.isPending}
            className="rounded-md border border-rose-300 bg-rose-50 px-2 py-1 font-mono text-[11.5px] font-medium text-rose-800 transition enabled:hover:border-rose-500 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {dismiss.isPending ? "Dismissing…" : "Confirm dismiss"}
          </button>
          <button
            type="button"
            onClick={() => {
              setShowForm(false);
              setReason("");
              dismiss.reset();
            }}
            disabled={dismiss.isPending}
            className="rounded-md border border-stone-300 px-2 py-1 font-mono text-[11.5px] text-stone-600 transition enabled:hover:border-stone-500 enabled:hover:text-stone-900 disabled:cursor-not-allowed disabled:opacity-50"
          >
            Cancel
          </button>
        </form>
      )}

      {dismiss.error && (
        <p
          role="alert"
          className="mt-2 rounded-md border border-rose-200 bg-rose-50 px-2 py-1 font-mono text-[11px] text-rose-800"
        >
          {dismiss.error.message}
        </p>
      )}
    </div>
  );
}

function Row({ k, children }: { readonly k: string; readonly children: React.ReactNode }) {
  return (
    <div className="grid grid-cols-[160px_1fr] gap-4 px-4 py-2 text-[13px]">
      <dt className="text-stone-500">{k}</dt>
      <dd className="text-stone-800">{children}</dd>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Diff placeholder
// ─────────────────────────────────────────────────────────────────────────────

function DiffPlaceholder({ cr }: { readonly cr: ChangeRequest }) {
  return (
    <section aria-labelledby="cr-detail-diff" className="mt-8">
      <h2
        id="cr-detail-diff"
        className="mb-2 font-mono text-[10.5px] uppercase tracking-wider text-stone-500"
      >
        Diff
      </h2>
      <div className="rounded-md border border-dashed border-stone-300 bg-stone-50 px-5 py-4">
        <p className="text-[13.5px] font-medium text-stone-700">
          Diff display unblocks when <code className="font-mono text-[12px]">GET /vcs/diff</code>{" "}
          accepts <code className="font-mono text-[12px]">?base=&amp;head=</code> query params.
        </p>
        <p className="mt-1 font-serif text-[13px] italic text-stone-500">
          Today the route only takes <code className="not-italic font-mono text-[12px]">?path=</code>{" "}
          (working tree vs HEAD). The renderer (web/src/components/DiffViewer.tsx) is ready — flip
          the fetch when the param ships.
        </p>
        <dl className="mt-4 grid grid-cols-[80px_1fr] gap-y-1 text-[12px]">
          <dt className="text-stone-500">base</dt>
          <dd className="truncate font-mono text-stone-800" title={cr.base_commit}>
            {cr.base_commit}
          </dd>
          <dt className="text-stone-500">head</dt>
          <dd className="truncate font-mono text-stone-800" title={cr.head_commit}>
            {cr.head_commit}
          </dd>
        </dl>
      </div>
    </section>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// State variants
// ─────────────────────────────────────────────────────────────────────────────

function LoadingDetail() {
  return (
    <div aria-busy="true" aria-label="Loading change request" className="animate-pulse">
      <div className="mb-3 h-3 w-1/3 rounded bg-stone-200" />
      <div className="mb-6 h-6 w-3/4 rounded bg-stone-200" />
      <div className="mb-2 h-3 w-1/2 rounded bg-stone-100" />
      <div className="h-3 w-2/3 rounded bg-stone-100" />
    </div>
  );
}

function NotFoundCard({ id, onBack }: { readonly id: string; readonly onBack: () => void }) {
  return (
    <div className="rounded-md border border-stone-200 bg-white px-6 py-10 text-center shadow-sm">
      <h2 className="text-[16px] font-medium text-stone-900">Change request not found.</h2>
      <p className="mx-auto mt-1 max-w-sm font-serif text-[14px] italic text-stone-500">
        No CR with id <code className="not-italic font-mono text-[12px]">{id}</code> visible to your
        account. It may have been deleted, archived, or it never existed.
      </p>
      <button
        type="button"
        onClick={onBack}
        className="mt-4 rounded-md border border-stone-300 bg-white px-3 py-1.5 text-[12.5px] font-medium text-stone-700 transition hover:border-stone-500 hover:text-stone-900"
      >
        Return to Reviews
      </button>
    </div>
  );
}

function ForbiddenCard() {
  return (
    <div role="alert" className="rounded-md border border-amber-200 bg-amber-50 px-5 py-4">
      <h2 className="text-[14px] font-medium text-amber-900">You don't have access to this CR.</h2>
      <p className="mt-1 font-serif text-[13px] italic text-amber-700">
        The backend rejected the request as forbidden. If you should have access, an admin can
        adjust your group memberships.
      </p>
    </div>
  );
}

function GenericErrorCard({
  error,
  onRetry,
}: {
  readonly error: Error | null;
  readonly onRetry: () => void;
}) {
  return (
    <div role="alert" className="rounded-md border border-rose-200 bg-rose-50 px-5 py-4 shadow-sm">
      <div className="font-mono text-[10.5px] uppercase tracking-wider text-rose-700">
        Couldn't load this change request
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
// Atoms
// ─────────────────────────────────────────────────────────────────────────────

function BackLink({ onClick }: { readonly onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="mb-6 inline-flex items-center gap-1.5 rounded-sm text-[12.5px] text-stone-500 transition hover:text-stone-900"
    >
      <span aria-hidden>←</span> Back to Reviews
    </button>
  );
}

function StatusBadges({
  status,
  approved,
}: {
  readonly status: "open" | "merged" | "rejected";
  readonly approved: boolean;
}) {
  const badges: ReadonlyArray<{ label: string; color: string }> = (() => {
    if (status === "merged") return [{ label: "merged", color: "bg-emerald-100 text-emerald-800" }];
    if (status === "rejected") return [{ label: "rejected", color: "bg-stone-200 text-stone-700" }];
    if (approved)
      return [
        { label: "open", color: "bg-amber-100 text-amber-800" },
        { label: "ready", color: "bg-orange-100 text-orange-800" },
      ];
    return [{ label: "open", color: "bg-amber-100 text-amber-800" }];
  })();
  return (
    <div className="flex shrink-0 gap-1">
      {badges.map((b) => (
        <span
          key={b.label}
          className={`rounded-md px-2 py-0.5 font-mono text-[10.5px] uppercase tracking-wider ${b.color}`}
        >
          {b.label}
        </span>
      ))}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/** Best-effort: pull the HTTP status off the SDK's HttpError. */
function httpStatusFromError(error: unknown): number | null {
  if (!error || typeof error !== "object") return null;
  const status = (error as { status?: unknown }).status;
  return typeof status === "number" ? status : null;
}
