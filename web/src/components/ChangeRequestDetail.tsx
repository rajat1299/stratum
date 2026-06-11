/**
 * ChangeRequestDetail — the per-CR review surface at /reviews/$id.
 *
 * Five render states:
 *
 *   Loading        skeletons matching the eventual layout
 *   404 (not found)  "CR not found" card + Back to Reviews
 *   403 (no access) "You don't have access" card
 *   Other error    alert + Retry
 *   Populated      full layout
 */

import type {
  ApprovalRecord,
  ChangeRequest,
  ChangeRequestResponse,
  ReviewComment,
  ReviewerAssignment,
  StratumRevertResult,
  ViewedFilesResponse,
} from "@stratum/sdk";
import type { UseQueryResult } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { DiffFragmentBody } from "./DiffViewer.tsx";
import {
  useApprovals,
  useApproveChangeRequest,
  useChangeRequest,
  useChangeRequestDiff,
  useComments,
  useCreateComment,
  useDismissApproval,
  useMergeChangeRequest,
  useRejectChangeRequest,
  useAssignReviewer,
  useReviewers,
  useRevertChangeRequest,
  useSetViewedFile,
  useViewedFiles,
} from "../lib/api/reviews.ts";
import {
  fragmentTotals,
  parseDiff,
  summariseFragmentKind,
  type DiffFragment,
} from "../lib/diff-parser.ts";
import { formatReviewActor, formatReviewActorList } from "../lib/review-actors.ts";

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
  const viewedFiles = useViewedFiles(cr.id);
  const [commentComposer, setCommentComposer] = useState<CommentComposerState>({
    open: false,
    kind: "general",
  });

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

      <ActionRow
        item={item}
        viewedFiles={viewedFiles}
        onRequestChanges={() => setCommentComposer({ open: true, kind: "changes_requested" })}
      />

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

      <ReviewersPanel crId={cr.id} isReadOnly={cr.status !== "open"} />

      <CommentsThread
        crId={cr.id}
        composer={commentComposer}
        isReadOnly={cr.status !== "open"}
        onComposerChange={setCommentComposer}
      />

      <DiffSection cr={cr} viewedFiles={viewedFiles} />
    </article>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Action row (D3 wires the mutations)
// ─────────────────────────────────────────────────────────────────────────────

function ActionRow({
  item,
  viewedFiles,
  onRequestChanges,
}: {
  readonly item: ChangeRequestResponse;
  readonly viewedFiles: UseQueryResult<ViewedFilesResponse, Error>;
  readonly onRequestChanges: () => void;
}) {
  const cr = item.change_request;
  const approval = item.approval_state;
  const id = cr.id;
  const status = cr.status;
  const approved = "approved" in approval && approval.approved;
  const isTerminal = status !== "open";
  const approve = useApproveChangeRequest();
  const reject = useRejectChangeRequest();
  const merge = useMergeChangeRequest();
  const revert = useRevertChangeRequest();
  const [showRevertConfirm, setShowRevertConfirm] = useState(false);
  const [showMergeConfirm, setShowMergeConfirm] = useState(false);
  const [showRejectConfirm, setShowRejectConfirm] = useState(false);

  const fileGate = fileReviewGate(item, viewedFiles);
  const canMerge = approved && !isTerminal && !fileGate.blocksMerge;
  const mergeBlockedReason =
    mergeBlockReason({ approval, approved }) ?? fileGate.reason;

  const anyPending = approve.isPending || reject.isPending || merge.isPending || revert.isPending;
  const firstError = approve.error ?? reject.error ?? merge.error ?? revert.error;
  const firstErrorAction = firstError === revert.error ? "revert" : "action";

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
          onClick={() => {
            merge.reset();
            setShowRejectConfirm(false);
            if (canMerge) setShowMergeConfirm(true);
          }}
          disabled={!canMerge || anyPending}
          title={
            isTerminal
              ? `This CR is ${status} — actions are read-only.`
              : mergeBlockedReason
          }
          className="rounded-md border border-orange-300 bg-orange-500 px-3 py-1.5 text-[13px] font-medium text-white transition enabled:hover:bg-orange-600 disabled:cursor-not-allowed disabled:border-stone-300 disabled:bg-stone-300 disabled:text-stone-50 disabled:opacity-60"
        >
          {merge.isPending ? "Merging…" : "Merge"}
        </button>
        <button
          type="button"
          onClick={onRequestChanges}
          disabled={isTerminal || anyPending}
          title={isTerminal ? `This CR is ${status} — actions are read-only.` : undefined}
          className="rounded-md border border-stone-300 px-3 py-1.5 text-[13px] font-medium text-stone-700 transition enabled:hover:border-stone-500 enabled:hover:text-stone-900 disabled:cursor-not-allowed disabled:opacity-40"
        >
          Request changes
        </button>
        <button
          type="button"
          onClick={() => {
            reject.reset();
            setShowMergeConfirm(false);
            if (!isTerminal) setShowRejectConfirm(true);
          }}
          disabled={isTerminal || anyPending}
          title={isTerminal ? `This CR is ${status} — actions are read-only.` : undefined}
          className="rounded-md border border-stone-300 px-3 py-1.5 text-[13px] font-medium text-stone-700 transition enabled:hover:border-rose-400 enabled:hover:text-rose-700 disabled:cursor-not-allowed disabled:opacity-40"
        >
          {reject.isPending ? "Rejecting…" : "Reject"}
        </button>
        {status === "merged" && (
          <button
            type="button"
            onClick={() => {
              revert.reset();
              setShowRevertConfirm(true);
            }}
            disabled={revert.isPending}
            className="rounded-md border border-stone-300 px-3 py-1.5 text-[13px] font-medium text-stone-700 transition enabled:hover:border-rose-400 enabled:hover:text-rose-700 disabled:cursor-not-allowed disabled:opacity-40"
          >
            Revert
          </button>
        )}
      </div>

      {!isTerminal && !showMergeConfirm && !showRejectConfirm && (
        <p className="max-w-2xl text-[12.5px] leading-relaxed text-stone-500">
          {canMerge ? (
            <>
              This will advance {cr.target_ref} from {shortHash(cr.base_commit)} to{" "}
              {shortHash(cr.head_commit)}. {cr.source_ref} must still point to{" "}
              {shortHash(cr.head_commit)}.
            </>
          ) : (
            mergeBlockedReason
          )}
        </p>
      )}

      {showMergeConfirm && !isTerminal && (
        <div className="max-w-2xl rounded-md border border-orange-200 bg-orange-50 px-3 py-3">
          <p className="text-[13px] leading-relaxed text-orange-950">
            Merge {cr.source_ref} into {cr.target_ref}. This will advance {cr.target_ref} from{" "}
            {shortHash(cr.base_commit)} to {shortHash(cr.head_commit)}.
          </p>
          <div className="mt-2 flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() =>
                merge.mutate(
                  { id },
                  {
                    onSuccess: () => setShowMergeConfirm(false),
                  },
                )
              }
              disabled={merge.isPending}
              className="rounded-md border border-orange-700 bg-orange-700 px-3 py-1.5 text-[12.5px] font-medium text-white transition enabled:hover:bg-orange-800 disabled:cursor-not-allowed disabled:opacity-40"
            >
              {merge.isPending ? "Merging…" : "Confirm merge"}
            </button>
            <button
              type="button"
              onClick={() => {
                merge.reset();
                setShowMergeConfirm(false);
              }}
              disabled={merge.isPending}
              className="rounded-md border border-orange-300 bg-white px-3 py-1.5 text-[12.5px] font-medium text-orange-900 transition enabled:hover:border-orange-500 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Cancel
            </button>
          </div>
        </div>
      )}

      {showRejectConfirm && !isTerminal && (
        <div className="max-w-2xl rounded-md border border-rose-200 bg-rose-50 px-3 py-3">
          <p className="text-[13px] leading-relaxed text-rose-900">
            Reject this change request. It will be closed without advancing {cr.target_ref}.
          </p>
          <div className="mt-2 flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() =>
                reject.mutate(
                  { id },
                  {
                    onSuccess: () => setShowRejectConfirm(false),
                  },
                )
              }
              disabled={reject.isPending}
              className="rounded-md border border-rose-700 bg-rose-700 px-3 py-1.5 text-[12.5px] font-medium text-white transition enabled:hover:bg-rose-800 disabled:cursor-not-allowed disabled:opacity-40"
            >
              {reject.isPending ? "Rejecting…" : "Confirm reject"}
            </button>
            <button
              type="button"
              onClick={() => {
                reject.reset();
                setShowRejectConfirm(false);
              }}
              disabled={reject.isPending}
              className="rounded-md border border-rose-300 bg-white px-3 py-1.5 text-[12.5px] font-medium text-rose-800 transition enabled:hover:border-rose-500 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Cancel
            </button>
          </div>
        </div>
      )}

      {showRevertConfirm && status === "merged" && (
        <div className="max-w-2xl rounded-md border border-rose-200 bg-rose-50 px-3 py-3">
          <p className="text-[13px] leading-relaxed text-rose-900">
            Return {cr.target_ref} to {shortHash(cr.base_commit)}. The merged changes from{" "}
            {shortHash(cr.head_commit)} will no longer be current.
          </p>
          <div className="mt-2 flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() =>
                revert.mutate(
                  { id, hash: cr.base_commit },
                  {
                    onSuccess: () => setShowRevertConfirm(false),
                  },
                )
              }
              disabled={revert.isPending}
              className="rounded-md border border-rose-700 bg-rose-700 px-3 py-1.5 text-[12.5px] font-medium text-white transition enabled:hover:bg-rose-800 disabled:cursor-not-allowed disabled:opacity-40"
            >
              {revert.isPending ? "Reverting..." : "Confirm revert"}
            </button>
            <button
              type="button"
              onClick={() => {
                revert.reset();
                setShowRevertConfirm(false);
              }}
              disabled={revert.isPending}
              className="rounded-md border border-rose-300 bg-white px-3 py-1.5 text-[12.5px] font-medium text-rose-800 transition enabled:hover:border-rose-500 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Cancel
            </button>
          </div>
        </div>
      )}

      {revert.data && <RevertEvidence result={revert.data} targetRef={cr.target_ref} />}

      {firstError && <ActionError error={firstError} action={firstErrorAction} />}
    </div>
  );
}

function mergeBlockReason({
  approval,
  approved,
}: {
  readonly approval: ChangeRequestResponse["approval_state"];
  readonly approved: boolean;
}): string | undefined {
  if (approved) return undefined;
  if (!("approved" in approval)) return "Approval status is unavailable.";

  if (approval.missing_required_reviewers.length > 0) {
    const names = formatReviewActorList(approval.missing_required_reviewers);
    return `Waiting for ${names}.`;
  }

  const missing = Math.max(approval.required_approvals - approval.approval_count, 0);
  if (missing > 0) return `Waiting for ${missing} approval${missing === 1 ? "" : "s"}.`;

  return "Waiting for approval.";
}

function fileReviewGate(
  item: ChangeRequestResponse,
  viewedFiles: UseQueryResult<ViewedFilesResponse, Error>,
): {
  readonly blocksMerge: boolean;
  readonly reason?: string;
  readonly allViewed: boolean;
  readonly unviewedCount: number;
} {
  const requiresViewing = (() => {
    const approval = item.approval_state;
    if ("approved" in approval) return approval.require_all_files_viewed;
    return item.require_all_files_viewed;
  })();

  if (!requiresViewing) {
    return { blocksMerge: false, allViewed: true, unviewedCount: 0 };
  }

  if (viewedFiles.isLoading) {
    return {
      blocksMerge: true,
      reason: "Loading file review state.",
      allViewed: false,
      unviewedCount: 0,
    };
  }

  if (viewedFiles.isError) {
    return {
      blocksMerge: true,
      reason: "Couldn't load file review state.",
      allViewed: false,
      unviewedCount: 0,
    };
  }

  const unviewedCount = viewedFiles.data?.unviewed_paths.length ?? 0;
  if (unviewedCount > 0) {
    const fileWord = unviewedCount === 1 ? "file" : "files";
    return {
      blocksMerge: true,
      reason: `Review ${unviewedCount} remaining ${fileWord} before merging.`,
      allViewed: false,
      unviewedCount,
    };
  }

  return { blocksMerge: false, allViewed: true, unviewedCount: 0 };
}

// ─────────────────────────────────────────────────────────────────────────────
// Comments thread + request-changes composer
// ─────────────────────────────────────────────────────────────────────────────

type CommentKind = "general" | "changes_requested";

interface CommentComposerState {
  readonly open: boolean;
  readonly kind: CommentKind;
}

function CommentsThread({
  crId,
  composer,
  isReadOnly,
  onComposerChange,
}: {
  readonly crId: string;
  readonly composer: CommentComposerState;
  readonly isReadOnly: boolean;
  readonly onComposerChange: (state: CommentComposerState) => void;
}) {
  const q = useComments(crId);
  const create = useCreateComment();
  const [body, setBody] = useState("");

  const trimmedBody = body.trim();

  return (
    <section aria-labelledby="cr-detail-comments" className="mt-8">
      <div className="mb-2 flex items-center justify-between gap-3">
        <h2
          id="cr-detail-comments"
          className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500"
        >
          Comments
        </h2>
        {!isReadOnly && (
          <button
            type="button"
            onClick={() => onComposerChange({ open: true, kind: "general" })}
            className="rounded-md border border-stone-300 px-2 py-0.5 font-mono text-[11px] text-stone-600 transition enabled:hover:border-stone-500 enabled:hover:text-stone-900"
          >
            Add note
          </button>
        )}
      </div>

      {q.isLoading && (
        <div
          aria-busy="true"
          aria-label="Loading comments"
          className="h-[72px] animate-pulse rounded-md border border-stone-200 bg-stone-50"
        />
      )}

      {q.isError && (
        <p
          role="alert"
          className="rounded-md border border-rose-200 bg-rose-50 px-3 py-2 font-mono text-[11.5px] text-rose-800"
        >
          Couldn't load comments: {q.error?.message ?? "unknown error"}
        </p>
      )}

      {q.isSuccess && (
        <div className="overflow-hidden rounded-md border border-stone-200 bg-white shadow-sm">
          {q.data.comments.length === 0 ? (
            <p className="px-4 py-3 text-[13px] text-stone-500">No comments yet.</p>
          ) : (
            <ul className="divide-y divide-stone-100">
              {q.data.comments.map((comment) => (
                <li key={comment.id}>
                  <CommentRow comment={comment} />
                </li>
              ))}
            </ul>
          )}
        </div>
      )}

      {composer.open && !isReadOnly && (
        <form
          onSubmit={(e) => {
            e.preventDefault();
            if (!trimmedBody) return;
            create.mutate(
              { id: crId, body: trimmedBody, kind: composer.kind },
              {
                onSuccess: () => {
                  setBody("");
                  onComposerChange({ open: false, kind: "general" });
                },
              },
            );
          }}
          className="mt-2 rounded-md border border-stone-200 bg-stone-50 px-3 py-3"
        >
          <div className="mb-2 flex items-center justify-between gap-3">
            <label
              htmlFor="cr-comment-body"
              className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500"
            >
              Comment
            </label>
            <span
              className={`rounded-sm px-1.5 py-0.5 font-mono text-[10px] uppercase tracking-wider ${
                composer.kind === "changes_requested"
                  ? "bg-orange-100 text-orange-800"
                  : "bg-stone-100 text-stone-500"
              }`}
            >
              {composer.kind === "changes_requested" ? "changes requested" : "note"}
            </span>
          </div>
          <textarea
            id="cr-comment-body"
            autoFocus
            value={body}
            onChange={(e) => setBody(e.currentTarget.value)}
            maxLength={280}
            disabled={create.isPending}
            className="min-h-[86px] w-full resize-y rounded-md border border-stone-300 bg-white px-3 py-2 text-[13px] leading-relaxed text-stone-900 outline-none transition focus:border-stone-500 focus:ring-2 focus:ring-stone-200 disabled:opacity-50"
          />
          <div className="mt-2 flex flex-wrap items-center gap-2">
            <button
              type="submit"
              disabled={create.isPending || !trimmedBody}
              className="rounded-md border border-stone-900 bg-stone-900 px-3 py-1.5 text-[12.5px] font-medium text-stone-50 transition enabled:hover:bg-stone-700 disabled:cursor-not-allowed disabled:opacity-40"
            >
              {create.isPending ? "Sending..." : "Send"}
            </button>
            <button
              type="button"
              onClick={() => {
                create.reset();
                setBody("");
                onComposerChange({ open: false, kind: "general" });
              }}
              disabled={create.isPending}
              className="rounded-md border border-stone-300 px-3 py-1.5 text-[12.5px] font-medium text-stone-600 transition enabled:hover:border-stone-500 enabled:hover:text-stone-900 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Cancel
            </button>
          </div>
          {create.error && (
            <p
              role="alert"
              className="mt-2 rounded-md border border-rose-200 bg-rose-50 px-2 py-1 font-mono text-[11px] text-rose-800"
            >
              {create.error.message}
            </p>
          )}
        </form>
      )}
    </section>
  );
}

function CommentRow({ comment }: { readonly comment: ReviewComment }) {
  const isChangesRequested = comment.kind === "changes_requested";
  return (
    <article
      className={`px-4 py-3 text-[13px] ${comment.active ? "text-stone-800" : "text-stone-400"}`}
    >
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1 font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
        <span>{formatReviewActor(comment.author)}</span>
        <span aria-hidden className="text-stone-300">
          ·
        </span>
        <span className={isChangesRequested ? "text-orange-700" : "text-stone-500"}>
          {isChangesRequested ? "changes requested" : "note"}
        </span>
        {comment.path !== null && (
          <>
            <span aria-hidden className="text-stone-300">
              ·
            </span>
            <span className="normal-case tracking-normal text-stone-600">{comment.path}</span>
          </>
        )}
        {!comment.active && (
          <>
            <span aria-hidden className="text-stone-300">
              ·
            </span>
            <span>removed</span>
          </>
        )}
      </div>
      <p className="mt-1 whitespace-pre-wrap leading-relaxed">{comment.body}</p>
    </article>
  );
}

function RevertEvidence({
  result,
  targetRef,
}: {
  readonly result: StratumRevertResult;
  readonly targetRef: string;
}) {
  const resolvedTargetRef = result.target_ref ?? targetRef;
  return (
    <div className="max-w-2xl rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-[12.5px] leading-relaxed text-emerald-950">
      <p>
        Reverted {resolvedTargetRef} to {shortHash(result.reverted_to)}.
      </p>
      {result.revert_commit && (
        <p className="mt-1">
          Revert commit {shortHash(result.revert_commit)}.
        </p>
      )}
      <p className="mt-1 font-mono text-[10.5px] uppercase tracking-wider text-emerald-800">
        Audit records this as a VCS revert event.
      </p>
    </div>
  );
}

function ActionError({
  error,
  action,
}: {
  readonly error: Error;
  readonly action: "action" | "revert";
}) {
  const isRevertConflict = action === "revert" && httpStatusFromError(error) === 409;
  return (
    <div
      role="alert"
      className="rounded-md border border-rose-200 bg-rose-50 px-3 py-2 font-mono text-[11.5px] text-rose-800"
    >
      {isRevertConflict && (
        <p className="mb-1 font-sans text-[12.5px] font-medium text-rose-900">
          Revert conflict or stale head.
        </p>
      )}
      <p>{error.message}</p>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Reviewers
// ─────────────────────────────────────────────────────────────────────────────

function ReviewersPanel({
  crId,
  isReadOnly,
}: {
  readonly crId: string;
  readonly isReadOnly: boolean;
}) {
  const q = useReviewers(crId);
  const assign = useAssignReviewer();
  const [reviewer, setReviewer] = useState("");
  const [required, setRequired] = useState(false);
  const [localError, setLocalError] = useState<string | null>(null);

  const normalizedReviewer = Number.parseInt(reviewer.trim(), 10);
  const canSubmit = Number.isInteger(normalizedReviewer) && normalizedReviewer > 0;

  return (
    <section aria-labelledby="cr-detail-reviewers" className="mt-8">
      <h2
        id="cr-detail-reviewers"
        className="mb-2 font-mono text-[10.5px] uppercase tracking-wider text-stone-500"
      >
        Reviewers
      </h2>

      {q.isLoading && (
        <div
          aria-busy="true"
          aria-label="Loading reviewers"
          className="h-[54px] animate-pulse rounded-md border border-stone-200 bg-stone-50"
        />
      )}

      {q.isError && (
        <p
          role="alert"
          className="rounded-md border border-rose-200 bg-rose-50 px-3 py-2 font-mono text-[11.5px] text-rose-800"
        >
          Couldn't load reviewers: {q.error?.message ?? "unknown error"}
        </p>
      )}

      {q.isSuccess && (
        <div className="overflow-hidden rounded-md border border-stone-200 bg-white shadow-sm">
          {q.data.assignments.length === 0 ? (
            <p className="px-4 py-3 text-[13px] text-stone-500">No reviewers assigned.</p>
          ) : (
            <ul className="divide-y divide-stone-100">
              {q.data.assignments.map((assignment) => (
                <li key={assignment.id}>
                  <ReviewerRow assignment={assignment} />
                </li>
              ))}
            </ul>
          )}
        </div>
      )}

      {!isReadOnly && (
        <form
          onSubmit={(e) => {
            e.preventDefault();
            if (!canSubmit) {
              setLocalError("Enter a reviewer number.");
              return;
            }
            setLocalError(null);
            assign.mutate(
              { id: crId, reviewerUid: normalizedReviewer, required },
              {
                onSuccess: () => {
                  setReviewer("");
                  setRequired(false);
                },
              },
            );
          }}
          className="mt-2 flex flex-wrap items-end gap-2 rounded-md border border-stone-200 bg-stone-50 px-3 py-3"
        >
          <label className="min-w-[148px] flex-1">
            <span className="mb-1 block font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
              Reviewer
            </span>
            <input
              type="text"
              inputMode="numeric"
              value={reviewer}
              onChange={(e) => {
                setReviewer(e.currentTarget.value);
                setLocalError(null);
              }}
              placeholder="42"
              disabled={assign.isPending}
              className="w-full rounded-md border border-stone-300 bg-white px-2 py-1.5 font-mono text-[12.5px] text-stone-900 outline-none transition focus:border-stone-500 focus:ring-2 focus:ring-stone-200 disabled:opacity-50"
            />
          </label>
          <label className="flex items-center gap-2 rounded-md border border-stone-200 bg-white px-2.5 py-1.5 text-[12.5px] text-stone-700">
            <input
              type="checkbox"
              checked={required}
              onChange={(e) => setRequired(e.currentTarget.checked)}
              disabled={assign.isPending}
              className="size-3.5 rounded border-stone-300 accent-stone-900"
            />
            Required approval
          </label>
          <button
            type="submit"
            disabled={assign.isPending}
            className="rounded-md border border-stone-900 bg-stone-900 px-3 py-1.5 text-[12.5px] font-medium text-stone-50 transition enabled:hover:bg-stone-700 disabled:cursor-not-allowed disabled:opacity-40"
          >
            {assign.isPending ? "Assigning..." : "Assign"}
          </button>

          {(localError ?? assign.error?.message) && (
            <p
              role="alert"
              className="basis-full rounded-md border border-rose-200 bg-rose-50 px-2 py-1 font-mono text-[11px] text-rose-800"
            >
              {localError ?? assign.error?.message}
            </p>
          )}
        </form>
      )}
    </section>
  );
}

function ReviewerRow({ assignment }: { readonly assignment: ReviewerAssignment }) {
  return (
    <div
      className={`grid grid-cols-[1fr_auto] items-center gap-3 px-4 py-2.5 text-[13px] ${
        assignment.active ? "text-stone-800" : "text-stone-400"
      }`}
    >
      <div className="min-w-0">
        <div className="font-medium">{formatReviewActor(assignment.reviewer)}</div>
        <div className="font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
          assigned by {formatReviewActor(assignment.assigned_by)}
        </div>
      </div>
      <div className="flex flex-wrap justify-end gap-1">
        <span
          className={`rounded-sm px-1.5 py-0.5 font-mono text-[10px] uppercase tracking-wider ${
            assignment.required ? "bg-orange-100 text-orange-800" : "bg-stone-100 text-stone-600"
          }`}
        >
          {assignment.required ? "required" : "optional"}
        </span>
        {!assignment.active && (
          <span className="rounded-sm bg-stone-100 px-1.5 py-0.5 font-mono text-[10px] uppercase tracking-wider text-stone-500">
            inactive
          </span>
        )}
      </div>
    </div>
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
              <span className="font-mono">{formatReviewActorList(a.approved_by)}</span>
            )}
          </Row>
          {a.required_reviewers.length > 0 && (
            <Row k="Required reviewers">
              <span className="font-mono">
                {a.required_reviewers
                  .map((u) => `${formatReviewActor(u)}${a.approved_required_reviewers.includes(u) ? " ✓" : ""}`)
                  .join(", ")}
              </span>
            </Row>
          )}
          {a.missing_required_reviewers.length > 0 && (
            <Row k="Missing reviewers">
              <span className="font-mono text-amber-700">
                {a.missing_required_reviewers.map((u) => formatReviewActor(u)).join(", ")}
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
            {formatReviewActor(approval.approved_by)}
          </div>
          {approval.comment ? (
            <div className="font-mono text-[11px] text-stone-500">
              Reason: {approval.comment}
            </div>
          ) : (
            <div className="font-mono text-[11px] text-stone-400">No reason recorded</div>
          )}
          <div className="font-mono text-[11px] text-stone-500">
            dismissed by {formatReviewActor(approval.dismissed_by)}
          </div>
          {approval.dismissal_reason ? (
            <div className="font-mono text-[11px] text-stone-500">
              Dismissal reason: {approval.dismissal_reason}
            </div>
          ) : null}
        </div>
        <span className="font-mono text-[10px] uppercase tracking-wider text-stone-400">
          dismissed
        </span>
      </div>
    );
  }

  return (
    <div className="px-4 py-2">
      <div className="grid grid-cols-[60px_1fr_auto] items-start gap-3 text-[12.5px]">
        <span aria-hidden className="text-emerald-600">
          ✓
        </span>
        <div className="min-w-0 font-mono text-stone-800">
          {formatReviewActor(approval.approved_by)}
          {approval.comment ? (
            <div className="mt-0.5 text-[11px] font-normal text-stone-600">
              Reason: {approval.comment}
            </div>
          ) : (
            <div className="mt-0.5 text-[11px] font-normal text-stone-400">No reason recorded</div>
          )}
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
// Diff
// ─────────────────────────────────────────────────────────────────────────────

function DiffSection({
  cr,
  viewedFiles,
}: {
  readonly cr: ChangeRequest;
  readonly viewedFiles: UseQueryResult<ViewedFilesResponse, Error>;
}) {
  const q = useChangeRequestDiff(cr);
  const setViewed = useSetViewedFile();
  const parsed = useMemo(() => (q.data !== undefined ? parseDiff(q.data) : null), [q.data]);
  const isTerminal = cr.status !== "open";
  const requiredPaths = viewedFiles.data?.required_paths ?? [];
  const unviewedPaths = viewedFiles.data?.unviewed_paths ?? [];
  const viewedCount = Math.max(requiredPaths.length - unviewedPaths.length, 0);
  const allViewed = requiredPaths.length === 0 || unviewedPaths.length === 0;
  const markAllDisabled =
    isTerminal || viewedFiles.isLoading || viewedFiles.isError || setViewed.isPending;

  function isPathViewed(path: string): boolean {
    if (!viewedFiles.isSuccess) return false;
    return !unviewedPaths.includes(path);
  }

  function togglePath(path: string, viewed: boolean) {
    setViewed.mutate({ id: cr.id, path, viewed });
  }

  function markAll(viewed: boolean) {
    const targets = viewed ? unviewedPaths : requiredPaths.filter((path) => isPathViewed(path));
    for (const path of targets) {
      setViewed.mutate({ id: cr.id, path, viewed });
    }
  }

  return (
    <section aria-labelledby="cr-detail-diff" className="mt-8">
      <h2
        id="cr-detail-diff"
        className="mb-2 font-mono text-[10.5px] uppercase tracking-wider text-stone-500"
      >
        Diff
      </h2>
      <div className="mb-2 flex flex-wrap items-center gap-x-3 gap-y-1 font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
        <span title={cr.base_commit}>base {shortHash(cr.base_commit)}</span>
        <span aria-hidden className="text-stone-300">
          →
        </span>
        <span title={cr.head_commit}>head {shortHash(cr.head_commit)}</span>
      </div>

      {viewedFiles.isSuccess && (
        <div className="mb-3 flex flex-wrap items-center gap-3">
          <span className="font-mono text-[11.5px] tabular-nums text-stone-600">
            {viewedCount} of {requiredPaths.length} files viewed
          </span>
          {!isTerminal && (
            <button
              type="button"
              onClick={() => markAll(!allViewed)}
              disabled={markAllDisabled || requiredPaths.length === 0}
              className="rounded-md border border-stone-300 px-2 py-0.5 font-mono text-[10.5px] text-stone-600 transition enabled:hover:border-stone-500 enabled:hover:text-stone-900 disabled:cursor-not-allowed disabled:opacity-40"
            >
              {allViewed ? "Unmark all" : "Mark all viewed"}
            </button>
          )}
        </div>
      )}

      {q.isLoading && (
        <div
          aria-busy="true"
          aria-label="Loading diff"
          className="h-[220px] animate-pulse rounded-md border border-stone-200 bg-stone-50"
        />
      )}

      {q.isError && (
        <div role="alert" className="rounded-md border border-rose-200 bg-rose-50 px-5 py-4">
          <div className="font-mono text-[10.5px] uppercase tracking-wider text-rose-700">
            Couldn't load diff
          </div>
          <p className="mt-1 font-mono text-[12px] text-rose-800">
            {q.error?.message ?? "Unknown error."}
          </p>
          <button
            type="button"
            onClick={() => {
              void q.refetch();
            }}
            className="mt-3 rounded-md border border-rose-300 bg-white px-3 py-1 text-[12px] font-medium text-rose-800 transition hover:border-rose-500 hover:bg-rose-50"
          >
            Retry
          </button>
        </div>
      )}

      {q.isSuccess && parsed !== null && parsed.isEmpty && (
        <div className="rounded-md border border-dashed border-stone-300 bg-stone-50 px-6 py-12 text-center text-stone-500">
          <p className="font-serif italic">No changes between these refs.</p>
        </div>
      )}

      {q.isSuccess && parsed !== null && !parsed.isEmpty && (
        <div className="flex flex-col gap-3">
          {parsed.fragments.map((fragment) => (
            <ReviewedFileCard
              key={fragment.path}
              crId={cr.id}
              fragment={fragment}
              viewed={isPathViewed(fragment.path)}
              checkboxDisabled={
                isTerminal ||
                viewedFiles.isLoading ||
                viewedFiles.isError ||
                (setViewed.isPending && setViewed.variables?.path === fragment.path)
              }
              onToggle={(viewed) => togglePath(fragment.path, viewed)}
            />
          ))}
        </div>
      )}
    </section>
  );
}

function ReviewedFileCard({
  crId,
  fragment,
  viewed,
  checkboxDisabled,
  onToggle,
}: {
  readonly crId: string;
  readonly fragment: DiffFragment;
  readonly viewed: boolean;
  readonly checkboxDisabled: boolean;
  readonly onToggle: (viewed: boolean) => void;
}) {
  const [open, setOpen] = useState(true);
  const { added, removed } = fragmentTotals(fragment);
  const kindLabel = summariseFragmentKind(fragment.kind);

  return (
    <section
      aria-labelledby={`cr-diff-${crId}-${fragment.path}`}
      className={`overflow-hidden rounded-md border bg-white shadow-sm ${
        viewed ? "border-stone-100 opacity-80" : "border-stone-200"
      }`}
    >
      <header className="flex items-center gap-3 border-b border-stone-100 bg-stone-50 px-3 py-2">
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          aria-expanded={open}
          aria-controls={`cr-diff-body-${crId}-${fragment.path}`}
          className="rounded-sm px-1 py-0.5 text-stone-500 hover:bg-stone-200"
        >
          <svg
            width="10"
            height="10"
            viewBox="0 0 16 16"
            aria-hidden
            style={{ transform: open ? "rotate(90deg)" : "none", transition: "transform 160ms ease" }}
          >
            <path d="M6 3l5 5-5 5" stroke="currentColor" strokeWidth={1.5} fill="none" strokeLinecap="round" />
          </svg>
        </button>
        <h3
          id={`cr-diff-${crId}-${fragment.path}`}
          className={`min-w-0 flex-1 truncate font-mono text-[12.5px] ${
            viewed ? "text-stone-500 line-through decoration-stone-300" : "text-stone-900"
          }`}
        >
          {fragment.path}
        </h3>
        {fragment.kind === "text-unified" && (
          <span className="font-mono text-[11px] tabular-nums">
            <span className="text-emerald-700">+{added}</span>{" "}
            <span className="text-rose-700">−{removed}</span>
          </span>
        )}
        <span className="font-mono text-[10px] uppercase tracking-wider text-stone-500">
          {kindLabel}
        </span>
        <label className="flex items-center gap-1.5 rounded-md border border-stone-200 bg-white px-2 py-0.5 text-[11.5px] text-stone-600">
          <input
            type="checkbox"
            checked={viewed}
            disabled={checkboxDisabled}
            onChange={(e) => onToggle(e.currentTarget.checked)}
            className="h-3 w-3 accent-emerald-600 disabled:cursor-not-allowed disabled:opacity-40"
          />
          Viewed
        </label>
      </header>
      {open && (
        <div id={`cr-diff-body-${crId}-${fragment.path}`}>
          <DiffFragmentBody fragment={fragment} />
        </div>
      )}
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

function shortHash(hash: string): string {
  return hash.slice(0, 8);
}
