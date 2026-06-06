/**
 * Review-related query + mutation hooks.
 *
 * One file co-locating everything that touches /change-requests,
 * /change-requests/:id, .../approvals, .../reviewers, .../comments,
 * .../merge, .../reject, .../approvals/:aid/dismiss.
 *
 * Pattern other api/ modules follow:
 *   - Each hook wraps useQuery / useMutation around an SDK call.
 *   - Query keys are stable arrays starting with the resource name,
 *     e.g. ["change-requests"] for the list, ["change-request", id] for
 *     the detail. Sign-out clears the entire cache via QueryProvider so
 *     we don't need to scope keys to the user.
 *   - Mutations don't auto-retry (QueryProvider default). Each mutation
 *     issues an Idempotency-Key under the hood via the SDK; explicit
 *     retry is the caller's responsibility.
 *   - Return types come straight from `@stratum/sdk`. Tightening the
 *     SDK type tightens this file at typecheck time.
 *
 * This module ships the list query only. Detail + approve + reject +
 * merge + reviewers + comments land in D2 through D6 of the roadmap.
 */

import type {
  ApprovalListResponse,
  ApprovalResponse,
  ChangeRequestListResponse,
  ChangeRequestResponse,
  CommentListResponse,
  CommentRequest,
  CommentResponse,
  ReviewerListResponse,
  ReviewerRequest,
  ReviewerResponse,
} from "@stratum/sdk";
import {
  useMutation,
  useQuery,
  useQueryClient,
  type UseMutationResult,
  type UseQueryResult,
} from "@tanstack/react-query";
import { isTerminalHttpError } from "../query.tsx";
import { useStratumClient } from "../stratum-client.ts";

// ─────────────────────────────────────────────────────────────────────────────
// Query keys
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Centralised key factory. Components and tests reference these so a typo
 * doesn't cause a phantom cache miss. Pattern borrowed from TanStack
 * Query's own docs.
 */
export const reviewKeys = {
  all: ["change-requests"] as const,
  list: () => [...reviewKeys.all, "list"] as const,
  detail: (id: string) => [...reviewKeys.all, "detail", id] as const,
  approvals: (id: string) => [...reviewKeys.all, "approvals", id] as const,
  reviewers: (id: string) => [...reviewKeys.all, "reviewers", id] as const,
  comments: (id: string) => [...reviewKeys.all, "comments", id] as const,
  diff: (base: string, head: string) => [...reviewKeys.all, "diff", base, head] as const,
};

// ─────────────────────────────────────────────────────────────────────────────
// useChangeRequests — list all change requests visible to the current user
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Returns the list of all change requests. Backend filters by what the
 * authenticated principal can see; we don't paginate yet (the API doesn't
 * either — it returns the bounded recent set).
 *
 * Components destructure { data, isLoading, isError, error, refetch }.
 * `data` is `ChangeRequestListResponse | undefined` while loading; tests
 * + components must handle the undefined case explicitly.
 */
export function useChangeRequests(): UseQueryResult<ChangeRequestListResponse, Error> {
  const client = useStratumClient();
  return useQuery({
    queryKey: reviewKeys.list(),
    queryFn: () => client.reviews.listChangeRequests(),
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// useChangeRequest — fetch a single change request by id
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Fetch a single change request + its current approval_state.
 *
 *   404            terminal — surfaces as error; the detail screen renders
 *                  a "not found" card with a link back to /reviews.
 *   403            terminal — surfaces as error; "you don't have access"
 *                  card. The user menu's Sign out button gives an out.
 *   5xx / network  retried (per the QueryProvider default policy).
 *
 * Detail keys are nested under reviewKeys.detail(id) so the list query's
 * invalidation doesn't accidentally refetch every open detail screen.
 */
export function useChangeRequest(id: string): UseQueryResult<ChangeRequestResponse, Error> {
  const client = useStratumClient();
  return useQuery({
    queryKey: reviewKeys.detail(id),
    queryFn: () => client.reviews.getChangeRequest(id),
    // Detail screen will be opened-and-read-many-times in a review session
    // (reviewer scrolls, jumps back to list, returns). 30s staleness is
    // a small risk vs the polish of "feels live."
    staleTime: 30_000,
    // Don't burn retries on terminal client errors — show the error card.
    retry: (failureCount, error) => !isTerminalHttpError(error) && failureCount < 2,
  });
}

/**
 * Fetch the exact diff for a CR's recorded base/head commits.
 *
 * This intentionally reads from `client.vcs.diff({ base, head })` rather
 * than assembling a URL in the component. The SDK owns the wire shape and
 * query encoding.
 */
export function useChangeRequestDiff(
  cr: ChangeRequestResponse["change_request"],
): UseQueryResult<string, Error> {
  const client = useStratumClient();
  return useQuery({
    queryKey: reviewKeys.diff(cr.base_commit, cr.head_commit),
    queryFn: () => client.vcs.diff({ base: cr.base_commit, head: cr.head_commit }),
    staleTime: 30_000,
    retry: (failureCount, error) => !isTerminalHttpError(error) && failureCount < 2,
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// useApprovals — list approvals on a CR (active + dismissed)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Fetch the full approval history for one CR — includes dismissed
 * records so the detail screen can show "approved by uid:42, dismissed
 * by uid:0 (reason: stale head)". Active approvals get an inline
 * Dismiss button; dismissed ones render their dismissal trail.
 *
 * Keyed under reviewKeys.approvals(id) so dismiss mutations can
 * invalidate just this list without touching the CR detail query.
 */
export function useApprovals(id: string): UseQueryResult<ApprovalListResponse, Error> {
  const client = useStratumClient();
  return useQuery({
    queryKey: reviewKeys.approvals(id),
    queryFn: () => client.reviews.listApprovals(id),
    staleTime: 30_000,
    retry: (failureCount, error) => !isTerminalHttpError(error) && failureCount < 2,
  });
}

/**
 * Convenience: pluck just the `change_requests` array out of the list
 * response. Components that only need the array (not the wrapper) use
 * this and skip the `data?.change_requests ?? []` boilerplate.
 */
export function useChangeRequestList(): {
  readonly items: readonly ChangeRequestResponse[];
  readonly isLoading: boolean;
  readonly isError: boolean;
  readonly error: Error | null;
  readonly refetch: () => void;
} {
  const q = useChangeRequests();
  return {
    items: q.data?.change_requests ?? [],
    isLoading: q.isLoading,
    isError: q.isError,
    error: q.error,
    refetch: () => {
      void q.refetch();
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Mutation hooks — D3
//
// Pattern, all four:
//   - Idempotency-Key generated frontend-side per mutationFn invocation
//     via crypto.randomUUID(). TanStack Query's mutate() runs mutationFn
//     once per user action, so each click → one key → one request, and
//     the SDK's auto-key fallback isn't relied on (which would generate
//     a different key per invocation, breaking idempotency under React
//     StrictMode's render double-invoke if a key were ever generated
//     during render).
//
//   - No automatic retry (QueryProvider default). Caller drives retry
//     by calling mutate() again. Idempotency keys make this safe.
//
//   - onSuccess invalidates both the affected detail query and the list
//     query. Components reading either one see the new state on next
//     render. The detail query refetch yields the new approval_state /
//     status; the list query refetch yields updated status badges +
//     filter counts.
//
//   - Hooks accept variables typed to the SDK's request types, not a
//     generic Record<string, unknown>. TypeScript catches a typo before
//     the mutation runs.
// ─────────────────────────────────────────────────────────────────────────────

/** Stable id generator. Exposed for tests that want to assert key shape. */
function newIdempotencyKey(): string {
  // crypto.randomUUID is available in modern browsers + happy-dom + node 19+.
  return globalThis.crypto.randomUUID();
}

/**
 * POST /change-requests/:id/approvals — record an approval (optionally
 * with a short comment). Returns ApprovalResponse which carries the
 * updated approval_state so consumers can show the new "X of Y approvals"
 * count immediately (in addition to the cache-driven refetch).
 */
export function useApproveChangeRequest(): UseMutationResult<
  ApprovalResponse,
  Error,
  { readonly id: string; readonly comment?: string }
> {
  const client = useStratumClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ id, comment }) => {
      const idempotencyKey = newIdempotencyKey();
      return client.reviews.approve(
        id,
        comment !== undefined ? { comment } : {},
        { idempotencyKey },
      );
    },
    onSuccess: (_data, vars) => {
      void queryClient.invalidateQueries({ queryKey: reviewKeys.detail(vars.id) });
      void queryClient.invalidateQueries({ queryKey: reviewKeys.list() });
      // Approve creates a new approval record — the approvals list cache
      // is now stale.
      void queryClient.invalidateQueries({ queryKey: reviewKeys.approvals(vars.id) });
    },
  });
}

/**
 * POST /change-requests/:id/reject — mark the CR rejected. Terminal —
 * subsequent approve/merge calls on this CR will 4xx after a reject.
 */
export function useRejectChangeRequest(): UseMutationResult<
  ChangeRequestResponse,
  Error,
  { readonly id: string }
> {
  const client = useStratumClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ id }) => {
      const idempotencyKey = newIdempotencyKey();
      return client.reviews.reject(id, { idempotencyKey });
    },
    onSuccess: (_data, vars) => {
      void queryClient.invalidateQueries({ queryKey: reviewKeys.detail(vars.id) });
      void queryClient.invalidateQueries({ queryKey: reviewKeys.list() });
    },
  });
}

/**
 * POST /change-requests/:id/merge — fast-forward-merge an approved CR.
 * Backend rejects with 403 if approval_state.approved is false, or 409
 * if source/target refs have moved since the CR was created. The action
 * row UI gates the button to approved+ready so 409 is the more likely
 * surface (someone else committed in the meantime).
 */
export function useMergeChangeRequest(): UseMutationResult<
  ChangeRequestResponse,
  Error,
  { readonly id: string }
> {
  const client = useStratumClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ id }) => {
      const idempotencyKey = newIdempotencyKey();
      return client.reviews.merge(id, { idempotencyKey });
    },
    onSuccess: (_data, vars) => {
      void queryClient.invalidateQueries({ queryKey: reviewKeys.detail(vars.id) });
      void queryClient.invalidateQueries({ queryKey: reviewKeys.list() });
    },
  });
}

/**
 * POST /change-requests/:id/approvals/:aid/dismiss — flag an existing
 * approval inactive (e.g. it was for an older head_commit). Inline action
 * on each approval row in the breakdown, not in the top-level action row.
 */
export function useDismissApproval(): UseMutationResult<
  ApprovalResponse,
  Error,
  { readonly id: string; readonly approvalId: string; readonly reason?: string }
> {
  const client = useStratumClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ id, approvalId, reason }) => {
      const idempotencyKey = newIdempotencyKey();
      return client.reviews.dismissApproval(
        id,
        approvalId,
        reason !== undefined ? { reason } : {},
        { idempotencyKey },
      );
    },
    onSuccess: (_data, vars) => {
      void queryClient.invalidateQueries({ queryKey: reviewKeys.detail(vars.id) });
      void queryClient.invalidateQueries({ queryKey: reviewKeys.list() });
      // Dismissed approval flips active→false in the list response.
      void queryClient.invalidateQueries({ queryKey: reviewKeys.approvals(vars.id) });
    },
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// useReviewers / useAssignReviewer — D5
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Fetch reviewer assignments for a CR. Required reviewers also appear in
 * approval_state, but this list preserves assignment metadata and active
 * status for the visible reviewer surface.
 */
export function useReviewers(id: string): UseQueryResult<ReviewerListResponse, Error> {
  const client = useStratumClient();
  return useQuery({
    queryKey: reviewKeys.reviewers(id),
    queryFn: () => client.reviews.listReviewers(id),
    staleTime: 30_000,
    retry: (failureCount, error) => !isTerminalHttpError(error) && failureCount < 2,
  });
}

/**
 * POST /change-requests/:id/reviewers — assign a reviewer or update
 * their required/optional status. Backend returns the fresh approval
 * state because required reviewers can change merge readiness.
 */
export function useAssignReviewer(): UseMutationResult<
  ReviewerResponse,
  Error,
  { readonly id: string; readonly reviewerUid: number; readonly required?: boolean }
> {
  const client = useStratumClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ id, reviewerUid, required }) => {
      const idempotencyKey = newIdempotencyKey();
      const request: ReviewerRequest = {
        reviewer_uid: reviewerUid,
        ...(required !== undefined ? { required } : {}),
      };
      return client.reviews.assignReviewer(id, request, { idempotencyKey });
    },
    onSuccess: (_data, vars) => {
      void queryClient.invalidateQueries({ queryKey: reviewKeys.reviewers(vars.id) });
      void queryClient.invalidateQueries({ queryKey: reviewKeys.detail(vars.id) });
      void queryClient.invalidateQueries({ queryKey: reviewKeys.approvals(vars.id) });
      void queryClient.invalidateQueries({ queryKey: reviewKeys.list() });
    },
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// useComments / useCreateComment — D4
//
// Review comments serve two purposes today:
//   - general                   reviewer's threaded discussion
//   - changes_requested         signals "do not merge, please revise"
//                               (the D2.2 "Request changes" button
//                               becomes a one-click composer with this
//                               kind pre-selected)
// Both surface in the same thread; the kind drives visual treatment.
//
// Audit details strip the body intentionally — backend redacts comment
// text from audit events. Components can safely show the body in the
// thread; we never log it.
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Fetch the full comment thread for a CR. Includes both active and
 * inactive (deleted-server-side; field is `active: boolean`). The
 * thread component renders both, dimming inactive ones with their
 * historical trail — same posture as the approvals list.
 */
export function useComments(id: string): UseQueryResult<CommentListResponse, Error> {
  const client = useStratumClient();
  return useQuery({
    queryKey: reviewKeys.comments(id),
    queryFn: () => client.reviews.listComments(id),
    staleTime: 30_000,
    retry: (failureCount, error) => !isTerminalHttpError(error) && failureCount < 2,
  });
}

/**
 * POST /change-requests/:id/comments — append a comment.
 *
 *   body  required, non-empty after trim, bounded by backend (~280 chars)
 *   path  optional absolute path; pin a comment to a file
 *   kind  optional, defaults to "general"; "changes_requested" is the
 *         signal the merge button reads via the UI
 *
 * onSuccess invalidates the comments list. We don't touch the detail
 * cache because creating a comment doesn't change approval_state —
 * the rollup the detail screen renders stays valid.
 */
export function useCreateComment(): UseMutationResult<
  CommentResponse,
  Error,
  { readonly id: string; readonly body: string; readonly path?: string; readonly kind?: "general" | "changes_requested" }
> {
  const client = useStratumClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ id, body, path, kind }) => {
      const idempotencyKey = newIdempotencyKey();
      const request: CommentRequest = {
        body,
        ...(path !== undefined ? { path } : {}),
        ...(kind !== undefined ? { kind } : {}),
      };
      return client.reviews.createComment(id, request, { idempotencyKey });
    },
    onSuccess: (_data, vars) => {
      void queryClient.invalidateQueries({ queryKey: reviewKeys.comments(vars.id) });
    },
  });
}
