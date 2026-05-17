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
  ChangeRequestListResponse,
  ChangeRequestResponse,
} from "@stratum/sdk";
import { useQuery, type UseQueryResult } from "@tanstack/react-query";
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
