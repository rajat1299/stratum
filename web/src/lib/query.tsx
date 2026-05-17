/**
 * QueryProvider — the single TanStack Query setup for the console.
 *
 * Sensible defaults:
 *
 *   staleTime              60_000   one minute. Most reads (change-requests,
 *                                   log) don't drift in seconds. Tighten on
 *                                   a per-query basis when a route does need
 *                                   live data (e.g. /audit, when SSE lands).
 *   gcTime                 300_000  five minutes before unused queries are
 *                                   garbage collected.
 *   refetchOnWindowFocus   true     reviewer comes back to the tab → get
 *                                   fresh data. Apple-style "always current".
 *   queries.retry          2        two retries with exponential backoff.
 *                                   Stratum returns precise status codes
 *                                   (403, 404, 409); we skip retrying those.
 *   mutations.retry        0        explicit retry only. Idempotency-Key
 *                                   makes this safe but we want the caller
 *                                   to drive the retry decision.
 *
 * Auth coupling:
 *   - QueryProvider sits *inside* AuthProvider so we can listen to
 *     sign-out and clear the cache. Otherwise a previous user's data
 *     would survive the StratumClient swap and leak into the next
 *     session's first paint.
 *
 * Provider order in main.tsx:
 *
 *   <AuthProvider>
 *     <QueryProvider>
 *       <RouterProvider />
 *     </QueryProvider>
 *   </AuthProvider>
 */

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, type ReactNode } from "react";
import { useAuth } from "./auth.tsx";

// ─────────────────────────────────────────────────────────────────────────────
// Defaults
// ─────────────────────────────────────────────────────────────────────────────

/** Build a QueryClient with our defaults. Exported so tests can build their own. */
export function makeQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: 60_000,
        gcTime: 300_000,
        refetchOnWindowFocus: true,
        retry: (failureCount, error) => {
          // Don't retry on precise client errors — these aren't transient.
          if (isTerminalHttpError(error)) return false;
          return failureCount < 2;
        },
      },
      mutations: {
        retry: 0,
      },
    },
  });
}

/** Heuristic: does the error look like a 4xx that wouldn't change on retry? */
export function isTerminalHttpError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const status = (error as { status?: unknown }).status;
  if (typeof status !== "number") return false;
  // 400 bad request, 403 forbidden, 404 not found, 409 conflict — all terminal.
  return status >= 400 && status < 500;
}

// ─────────────────────────────────────────────────────────────────────────────
// Provider
// ─────────────────────────────────────────────────────────────────────────────

export interface QueryProviderProps {
  readonly children: ReactNode;
  /** Inject a custom client in tests. Production gets a fresh per-mount client. */
  readonly client?: QueryClient;
}

export function QueryProvider({ children, client }: QueryProviderProps) {
  // useMemo (not useState) so the client is referentially stable across
  // re-renders without holding it in component state.
  const queryClient = useMemo(() => client ?? makeQueryClient(), [client]);
  const auth = useAuth();

  // Clear cache on the authed → anon TRANSITION only. The initial hydrate
  // to anon (most-common first paint) must NOT clear, because any query
  // a layout component started in parallel would have its result wiped.
  const wasAuthedRef = useRef(false);
  useEffect(() => {
    if (auth.state.status === "authed") {
      wasAuthedRef.current = true;
    } else if (auth.state.status === "anon" && wasAuthedRef.current) {
      queryClient.clear();
      wasAuthedRef.current = false;
    }
  }, [auth.state.status, queryClient]);

  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}
