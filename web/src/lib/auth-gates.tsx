/**
 * Auth gates — the small components route layouts wrap their <Outlet>s in.
 *
 * Split out of router.tsx so they're testable without booting the router.
 * Two flavours:
 *
 *   <RequireAuth>     redirects to /login when anon; renders children when
 *                     authed; shows a quiet placeholder during hydration.
 *
 *   <RequireAnon>     redirects to /reviews when already authed; renders
 *                     children when anon; placeholder during hydration.
 *                     Used by /login so signed-in users don't see the form.
 *
 * The redirect target paths are constructor args so the gates aren't
 * coupled to specific route names; the router file picks the names.
 *
 * Redirects fire from a useEffect so React renders something this tick
 * even if the gate decides to navigate — avoids "you can't update a
 * component while rendering a different component" warnings under StrictMode.
 */

import { useEffect, type ReactNode } from "react";
import { useAuth } from "./auth.tsx";

export interface AuthGateProps {
  readonly children: ReactNode;
  /** Where to send the user when the gate decides to redirect. */
  readonly redirectTo: string;
  /** Injected by the router file. Pass useNavigate or a test stub. */
  readonly navigate: (to: string) => void;
  /** Optional override for the hydration placeholder. */
  readonly fallback?: ReactNode;
}

const defaultFallback = (
  <div
    role="status"
    aria-live="polite"
    className="grid min-h-screen place-items-center bg-stone-50 text-stone-400"
  >
    <span className="font-mono text-[11px] uppercase tracking-wider">loading…</span>
  </div>
);

/** Allow rendering only when authed. Anon → redirect. */
export function RequireAuth({ children, redirectTo, navigate, fallback }: AuthGateProps) {
  const { state } = useAuth();
  useEffect(() => {
    if (state.status === "anon") navigate(redirectTo);
  }, [state.status, navigate, redirectTo]);
  if (state.status === "authed") return <>{children}</>;
  return <>{fallback ?? defaultFallback}</>;
}

/** Allow rendering only when anon. Authed → redirect (so /login bounces to /reviews). */
export function RequireAnon({ children, redirectTo, navigate, fallback }: AuthGateProps) {
  const { state } = useAuth();
  useEffect(() => {
    if (state.status === "authed") navigate(redirectTo);
  }, [state.status, navigate, redirectTo]);
  if (state.status === "anon") return <>{children}</>;
  return <>{fallback ?? defaultFallback}</>;
}
