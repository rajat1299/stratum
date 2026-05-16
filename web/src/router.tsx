/**
 * Router — code-based TanStack Router setup for Phase A2.
 *
 * Five routes:
 *
 *   /              IndexRedirect — bounces to /reviews (or /login if anon)
 *   /login         LoginScreen, wrapped in RequireAnon
 *   /reviews       ReviewsPlaceholder, wrapped in RequireAuth
 *   /spike/diff    The week-2 design spike — no auth required, dev tool
 *   *              NotFound — minimal, links back home
 *
 * We're going code-based (not file-based + plugin codegen) because:
 *   - The route count is small for the next several phases.
 *   - One file is easier to review than a tree of files + a generated
 *     routeTree.gen.ts.
 *   - Migration to file-based later is mechanical when the plugin is
 *     installed.
 *
 * Auth gating uses the components in ./lib/auth-gates.tsx rather than
 * TanStack's beforeLoad hook. The gates are pure React + useEffect,
 * which keeps the gate logic testable without booting the router and
 * avoids the contortion of getting React context out of a non-React
 * beforeLoad callback.
 */

import {
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
  useNavigate,
} from "@tanstack/react-router";
import { useCallback, useEffect } from "react";
import { LoginScreen } from "./components/LoginScreen.tsx";
import { ReviewsPlaceholder } from "./components/ReviewsPlaceholder.tsx";
import { RequireAnon, RequireAuth } from "./lib/auth-gates.tsx";
import { useAuth } from "./lib/auth.tsx";
import { SpikeApp } from "./spike/diff-spike.tsx";

// ─────────────────────────────────────────────────────────────────────────────
// Routes
// ─────────────────────────────────────────────────────────────────────────────

const rootRoute = createRootRoute({
  component: RootLayout,
  notFoundComponent: NotFound,
});

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: IndexRedirect,
});

const loginRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/login",
  component: LoginRoute,
});

const reviewsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/reviews",
  component: ReviewsRoute,
});

const spikeDiffRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/spike/diff",
  component: SpikeApp,
});

const routeTree = rootRoute.addChildren([indexRoute, loginRoute, reviewsRoute, spikeDiffRoute]);

export const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Components
// ─────────────────────────────────────────────────────────────────────────────

function RootLayout() {
  // Currently chrome-less; the app shell (nav, breadcrumb, palette) lands
  // in Phase A3. For now Outlet renders directly into the page.
  return <Outlet />;
}

function IndexRedirect() {
  const { state } = useAuth();
  const navigate = useNavigate();
  useEffect(() => {
    if (state.status === "authed") navigate({ to: "/reviews" }).catch(() => undefined);
    else if (state.status === "anon") navigate({ to: "/login" }).catch(() => undefined);
  }, [state.status, navigate]);
  return (
    <div
      role="status"
      aria-live="polite"
      className="grid min-h-screen place-items-center bg-stone-50 text-stone-400"
    >
      <span className="font-mono text-[11px] uppercase tracking-wider">loading…</span>
    </div>
  );
}

function LoginRoute() {
  const nav = useNavigateString();
  return (
    <RequireAnon redirectTo="/reviews" navigate={nav}>
      <LoginScreen />
    </RequireAnon>
  );
}

function ReviewsRoute() {
  const nav = useNavigateString();
  return (
    <RequireAuth redirectTo="/login" navigate={nav}>
      <ReviewsPlaceholder />
    </RequireAuth>
  );
}

function NotFound() {
  return (
    <div className="grid min-h-screen place-items-center bg-stone-50 text-center">
      <div>
        <h1 className="text-[24px] font-medium tracking-tight text-stone-900">404</h1>
        <p className="mt-1 font-serif text-[14px] italic text-stone-500">No route here.</p>
        <a
          href="/"
          className="mt-4 inline-block rounded-md border border-stone-300 px-3 py-1.5 text-[12.5px] text-stone-700 hover:border-stone-500 hover:text-stone-900"
        >
          Back home
        </a>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Hooks
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Adapter: useNavigate returns a TanStack-typed function. Our auth gates
 * accept `(to: string) => void` so they stay decoupled from the router
 * library. This bridge does the cast in exactly one place.
 */
function useNavigateString(): (to: string) => void {
  const nav = useNavigate();
  return useCallback(
    (to: string) => {
      // The gates pass plain strings; TanStack Router's typed routing wants
      // a typed route id. The gates are the abstraction boundary so the
      // cast lives here, in exactly one place.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      void nav({ to: to as any });
    },
    [nav],
  );
}
