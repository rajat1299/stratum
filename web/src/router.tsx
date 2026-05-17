/**
 * Router — code-based TanStack Router setup.
 *
 * Structure:
 *
 *   rootRoute
 *   ├── /                  IndexRedirect          (chrome-less)
 *   ├── /login             LoginScreen + RequireAnon  (chrome-less)
 *   ├── /spike/diff        SpikeApp                  (chrome-less, dev tool)
 *   └── _shell (layout)    ShellLayout
 *       │                  RequireAuth + AppShell + CommandPalette
 *       ├── /reviews       ReviewsPlaceholder
 *       ├── /repository    RepositoryPlaceholder
 *       ├── /audit         AuditPlaceholder
 *       └── /settings      SettingsPlaceholder
 *
 * The layout route uses `id` (no path), groups its children under one
 * RequireAuth + AppShell render so the chrome doesn't unmount on every
 * route change. Auth gating happens once at the layout level instead of
 * once per child route.
 *
 * Login + index redirect + spike stay chrome-less so the full-screen
 * treatments work as designed.
 */

import {
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
  useNavigate,
  useRouterState,
} from "@tanstack/react-router";
import { lazy, Suspense, useCallback, useEffect, useMemo, useState } from "react";
import { AppShell, type NavItem } from "./components/AppShell.tsx";
import { CommandPalette, type CommandItem, usePaletteShortcut } from "./components/CommandPalette.tsx";
import { LoginScreen } from "./components/LoginScreen.tsx";
import { RequireAnon, RequireAuth } from "./lib/auth-gates.tsx";
import { useAuth } from "./lib/auth.tsx";

// ─────────────────────────────────────────────────────────────────────────────
// Lazy-loaded route components (Suspense fallback at RootLayout)
//
// Eager:  LoginScreen, AppShell, CommandPalette, auth gates — these ship in
//         the main bundle so first-paint on /login is fast (no spike code,
//         no diff parser).
// Lazy:   In-shell placeholders + both spikes. Each becomes its own chunk;
//         the parser + viewer + spike layouts don't load until a route that
//         needs them.
// ─────────────────────────────────────────────────────────────────────────────

const ReviewsScreen = lazy(() =>
  import("./components/ReviewsScreen.tsx").then((m) => ({ default: m.ReviewsScreen })),
);
const RepositoryPlaceholder = lazy(() =>
  import("./components/RepositoryPlaceholder.tsx").then((m) => ({ default: m.RepositoryPlaceholder })),
);
const AuditPlaceholder = lazy(() =>
  import("./components/AuditPlaceholder.tsx").then((m) => ({ default: m.AuditPlaceholder })),
);
const SettingsPlaceholder = lazy(() =>
  import("./components/SettingsPlaceholder.tsx").then((m) => ({ default: m.SettingsPlaceholder })),
);
const SpikeApp = lazy(() =>
  import("./spike/diff-spike.tsx").then((m) => ({ default: m.SpikeApp })),
);
const DiffAsReviewedSpike = lazy(() =>
  import("./spike/diff-as-reviewed.tsx").then((m) => ({ default: m.DiffAsReviewedSpike })),
);

// ─────────────────────────────────────────────────────────────────────────────
// Nav config (shell + palette pull from the same source)
// ─────────────────────────────────────────────────────────────────────────────

const NAV_ITEMS: NavItem[] = [
  { to: "/reviews", label: "Reviews", icon: <InboxIcon /> },
  { to: "/repository", label: "Repository", icon: <FolderIcon /> },
  { to: "/audit", label: "Audit", icon: <ClipboardIcon /> },
  { to: "/settings", label: "Settings", icon: <GearIcon /> },
];

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

const spikeDiffRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/spike/diff",
  component: SpikeApp,
});

const spikeReviewRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/spike/review",
  component: DiffAsReviewedSpike,
});

// Pathless layout route — owns the shell + palette + auth gate.
const shellLayoutRoute = createRoute({
  getParentRoute: () => rootRoute,
  id: "_shell",
  component: ShellLayout,
});

const reviewsRoute = createRoute({
  getParentRoute: () => shellLayoutRoute,
  path: "/reviews",
  component: ReviewsScreen,
});

const repositoryRoute = createRoute({
  getParentRoute: () => shellLayoutRoute,
  path: "/repository",
  component: RepositoryPlaceholder,
});

const auditRoute = createRoute({
  getParentRoute: () => shellLayoutRoute,
  path: "/audit",
  component: AuditPlaceholder,
});

const settingsRoute = createRoute({
  getParentRoute: () => shellLayoutRoute,
  path: "/settings",
  component: SettingsPlaceholder,
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  loginRoute,
  spikeDiffRoute,
  spikeReviewRoute,
  shellLayoutRoute.addChildren([reviewsRoute, repositoryRoute, auditRoute, settingsRoute]),
]);

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
  // Suspense here catches every lazy route below. Single fallback keeps
  // route transitions visually consistent. The shell layout below adds a
  // tighter fallback inside the chrome so the sidebar doesn't flash to
  // "loading" between in-shell route swaps.
  return (
    <Suspense fallback={<LoadingScreen />}>
      <Outlet />
    </Suspense>
  );
}

function IndexRedirect() {
  const { state } = useAuth();
  const navigate = useNavigate();
  useEffect(() => {
    if (state.status === "authed") navigate({ to: "/reviews" }).catch(() => undefined);
    else if (state.status === "anon") navigate({ to: "/login" }).catch(() => undefined);
  }, [state.status, navigate]);
  return <LoadingScreen />;
}

function LoginRoute() {
  const nav = useNavigateString();
  return (
    <RequireAnon redirectTo="/reviews" navigate={nav}>
      <LoginScreen />
    </RequireAnon>
  );
}

/**
 * The single in-shell layout. Owns auth gating, the AppShell chrome, and
 * the command palette state + items. Children render inside <Outlet />
 * (which is rendered inside <AppShell>).
 */
function ShellLayout() {
  const nav = useNavigateString();
  const auth = useAuth();
  const pathname = useRouterState({ select: (s) => s.location.pathname });
  const [paletteOpen, setPaletteOpen] = useState(false);

  const openPalette = useCallback(() => setPaletteOpen(true), []);
  const closePalette = useCallback(() => setPaletteOpen(false), []);
  usePaletteShortcut(openPalette);

  const items: CommandItem[] = useMemo(() => {
    const navItems: CommandItem[] = NAV_ITEMS.map((n) => ({
      id: `nav-${n.to}`,
      label: `Go to ${n.label}`,
      description: n.to,
      run: () => nav(n.to),
    }));
    return [
      ...navItems,
      {
        id: "spike-diff",
        label: "Open diff spike",
        description: "/spike/diff",
        run: () => nav("/spike/diff"),
      },
      {
        id: "spike-review",
        label: "Open diff-as-reviewed sketch",
        description: "/spike/review",
        run: () => nav("/spike/review"),
      },
      {
        id: "sign-out",
        label: "Sign out",
        run: () => auth.signOut(),
      },
    ];
  }, [nav, auth]);

  return (
    <RequireAuth redirectTo="/login" navigate={nav}>
      <AppShell nav={NAV_ITEMS} pathname={pathname} navigate={nav} onOpenPalette={openPalette}>
        {/* Tight Suspense inside the shell so route swaps don't flash the
            full-screen LoadingScreen — the sidebar stays put, only the
            main area shows the spinner while the chunk loads. */}
        <Suspense fallback={<InShellLoading />}>
          <Outlet />
        </Suspense>
      </AppShell>
      <CommandPalette open={paletteOpen} onClose={closePalette} items={items} />
    </RequireAuth>
  );
}

function InShellLoading() {
  return (
    <div
      role="status"
      aria-live="polite"
      className="grid h-full place-items-center text-stone-400"
    >
      <span className="font-mono text-[11px] uppercase tracking-wider">loading…</span>
    </div>
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

function LoadingScreen() {
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

// ─────────────────────────────────────────────────────────────────────────────
// Hooks
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Adapter: useNavigate returns a TanStack-typed function. Our gates and
 * shell accept `(to: string) => void` so they stay decoupled from the
 * router library. This bridge does the cast in exactly one place.
 */
function useNavigateString(): (to: string) => void {
  const nav = useNavigate();
  return useCallback(
    (to: string) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      void nav({ to: to as any });
    },
    [nav],
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Icons (inlined — avoid taking an icon-library dep before A5's design pass)
// ─────────────────────────────────────────────────────────────────────────────

function InboxIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden>
      <path
        d="M2 9h3l1.5 2h3L11 9h3M2 9V5l1.5-2h9L14 5v4M2 9v3.5A.5.5 0 0 0 2.5 13h11a.5.5 0 0 0 .5-.5V9"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function FolderIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden>
      <path
        d="M2 4.5A1.5 1.5 0 0 1 3.5 3h3l1.5 2h4.5A1.5 1.5 0 0 1 14 6.5v5A1.5 1.5 0 0 1 12.5 13h-9A1.5 1.5 0 0 1 2 11.5v-7Z"
        stroke="currentColor"
        strokeWidth="1.5"
      />
    </svg>
  );
}

function ClipboardIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden>
      <rect x="3.5" y="3" width="9" height="11" rx="1" stroke="currentColor" strokeWidth="1.5" />
      <rect x="5.5" y="1.5" width="5" height="2.5" rx="0.5" stroke="currentColor" strokeWidth="1.5" />
      <path d="M5.5 7h5M5.5 10h3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

function GearIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden>
      <circle cx="8" cy="8" r="2" stroke="currentColor" strokeWidth="1.5" />
      <path
        d="M8 1v2.5M8 12.5V15M3 3l1.8 1.8M11.2 11.2 13 13M1 8h2.5M12.5 8H15M3 13l1.8-1.8M11.2 4.8 13 3"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
      />
    </svg>
  );
}
