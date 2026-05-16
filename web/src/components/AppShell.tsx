/**
 * AppShell — the chrome the in-app routes render inside.
 *
 *   ┌──────────────┬─────────────────────────────────────────────┐
 *   │   sidebar    │   Reviews                ⌘K        AK ▾    │  top bar
 *   ├──────────────┼─────────────────────────────────────────────┤
 *   │ ◇ stratum    │                                              │
 *   │              │                                              │
 *   │ Reviews   3  │             <Outlet />                       │
 *   │ Repository   │                                              │
 *   │ Audit        │                                              │
 *   │ Settings     │                                              │
 *   │              │                                              │
 *   │ AK alice ●   │                                              │
 *   └──────────────┴─────────────────────────────────────────────┘
 *
 * Renders by itself (no children prop) so it can be used as the
 * component of a layout route. Children are provided via TanStack
 * Router's <Outlet />.
 *
 * Auth is enforced at the layout level (RequireAuth wraps Outlet there);
 * AppShell itself assumes an authed session and reads it for the user
 * menu.
 *
 * Wiring to the router (palette toggle, etc.) is intentionally exposed
 * as a context — keeps AppShell pure-presentational and lets the
 * palette stub live in its own module.
 */

import { type ReactNode, createContext, useContext, useMemo, useState } from "react";
import { useAuth } from "../lib/auth.tsx";

// ─────────────────────────────────────────────────────────────────────────────
// Public types
// ─────────────────────────────────────────────────────────────────────────────

export interface NavItem {
  readonly to: string;
  readonly label: string;
  readonly icon: ReactNode;
  /** Optional count rendered as a small pill next to the label. */
  readonly badge?: number;
}

export interface AppShellProps {
  readonly nav: readonly NavItem[];
  /** Current pathname for active-state highlight. */
  readonly pathname: string;
  /** Routes navigation when a nav item or breadcrumb is clicked. */
  readonly navigate: (to: string) => void;
  /** Renderer for the main content area. */
  readonly children: ReactNode;
  /** Trigger that opens the command palette. AppShell renders the button; the
   *  parent owns the actual palette state. */
  readonly onOpenPalette?: () => void;
}

// ─────────────────────────────────────────────────────────────────────────────
// Shell
// ─────────────────────────────────────────────────────────────────────────────

export function AppShell({ nav, pathname, navigate, children, onOpenPalette }: AppShellProps) {
  const ctx = useMemo(() => ({ navigate, onOpenPalette }), [navigate, onOpenPalette]);
  return (
    <AppShellCtx.Provider value={ctx}>
      <div className="grid h-screen grid-cols-[200px_1fr] grid-rows-[48px_1fr] bg-stone-50">
        <Sidebar nav={nav} pathname={pathname} navigate={navigate} />
        <TopBar pathname={pathname} nav={nav} onOpenPalette={onOpenPalette} />
        <main className="col-start-2 row-start-2 overflow-y-auto bg-stone-50">{children}</main>
      </div>
    </AppShellCtx.Provider>
  );
}

const AppShellCtx = createContext<{
  readonly navigate: (to: string) => void;
  readonly onOpenPalette: (() => void) | undefined;
} | null>(null);

/** Inside the shell, use this if you need to programmatically open the palette. */
export function useAppShell() {
  const ctx = useContext(AppShellCtx);
  if (!ctx) throw new Error("useAppShell must be used inside <AppShell>");
  return ctx;
}

// ─────────────────────────────────────────────────────────────────────────────
// Sidebar
// ─────────────────────────────────────────────────────────────────────────────

function Sidebar({
  nav,
  pathname,
  navigate,
}: {
  readonly nav: readonly NavItem[];
  readonly pathname: string;
  readonly navigate: (to: string) => void;
}) {
  const auth = useAuth();
  const sessionLabel =
    auth.state.status === "authed"
      ? auth.state.credentials.type === "user"
        ? auth.state.credentials.username
        : auth.state.credentials.type === "workspace"
          ? "workspace"
          : "agent"
      : "—";

  return (
    <aside className="row-span-2 row-start-1 flex flex-col border-r border-stone-200 bg-white">
      <div className="flex h-12 items-center gap-2.5 border-b border-stone-200 px-4">
        <BrandMark />
        <span className="text-[14px] font-semibold tracking-tight text-stone-900">stratum</span>
      </div>

      <nav aria-label="Primary" className="flex-1 px-2 py-3">
        <ul className="flex flex-col gap-0.5">
          {nav.map((item) => {
            const active = isActive(pathname, item.to);
            return (
              <li key={item.to}>
                <button
                  type="button"
                  onClick={() => navigate(item.to)}
                  aria-current={active ? "page" : undefined}
                  className={`group flex w-full items-center gap-2.5 rounded-md px-2.5 py-1.5 text-left text-[13px] font-medium transition-colors ${
                    active
                      ? "bg-orange-50 text-orange-800"
                      : "text-stone-700 hover:bg-stone-100 hover:text-stone-900"
                  }`}
                >
                  <span
                    aria-hidden
                    className={`shrink-0 ${active ? "text-orange-600" : "text-stone-400 group-hover:text-stone-600"}`}
                  >
                    {item.icon}
                  </span>
                  <span className="flex-1 truncate">{item.label}</span>
                  {item.badge !== undefined && item.badge > 0 && (
                    <span
                      className={`min-w-[18px] rounded-full px-1.5 text-center font-mono text-[10.5px] tabular-nums ${
                        active ? "bg-orange-100 text-orange-800" : "bg-stone-100 text-stone-600"
                      }`}
                    >
                      {item.badge}
                    </span>
                  )}
                </button>
              </li>
            );
          })}
        </ul>
      </nav>

      <div className="flex items-center gap-2 border-t border-stone-200 px-3 py-2.5">
        <Avatar label={sessionLabel} />
        <div className="min-w-0 flex-1">
          <div className="truncate text-[12px] font-medium text-stone-800">{sessionLabel}</div>
          <div className="font-mono text-[10.5px] text-stone-500">signed in</div>
        </div>
        <span aria-hidden className="h-2 w-2 rounded-full bg-green-500" />
      </div>
    </aside>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Top bar
// ─────────────────────────────────────────────────────────────────────────────

function TopBar({
  pathname,
  nav,
  onOpenPalette,
}: {
  readonly pathname: string;
  readonly nav: readonly NavItem[];
  readonly onOpenPalette: (() => void) | undefined;
}) {
  return (
    <header className="col-start-2 row-start-1 flex items-center gap-3 border-b border-stone-200 bg-white/85 px-4 backdrop-blur">
      <Breadcrumb pathname={pathname} nav={nav} />
      <div className="flex-1" />
      <button
        type="button"
        onClick={onOpenPalette}
        disabled={!onOpenPalette}
        aria-label="Open command palette"
        className="hidden items-center gap-2 rounded-md border border-stone-200 bg-stone-50 px-2.5 py-1 text-[12px] text-stone-500 transition hover:border-stone-300 hover:text-stone-700 disabled:cursor-default disabled:opacity-50 sm:flex"
      >
        <span className="font-mono">Search…</span>
        <span className="rounded border border-stone-300 px-1 font-mono text-[10px] text-stone-500">⌘K</span>
      </button>
      <UserMenu />
    </header>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Breadcrumb
// ─────────────────────────────────────────────────────────────────────────────

export function Breadcrumb({
  pathname,
  nav,
}: {
  readonly pathname: string;
  readonly nav: readonly NavItem[];
}) {
  // For Phase A3, the breadcrumb is just the current section name. Nested
  // segments (e.g. Reviews › cr-42 › §3.2) land in Phase D when there's
  // real route data; we add a `segments` prop then without changing callers.
  const item = nav.find((n) => isActive(pathname, n.to));
  return (
    <nav aria-label="Breadcrumb" className="flex items-center gap-2 text-[13px] font-medium text-stone-800">
      <span className="text-stone-500">{item?.label ?? "Stratum"}</span>
    </nav>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// User menu
// ─────────────────────────────────────────────────────────────────────────────

function UserMenu() {
  const auth = useAuth();
  const [open, setOpen] = useState(false);

  if (auth.state.status !== "authed") return null;
  const cred = auth.state.credentials;
  const label = cred.type === "user" ? cred.username : cred.type;

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        aria-haspopup="menu"
        aria-expanded={open}
        className="flex items-center gap-2 rounded-md px-1.5 py-0.5 text-[12.5px] text-stone-700 transition hover:bg-stone-100"
      >
        <Avatar label={label} size={26} />
        <svg width="9" height="9" viewBox="0 0 16 16" aria-hidden>
          <path d="M3 6l5 5 5-5" stroke="currentColor" strokeWidth={1.5} fill="none" strokeLinecap="round" />
        </svg>
      </button>
      {open && (
        <>
          <button
            type="button"
            aria-label="Close menu"
            onClick={() => setOpen(false)}
            className="fixed inset-0 z-10 cursor-default"
          />
          <div
            role="menu"
            className="absolute right-0 z-20 mt-1 w-44 overflow-hidden rounded-md border border-stone-200 bg-white shadow-lg"
          >
            <div className="border-b border-stone-100 px-3 py-2 text-[12px] text-stone-600">
              Signed in as <span className="font-medium text-stone-900">{label}</span>
            </div>
            <button
              type="button"
              role="menuitem"
              onClick={() => {
                setOpen(false);
                auth.signOut();
              }}
              className="block w-full px-3 py-2 text-left text-[12.5px] text-stone-700 transition hover:bg-stone-50 hover:text-stone-900"
            >
              Sign out
            </button>
          </div>
        </>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Atoms
// ─────────────────────────────────────────────────────────────────────────────

function BrandMark() {
  return (
    <span className="inline-grid h-[18px] w-[18px] grid-cols-2 gap-[1.5px]" aria-hidden>
      <span className="rounded-[1.5px] bg-stone-900" />
      <span className="rounded-[1.5px] bg-orange-600" />
      <span className="rounded-[1.5px] bg-stone-900" />
      <span className="rounded-[1.5px] bg-stone-900 opacity-40" />
    </span>
  );
}

function Avatar({ label, size = 22 }: { readonly label: string; readonly size?: number }) {
  const initials = label.slice(0, 2).toUpperCase();
  return (
    <span
      aria-hidden
      className="grid place-items-center rounded-full bg-stone-900 font-mono text-[10px] font-medium uppercase text-stone-50"
      style={{ width: size, height: size }}
    >
      {initials}
    </span>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function isActive(pathname: string, to: string): boolean {
  if (to === "/") return pathname === "/";
  return pathname === to || pathname.startsWith(`${to}/`);
}
