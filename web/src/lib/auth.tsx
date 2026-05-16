/**
 * Auth — session state for the console.
 *
 * v1 supports the three Stratum auth modes the SDK already understands:
 *
 *   - "user"      Authorization: User <username>      (local users via /auth/login)
 *   - "bearer"    Authorization: Bearer <token>        (agent tokens via addagent)
 *   - "workspace" Authorization: Bearer <ws-token>    + X-Stratum-Workspace
 *                                                      (mounted workspace sessions)
 *
 * The state machine is intentionally small:
 *
 *   "loading"  → checking storage on first mount
 *   "anon"     → no session; show login screen
 *   "authed"   → SDK auth credentials are usable
 *
 * OIDC will land later as a fourth provider; the SDK's StratumAuth shape is
 * the contract, so adding OIDC means swapping the credential source, not
 * touching consumers of useAuth().
 *
 * Storage is pluggable so tests don't reach for localStorage. In prod we
 * write to localStorage under a single namespaced key. Workspace tokens
 * are stored as plain text — the same security posture as bearer tokens
 * in any other dev console (Vercel, Supabase, PlanetScale). When OIDC
 * lands, sessions move to httpOnly cookies and this storage goes away.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import type { StratumAuth } from "@stratum/sdk";

const STORAGE_KEY = "stratum.auth.v1";

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

export type AuthState =
  | { readonly status: "loading" }
  | { readonly status: "anon" }
  | { readonly status: "authed"; readonly credentials: StratumAuth };

export interface AuthApi {
  readonly state: AuthState;
  /** Sign in with a local user. The console will call /auth/login first to verify. */
  signInAsUser(username: string): void;
  /** Sign in with an agent bearer token. */
  signInWithBearer(token: string): void;
  /** Sign in with a workspace bearer token + workspace id. */
  signInWithWorkspace(workspaceId: string, workspaceToken: string): void;
  /** Drop the session. */
  signOut(): void;
}

/**
 * Minimal pluggable storage. Defaults to localStorage in the browser and to
 * a no-op when storage isn't available (SSR, sandboxed iframe).
 */
export interface AuthStorage {
  read(): StratumAuth | null;
  write(value: StratumAuth | null): void;
}

// ─────────────────────────────────────────────────────────────────────────────
// Storage
// ─────────────────────────────────────────────────────────────────────────────

export function localStorageAuthStorage(): AuthStorage {
  return {
    read() {
      if (typeof localStorage === "undefined") return null;
      try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (!raw) return null;
        return parseStoredAuth(JSON.parse(raw));
      } catch {
        return null;
      }
    },
    write(value) {
      if (typeof localStorage === "undefined") return;
      try {
        if (value === null) localStorage.removeItem(STORAGE_KEY);
        else localStorage.setItem(STORAGE_KEY, JSON.stringify(value));
      } catch {
        // quota exceeded or storage disabled — fail silently; the in-memory
        // state still works for this session.
      }
    },
  };
}

/** In-memory storage for tests. */
export function memoryAuthStorage(initial: StratumAuth | null = null): AuthStorage {
  let value = initial;
  return {
    read() {
      return value;
    },
    write(next) {
      value = next;
    },
  };
}

/**
 * Validate something we read out of storage. Anything that doesn't match
 * one of the three SDK shapes returns `null` — the same fail-closed posture
 * as parseBanner. We don't trust untyped JSON even from our own origin.
 */
export function parseStoredAuth(value: unknown): StratumAuth | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const v = value as Record<string, unknown>;
  switch (v["type"]) {
    case "user":
      return typeof v["username"] === "string" && v["username"].length > 0
        ? { type: "user", username: v["username"] }
        : null;
    case "bearer":
      return typeof v["token"] === "string" && v["token"].length > 0
        ? { type: "bearer", token: v["token"] }
        : null;
    case "workspace":
      return typeof v["workspaceId"] === "string" &&
        typeof v["workspaceToken"] === "string" &&
        v["workspaceId"].length > 0 &&
        v["workspaceToken"].length > 0
        ? {
            type: "workspace",
            workspaceId: v["workspaceId"],
            workspaceToken: v["workspaceToken"],
          }
        : null;
    default:
      return null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Context + Provider
// ─────────────────────────────────────────────────────────────────────────────

const AuthContext = createContext<AuthApi | null>(null);

export interface AuthProviderProps {
  readonly children: ReactNode;
  /** Inject storage in tests; defaults to localStorage. */
  readonly storage?: AuthStorage;
}

export function AuthProvider({ children, storage }: AuthProviderProps) {
  const store = useMemo(() => storage ?? localStorageAuthStorage(), [storage]);
  const [state, setState] = useState<AuthState>({ status: "loading" });

  // Hydrate once on mount. We use an effect (not lazy initializer) so SSR
  // and StrictMode-double-mount produce a deterministic "loading → result"
  // transition that consumers can render against.
  useEffect(() => {
    const stored = store.read();
    setState(stored ? { status: "authed", credentials: stored } : { status: "anon" });
  }, [store]);

  const persist = useCallback(
    (credentials: StratumAuth | null) => {
      store.write(credentials);
      setState(credentials ? { status: "authed", credentials } : { status: "anon" });
    },
    [store],
  );

  const api = useMemo<AuthApi>(
    () => ({
      state,
      signInAsUser(username) {
        const trimmed = username.trim();
        if (!trimmed) return;
        persist({ type: "user", username: trimmed });
      },
      signInWithBearer(token) {
        const trimmed = token.trim();
        if (!trimmed) return;
        persist({ type: "bearer", token: trimmed });
      },
      signInWithWorkspace(workspaceId, workspaceToken) {
        const id = workspaceId.trim();
        const tok = workspaceToken.trim();
        if (!id || !tok) return;
        persist({ type: "workspace", workspaceId: id, workspaceToken: tok });
      },
      signOut() {
        persist(null);
      },
    }),
    [state, persist],
  );

  return <AuthContext.Provider value={api}>{children}</AuthContext.Provider>;
}

/**
 * Read the auth API. Throws when used outside <AuthProvider> so test failures
 * point at a real misconfiguration rather than a silent `null` deref.
 */
export function useAuth(): AuthApi {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used inside <AuthProvider>");
  return ctx;
}
