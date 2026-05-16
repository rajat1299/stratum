/**
 * LoginScreen — the un-authenticated entry to the console.
 *
 * Three modes, mapping 1:1 to the SDK's StratumAuth union:
 *   - User      Username field (local users via /auth/login).
 *   - Bearer    Agent token field. Bytes start with sk_strat_.
 *   - Workspace Workspace id + workspace token, behind "More options"
 *               since reviewers rarely sign in this way (it's an agent
 *               session shape; operators issue tokens, agents consume).
 *
 * We don't pre-verify against /auth/login or /health in v1 — credentials
 * are persisted on submit and the first downstream query surfaces a
 * bad-auth error. Pre-flight verification lands in Phase A4 once the
 * TanStack Query layer is in place to host the cache + error handling.
 *
 * On successful submit the router redirect (see web/src/router.tsx)
 * sends the user to /reviews.
 */

import { type FormEvent, useState } from "react";
import { useAuth } from "../lib/auth.tsx";

type Mode = "user" | "bearer" | "workspace";

export function LoginScreen() {
  const auth = useAuth();
  const [mode, setMode] = useState<Mode>("user");
  const [showMore, setShowMore] = useState(false);

  const [username, setUsername] = useState("");
  const [token, setToken] = useState("");
  const [wsId, setWsId] = useState("");
  const [wsToken, setWsToken] = useState("");

  function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    if (mode === "user" && username.trim()) auth.signInAsUser(username);
    else if (mode === "bearer" && token.trim()) auth.signInWithBearer(token);
    else if (mode === "workspace" && wsId.trim() && wsToken.trim()) {
      auth.signInWithWorkspace(wsId, wsToken);
    }
  }

  const canSubmit =
    (mode === "user" && username.trim().length > 0) ||
    (mode === "bearer" && token.trim().length > 0) ||
    (mode === "workspace" && wsId.trim().length > 0 && wsToken.trim().length > 0);

  return (
    <div className="grid min-h-screen place-items-center bg-stone-50 px-4 py-12">
      <main className="w-full max-w-sm" aria-labelledby="login-title">
        <header className="mb-8 flex items-center gap-2.5">
          <BrandMark />
          <span className="text-[15px] font-semibold tracking-tight text-stone-900">stratum</span>
        </header>

        <h1 id="login-title" className="text-[24px] font-medium leading-tight tracking-tight text-stone-900">
          Sign in to your workspace.
        </h1>
        <p className="mb-7 mt-1 font-serif text-[14px] italic text-stone-500">
          Local users today. SSO when OIDC ships.
        </p>

        <div
          role="tablist"
          aria-label="Sign-in method"
          className="mb-5 inline-flex rounded-md bg-stone-200/70 p-0.5"
        >
          <TabBtn label="User" active={mode === "user"} onClick={() => setMode("user")} />
          <TabBtn label="Bearer token" active={mode === "bearer"} onClick={() => setMode("bearer")} />
        </div>

        <form onSubmit={submit} className="flex flex-col gap-3" noValidate>
          {mode === "user" && (
            <Field
              id="username"
              label="Username"
              hint="Your stratum user, e.g. alice"
              value={username}
              onChange={setUsername}
              autoFocus
              autoComplete="username"
            />
          )}

          {mode === "bearer" && (
            <Field
              id="bearer"
              label="Agent token"
              hint="Begins with sk_strat_. Issued by addagent."
              type="password"
              value={token}
              onChange={setToken}
              autoFocus
              autoComplete="off"
              spellCheck={false}
            />
          )}

          {mode === "workspace" && (
            <>
              <Field
                id="ws-id"
                label="Workspace id"
                hint="UUID returned by POST /workspaces."
                value={wsId}
                onChange={setWsId}
                autoFocus
                autoComplete="off"
                spellCheck={false}
              />
              <Field
                id="ws-token"
                label="Workspace token"
                hint="Returned once by POST /workspaces/:id/tokens."
                type="password"
                value={wsToken}
                onChange={setWsToken}
                autoComplete="off"
                spellCheck={false}
              />
            </>
          )}

          <button
            type="submit"
            disabled={!canSubmit}
            className="mt-3 rounded-md bg-stone-900 px-4 py-2.5 text-[14px] font-medium text-stone-50 transition enabled:hover:bg-stone-800 disabled:cursor-not-allowed disabled:opacity-40"
          >
            Continue
          </button>
        </form>

        <details
          className="mt-8 text-[12.5px] text-stone-500"
          open={showMore || mode === "workspace"}
          onToggle={(e) => setShowMore((e.target as HTMLDetailsElement).open)}
        >
          <summary className="cursor-pointer select-none rounded-sm py-1 outline-none hover:text-stone-700 focus-visible:ring-2 focus-visible:ring-stone-300">
            More options
          </summary>
          <button
            type="button"
            onClick={() => setMode("workspace")}
            className={`mt-2 block w-full rounded-sm px-2 py-1 text-left text-[12.5px] ${
              mode === "workspace" ? "bg-stone-100 text-stone-900" : "text-stone-600 hover:bg-stone-100 hover:text-stone-900"
            }`}
          >
            Sign in with a workspace token →
          </button>
        </details>

        <footer className="mt-10 text-center font-mono text-[10.5px] text-stone-400">
          stratum console · v0
        </footer>
      </main>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Atoms
// ─────────────────────────────────────────────────────────────────────────────

function TabBtn({
  label,
  active,
  onClick,
}: {
  readonly label: string;
  readonly active: boolean;
  readonly onClick: () => void;
}) {
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      onClick={onClick}
      className={`rounded-[5px] px-3 py-1 text-[12.5px] font-medium transition ${
        active
          ? "bg-stone-50 text-stone-900 shadow-sm ring-1 ring-stone-200"
          : "text-stone-600 hover:text-stone-900"
      }`}
    >
      {label}
    </button>
  );
}

interface FieldProps {
  readonly id: string;
  readonly label: string;
  readonly hint: string;
  readonly value: string;
  readonly onChange: (value: string) => void;
  readonly type?: "text" | "password";
  readonly autoFocus?: boolean;
  readonly autoComplete?: string;
  readonly spellCheck?: boolean;
}

function Field({ id, label, hint, value, onChange, type = "text", autoFocus, autoComplete, spellCheck }: FieldProps) {
  const hintId = `${id}-hint`;
  return (
    <div className="flex flex-col gap-1">
      <label htmlFor={id} className="text-[12.5px] font-medium text-stone-800">
        {label}
      </label>
      <input
        id={id}
        type={type}
        value={value}
        onChange={(e) => onChange(e.currentTarget.value)}
        autoFocus={autoFocus}
        autoComplete={autoComplete}
        spellCheck={spellCheck}
        aria-describedby={hintId}
        className="rounded-md border border-stone-300 bg-white px-3 py-2 text-[14px] text-stone-900 outline-none transition placeholder:text-stone-400 focus:border-stone-500 focus:ring-2 focus:ring-stone-200"
      />
      <p id={hintId} className="font-mono text-[10.5px] text-stone-500">
        {hint}
      </p>
    </div>
  );
}

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
