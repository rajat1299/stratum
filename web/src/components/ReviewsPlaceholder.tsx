/**
 * ReviewsPlaceholder — the post-login landing surface.
 *
 * Replaced by the full reviewer console in Phase D (the 4-week slice
 * cadence the roadmap calls the "daily driver"). This placeholder
 * exists so the auth flow can be tested end-to-end:
 *
 *   /  → /login (anon) or /reviews (authed)
 *   /login successful submit → /reviews
 *
 * Also serves as the canonical sign-out surface during the in-between
 * weeks. We don't render the sign-out button inside the spike, so this
 * is the only place a developer can reset auth state without devtools.
 */

import { useAuth } from "../lib/auth.tsx";

export function ReviewsPlaceholder() {
  const auth = useAuth();
  const credentials = auth.state.status === "authed" ? auth.state.credentials : null;

  return (
    <div className="min-h-screen bg-stone-50 px-8 py-16">
      <div className="mx-auto max-w-2xl">
        <h1 className="text-[26px] font-medium leading-tight tracking-tight text-stone-900">
          Reviews — the daily driver lands in Phase D.
        </h1>
        <p className="mt-1 font-serif text-[14px] italic text-stone-500">
          For now: confirm the auth flow round-trips and the SDK client mints with the
          right credentials.
        </p>

        <section className="mt-10 rounded-md border border-stone-200 bg-white p-5 shadow-sm">
          <div className="mb-3 font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
            Current session
          </div>
          {credentials ? (
            <dl className="grid grid-cols-[100px_1fr] gap-y-2 text-[13px]">
              <dt className="font-mono text-stone-500">type</dt>
              <dd className="font-mono text-stone-900">{credentials.type}</dd>
              {credentials.type === "user" && (
                <>
                  <dt className="font-mono text-stone-500">username</dt>
                  <dd className="font-mono text-stone-900">{credentials.username}</dd>
                </>
              )}
              {credentials.type === "bearer" && (
                <>
                  <dt className="font-mono text-stone-500">token</dt>
                  <dd className="truncate font-mono text-stone-700">
                    {mask(credentials.token)}
                  </dd>
                </>
              )}
              {credentials.type === "workspace" && (
                <>
                  <dt className="font-mono text-stone-500">workspace_id</dt>
                  <dd className="font-mono text-stone-900">{credentials.workspaceId}</dd>
                  <dt className="font-mono text-stone-500">token</dt>
                  <dd className="truncate font-mono text-stone-700">
                    {mask(credentials.workspaceToken)}
                  </dd>
                </>
              )}
            </dl>
          ) : (
            <p className="font-serif text-[13px] italic text-stone-500">
              (no session — you shouldn't see this; the auth gate should have redirected)
            </p>
          )}
        </section>

        <section className="mt-6 flex items-center gap-3">
          <a
            href="/spike/diff"
            className="rounded-md border border-stone-300 px-3 py-1.5 text-[12.5px] font-medium text-stone-700 transition hover:border-stone-500 hover:text-stone-900"
          >
            Open diff spike →
          </a>
          <button
            type="button"
            onClick={() => auth.signOut()}
            className="rounded-md border border-stone-300 px-3 py-1.5 text-[12.5px] font-medium text-stone-700 transition hover:border-stone-500 hover:text-stone-900"
          >
            Sign out
          </button>
        </section>
      </div>
    </div>
  );
}

function mask(value: string): string {
  if (value.length <= 8) return "•".repeat(value.length);
  return `${value.slice(0, 4)}${"•".repeat(Math.max(value.length - 8, 4))}${value.slice(-4)}`;
}
