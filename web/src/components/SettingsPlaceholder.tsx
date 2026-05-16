/**
 * SettingsPlaceholder — workspaces + tokens + protected rules + recovery.
 *
 * Lands across slices E2–E4. For now we surface session diagnostics
 * (moved here from the original ReviewsPlaceholder) so a developer
 * can confirm what credentials the SDK is minting.
 */

import { useAuth } from "../lib/auth.tsx";
import { Placeholder } from "./Placeholder.tsx";

export function SettingsPlaceholder() {
  const auth = useAuth();
  const credentials = auth.state.status === "authed" ? auth.state.credentials : null;

  return (
    <Placeholder
      phase="Phase E — operator surfaces"
      title="Settings."
      description="Workspaces, tokens, protected rules, recovery dashboard. Lands across slices E2–E4."
    >
      <section className="rounded-md border border-stone-200 bg-white p-5 shadow-sm">
        <div className="mb-3 font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
          Current session
        </div>
        {credentials ? (
          <dl className="grid grid-cols-[120px_1fr] gap-y-2 text-[13px]">
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
                <dd className="truncate font-mono text-stone-700">{mask(credentials.token)}</dd>
              </>
            )}
            {credentials.type === "workspace" && (
              <>
                <dt className="font-mono text-stone-500">workspace_id</dt>
                <dd className="font-mono text-stone-900">{credentials.workspaceId}</dd>
                <dt className="font-mono text-stone-500">token</dt>
                <dd className="truncate font-mono text-stone-700">{mask(credentials.workspaceToken)}</dd>
              </>
            )}
          </dl>
        ) : (
          <p className="font-serif text-[13px] italic text-stone-500">
            (no session — you shouldn't see this; the auth gate should have redirected)
          </p>
        )}
      </section>
    </Placeholder>
  );
}

function mask(value: string): string {
  if (value.length <= 8) return "•".repeat(value.length);
  return `${value.slice(0, 4)}${"•".repeat(Math.max(value.length - 8, 4))}${value.slice(-4)}`;
}
