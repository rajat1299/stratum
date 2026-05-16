/**
 * ReviewsPlaceholder — the post-login landing surface.
 *
 * Replaced by the full reviewer console in Phase D. For now this is
 * a calm card that lives inside the AppShell.
 */

import { Placeholder } from "./Placeholder.tsx";

export function ReviewsPlaceholder() {
  return (
    <Placeholder
      phase="Phase D — the daily driver"
      title="No change requests to review yet."
      description="When agents commit to a branch you're a reviewer on, they show up here with a diff, rationale, and one-click approve."
    >
      <a
        href="/spike/diff"
        className="inline-flex items-center gap-2 rounded-md border border-stone-300 px-3 py-1.5 text-[12.5px] font-medium text-stone-700 transition hover:border-stone-500 hover:text-stone-900"
      >
        Open diff spike <span aria-hidden>→</span>
      </a>
    </Placeholder>
  );
}
