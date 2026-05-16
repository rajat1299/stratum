/**
 * RepositoryPlaceholder — file tree + commit log + diff per ref.
 * Replaced by the full repo browser in Phase B (slices B1–B4).
 */

import { Placeholder } from "./Placeholder.tsx";

export function RepositoryPlaceholder() {
  return (
    <Placeholder
      phase="Phase B — repo browser"
      title="Repository."
      description="Branch picker on the left, file tree, file content view with MIME-aware rendering, commit timeline. Lands across slices B1 through B4."
    />
  );
}
