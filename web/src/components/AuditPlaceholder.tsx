/**
 * AuditPlaceholder — admin-only audit event stream.
 * Replaced by the full audit panel in Phase E (slice E1).
 */

import { Placeholder } from "./Placeholder.tsx";

export function AuditPlaceholder() {
  return (
    <Placeholder
      phase="Phase E — operator surfaces"
      title="Audit."
      description="Every read, write, commit, and policy decision the server emitted, filterable by actor and resource. Admin-gated."
    />
  );
}
