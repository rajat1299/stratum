# Capability manifest — requirements from frontend → backend

**Owner:** Head of Frontend Engineering
**Status:** Draft 1 — proposed contract for `GET /v1/capabilities`
**Audience:** Backend tech lead, head of product

The frontend will be built against this manifest from week 2, with a mock living at `web/src/lib/capabilities.mock.ts`. The day backend ships the real endpoint, we delete the mock — but only if the shape below holds. Please push back hard if any of this is wrong; better an argument now than a refactor in week 8.

The inspiration is mirage's VFP capability layer (`typescript/packages/core/src/vfp/{types,capability,methods,skill}.ts`); the requirements below are Stratum-specific and not just a port.

---

## 1. Why we need it

Three problems the manifest solves:

1. **Graceful unsupported-state rendering.** The HTTP API already returns `501` for "operation not supported: durable-cloud route is not supported yet" and the semantic-search route doesn't exist yet. Today the UI has no way to distinguish "the server doesn't support this in this mode" from "the user lacks permission" from "this is a real bug." We render a "Feature unavailable" panel with a clean reason — or we don't show the menu item at all — based on the manifest.

2. **Conditional feature surfacing.** The console has surfaces that only make sense for some server runtimes (recovery dashboard ↔ durable-cloud; run-records ↔ V3 execution). Without a manifest, every feature flag becomes a backend constant we have to copy. The manifest lets us flag features by capability, not by version sniffing.

3. **A typed contract.** Once the manifest is stable we type-generate from it. That's the wall between us and shape drift.

---

## 2. Endpoint shape

```http
GET /v1/capabilities
```

- **Auth:** none required. The manifest must be safe to fetch without a session so the login screen can branch on it (e.g., hide "Sign in with SSO" when OIDC is off).
- **Caching:** server includes `Cache-Control: max-age=60, must-revalidate`. Frontend cache key is the response body's `revision` field; we invalidate the TanStack Query cache on revision change.
- **Status:** always `200`. Unknown server states should never produce a `5xx` from this endpoint — that's the one endpoint we rely on to *describe* server state.

---

## 3. Proposed response shape

```jsonc
{
  // Stable identifier for this manifest; changes only when the shape or values change.
  // Used as a TanStack Query key so frontends invalidate cleanly.
  "revision": "2026-05-15-1",

  // Server identity
  "server": {
    "name": "stratum",
    "version": "1.0.0",
    "build": "ab12cd34",                     // short commit
    "backend_mode": "local",                 // "local" | "durable" | "durable-cloud"
    "core_runtime": "local-state",           // "local-state" | "durable-cloud"
    "build_features": ["postgres"]           // cargo features compiled in
  },

  // Auth
  "auth": {
    "modes": ["user", "bearer", "workspace"],
    "providers": [
      { "id": "local", "label": "Local users", "default": true },
      { "id": "oidc", "label": "SSO (OIDC)", "default": false, "available": false }
    ]
  },

  // Route surface. Each route declares: implemented? requires admin? what reasons
  // it might return 501 with? The frontend uses this to enable/disable nav items
  // and to render "unsupported" reasons.
  "routes": {
    "filesystem": {
      "read":   { "available": true,  "admin": false },
      "list":   { "available": true,  "admin": false },
      "stat":   { "available": true,  "admin": false },
      "write":  { "available": true,  "admin": false, "idempotent": true },
      "delete": { "available": true,  "admin": false, "idempotent": true },
      "patch":  { "available": true,  "admin": false, "idempotent": true },
      "copy":   { "available": true,  "admin": false, "idempotent": true },
      "move":   { "available": true,  "admin": false, "idempotent": true }
    },
    "search": {
      "grep":     { "available": true,  "admin": false },
      "find":     { "available": true,  "admin": false },
      "tree":     { "available": true,  "admin": false },
      "semantic": { "available": false, "admin": false,
                    "reason": "not implemented",
                    "tracking_ref": "execution-roadmap §3" }
    },
    "vcs": {
      "log":      { "available": true,  "admin": true },
      "status":   { "available": true,  "admin": true },
      "diff":     { "available": true,  "admin": true },
      "refs":     { "available": true,  "admin": true, "idempotent": true },
      "commit":   { "available": true,  "admin": true, "idempotent": true,
                    "blocked_when": ["durable-cloud"] },
      "revert":   { "available": true,  "admin": true, "idempotent": true,
                    "blocked_when": ["durable-cloud"] },
      "recovery": { "available": true,  "admin": true,
                    "requires": ["durable-cloud"] }
    },
    "review": {
      "change_requests": { "available": true, "admin": true, "idempotent": true },
      "approvals":       { "available": true, "admin": true, "idempotent": true },
      "reviewers":       { "available": true, "admin": true, "idempotent": true },
      "comments":        { "available": true, "admin": true, "idempotent": true },
      "merge":           { "available": true, "admin": true, "idempotent": true },
      "reject":          { "available": true, "admin": true, "idempotent": true },
      "dismiss":         { "available": true, "admin": true, "idempotent": true }
    },
    "workspaces": {
      "list":         { "available": true, "admin": true },
      "create":       { "available": true, "admin": true, "idempotent": true },
      "issue_token":  { "available": true, "admin": true, "idempotent": false,
                        "reason": "secret-bearing response; idempotency replay unsafe" },
      "revoke_token": { "available": true, "admin": true, "idempotent": true }
    },
    "audit":   { "available": true,  "admin": true },
    "runs":    { "available": true,  "admin": false, "idempotent": true,
                 "execution": false,
                 "notes": "Phase-1 record only; no execution scheduler yet." }
  },

  // Diff capabilities — the frontend uses this to decide which renderers to wire.
  "diff": {
    "format": "text/v1",                       // bumps if Rust output changes
    "max_text_diff_bytes": 524288,             // 512 KiB
    "max_text_diff_cells": 4000000,
    "context_lines": 3,
    "supported_fragment_kinds": [
      "text-unified",
      "metadata-only",
      "binary",
      "too-large",
      "kind-changed"
    ],
    "json_format_available": false             // when true, we'll switch
  },

  // Protected-rule shape, so the policy editor can build a form against it.
  "protection": {
    "ref_rules": {
      "available": true,
      "required_approvals_max": 16
    },
    "path_rules": {
      "available": true,
      "required_approvals_max": 16,
      "target_ref_optional": true
    }
  },

  // Idempotency. The SDK uses this to set a sensible default and to drop the
  // header on endpoints that refuse it.
  "idempotency": {
    "header": "Idempotency-Key",
    "max_key_bytes": 255,
    "stale_pending_seconds": 60,
    "completed_retention_seconds": 86400,
    "endpoints_supported": [
      "PUT /fs/{path}",
      "PATCH /fs/{path}",
      "DELETE /fs/{path}",
      "POST /fs/{path}?op=copy|move",
      "POST /runs",
      "POST /vcs/commit",
      "POST /vcs/revert",
      "POST /vcs/refs",
      "PATCH /vcs/refs/{name}",
      "POST /protected/refs",
      "POST /protected/paths",
      "POST /change-requests",
      "POST /change-requests/{id}/approvals",
      "POST /change-requests/{id}/reviewers",
      "POST /change-requests/{id}/comments",
      "POST /change-requests/{id}/reject",
      "POST /change-requests/{id}/merge",
      "POST /change-requests/{id}/approvals/{approval_id}/dismiss",
      "POST /workspaces"
    ]
  },

  // Recovery shape (operator dashboard).
  "recovery": {
    "available": true,
    "phases": ["pre_visibility", "post_cas", "fs_mutations", "object_cleanup"],
    "destructive_cleanup_enabled": false,
    "scheduler_present": true
  },

  // Limits — the frontend renders these inline ("file is too large to upload here, max 10 MB")
  "limits": {
    "max_file_size_bytes": 10485760,
    "max_inodes": 1000000,
    "max_depth": 256,
    "audit_default_limit": 100,
    "audit_max_limit": 1000,
    "log_max_limit": 1000
  },

  // UI hints from the server. Optional. The frontend ignores any unknown hint
  // and renders defaults; this is purely for graceful upgrades without a frontend
  // release.
  "hints": {
    "banner": null,                            // { "kind": "info"|"warn", "text": "..." } | null
    "branding": null,                          // { "workspace_label": "Acme", "logo_url": "..." } | null
    "support_url": "https://stratum.dev/support"
  }
}
```

---

## 4. The five things we need from this contract

1. **`server.backend_mode` and `server.core_runtime`** — drives the "Recovery" nav visibility and the "merge is read-only on this server" guardrail.
2. **`routes[group][verb].available`** — drives every menu item, every disabled button, every empty state. Without it we hard-code feature flags.
3. **`routes[group][verb].admin`** — lets us hide admin routes from non-admin users before they get a `403`.
4. **`diff.format` + `diff.supported_fragment_kinds`** — our diff parser asserts the format at startup. If you ship JSON diffs (`diff.json_format_available: true` + `Accept: application/json` on `/vcs/diff`), we add a JSON code path without touching the text path.
5. **`hints.banner`** — operator-friendly way to ship "Stratum is in maintenance until 14:00 UTC" without us cutting a release.

---

## 5. What we'd like to *not* see in this manifest

- **No secrets, ever.** No API keys, no database URLs, no R2 credentials. We won't log it, but unauthenticated callers can hit `/v1/capabilities` and that's a perimeter we don't want exposed.
- **No per-user fields.** The manifest is server-shaped, not user-shaped. User-shaped data goes in `/auth/login` response.
- **No raw `RepoId`s.** Workspace context is plumbed through headers; the manifest shouldn't leak repo identities a caller doesn't already have.

---

## 6. Versioning + migration story

- `revision` is the simplest possible cache key: ISO date + counter. Bumps whenever any field changes shape or semantics, never just because we added a `hints.banner`.
- We treat additive changes as compatible (new fields under existing groups). Removals or semantic changes require a coordinated frontend release; backend should mark them in the changelog under "manifest revision X breaking."
- If we ever ship a v2 manifest (e.g., a totally different shape), the v1 endpoint remains live for at least 60 days at `GET /v1/capabilities` while we cut over the frontend at `GET /v2/capabilities`.

---

## 7. Asks of backend (timing)

| Ask | Why | When |
|---|---|---|
| Lock the shape above (or push back with edits) | Without this, every feature flag is duct tape. | Within 1 week — week-2 unblock. |
| Ship the endpoint behind a feature flag, returning the static manifest above | Lets us delete the mock immediately. | Within 3 weeks. |
| Land `routes.*.available` consistency with the actual route table — i.e. if `/vcs/recovery` 404s, manifest must say `available: false` | Otherwise our menu shows things that don't exist. | Same PR as the endpoint. |
| Optional but valuable: `Accept: application/json` on `/vcs/diff` returning a structured fragment array | We delete our parser. Smaller surface to maintain. | Anytime in the 90 days. |

---

## 8. Alignment with backend's parallel plan

Backend has independently drafted `docs/plans/2026-05-15-capability-manifest-endpoint.md` for the same endpoint. Reviewed: **the proposed shape is identical** at the group level (`revision`, `server`, `auth`, `routes`, `diff`, `protection`, `idempotency`, `recovery`, `limits`, `hints`), and our perimeter guidance matches theirs verbatim — "no paths, repo ids, tokens, request bodies, DB URLs, R2 endpoints, object keys, raw backend errors, commit messages, or per-user fields."

Backend's plan is going one better than this doc on the SDK story: they intend to ship deterministic JSON fixtures at `sdk/contracts/capabilities.v1.json` and `…durable-cloud.json`, plus `getCapabilities()` helpers in the TS and Python SDKs. We'll consume those fixtures directly as `web/src/lib/capabilities.mock.ts` so the manifest day we go live is a one-line import change, not a rewrite.

**Open items I'd like backend to confirm in their plan, before they write the Rust struct:**

1. **Per-verb route shape.** Section §3 above proposes each route as `{ available, admin, idempotent?, reason?, tracking_ref?, requires?, blocked_when? }`. Their plan lists the groups but not the per-verb fields — please confirm those fields make it in (or push back).
2. **`hints.banner` shape.** I proposed `{ kind: "info"|"warn", text: string } | null` for op-level messaging without a frontend release. Please commit to that shape, or tell me what you'd prefer.
3. **`server.backend_mode` value enum.** We need `"local" | "durable" | "durable-cloud"` (verbatim from §3) — please don't add a fourth value without a coordinated PR.

If 1–3 are agreed in your plan PR, we will delete this doc's §3 sample and reference `sdk/contracts/capabilities.v1.json` directly.

---

*— Frontend Engineering*
