# Private Beta Contract

Status: 2026-06-08. Capability manifest revision: `2026-06-08-1`.

This contract freezes what Stratum supports for private beta. It is grounded in
`sdk/contracts/capabilities.v1.json`,
`sdk/contracts/capabilities.v1.durable-cloud.json`, and the
`/v1/capabilities` route.

## Golden Path Summary

The local golden path is currently the only complete end-to-end path. It uses
the local-state runtime, the local CLI for setup, `stratum-server` as the HTTP
API, and the local review/change-request routes for the beta review loop:
workspace setup, scoped token, agent edit, change request, diff review,
approval or rejection, merge, audit trail, and revert.

Hosted durable is a narrower preview. Its private-beta admin posture is an
operator-provisioned, repo-bound hosted admin bearer: a workspace bearer token
whose durable principal is active and root/wheel-scoped for the matching repo.
That admin bearer can provision and manage hosted workspaces and scoped agent
tokens. It is separate from normal scoped agent tokens, which can only edit
within their declared prefixes and session refs. Hosted durable is not a
hosted admin product yet.

## Support Matrix

| Surface | Local demo | Hosted durable preview |
|---|---|---|
| Runtime | `local-state` | `durable-cloud` |
| Complete beta path | Yes. This is the supported end-to-end demo path. | No. Preview route surface only. |
| Workspace source | Local Stratum state exposed as `/` through the HTTP workspace API. | Durable core state exposed as `/` for a repo-bound hosted workspace. |
| Required setup | Create local users, agents, directories, and demo content. | Operator creates the repo context, hosted admin bearer, workspace, target agent identity, scoped agent token, and session-ref. |
| Client path | CLI setup plus HTTP API, with `stratumctl` as a thin client where available. | HTTP API or `stratumctl`-style calls with explicit workspace, repo, and session context. |
| Workspace lifecycle | Workspace list/create and token issue/revoke routes are present in local-state. | Workspace list/create/get and token issue/revoke routes are available through the repo-bound durable admin path. |
| Audit listing | Supported for user-admin sessions only. Bearer tokens are rejected. | Audit listing is unsupported right now. |
| Execution | Not part of the beta contract. Default capabilities report `/execute` unavailable. | Unsupported. |
| Semantic search | Unsupported in the beta contract. Local capabilities report it unavailable. | Unsupported in the checked-in durable manifest because the search index is unavailable. |
| MCP/FUSE | Not the private-beta golden path. | Durable MCP and durable FUSE are not supported. |

## Auth Posture

| Auth area | Local demo | Hosted durable preview |
|---|---|---|
| Advertised auth modes | User, bearer, and workspace. | Workspace only. |
| Local users | Available and default for local setup and admin actions. | Not available. |
| Agent bearer tokens | Supported for local agent access. | Not the hosted durable contract by itself. Hosted durable uses workspace-scoped access. |
| Workspace bearer access | Supported for workspace-scoped filesystem, search, and tree routes. | Required with explicit workspace and repo context. Hosted admin requires an operator-provisioned repo-bound bearer backed by an active root/wheel durable principal. |
| OIDC/SAML/SCIM | Disabled foundations only. They are not private-beta login, admin, or provisioning paths. | Disabled foundations only. They are not private-beta login, admin, or provisioning paths. |
| Hosted admin UI | Not applicable. | Not ready. Operators use the durable admin HTTP path or setup scripts. |

Durable admin routes reject local `Authorization: User ...`, hosted
`Authorization: Stratum-Session ...`, bearer tokens without matching
workspace/repo binding, and bearer tokens whose durable principal is missing,
inactive, or not root/wheel-scoped for the requested repo.

## Route Support

| Route group | Local demo | Hosted durable preview |
|---|---|---|
| Health and capabilities | Supported. | Supported. |
| Filesystem read/list/stat | Supported. | Supported for a pre-provisioned workspace. |
| Filesystem write/delete/patch/copy/move | Supported with idempotency support. | Supported only with workspace bearer access and a durable session-ref. |
| Search grep/find/tree | Supported. | Supported for a pre-provisioned workspace. |
| Search semantic | Unsupported. | Unsupported in the checked-in durable manifest because the search index is unavailable. |
| VCS log/status/diff | Supported, admin-gated. | Supported with explicit repo context and an admin-capable workspace session. |
| VCS refs/commit/revert | Supported, admin-gated, and idempotent where advertised. | Supported on the pre-provisioned route surface; commit also requires a durable session-ref. |
| VCS recovery | Unsupported. | Unsupported. |
| Review and change-request routes | Supported, admin-gated, and idempotent where advertised. | Supported with explicit repo context and an admin-capable workspace session. |
| Protected refs/paths | Supported. | Supported. |
| Workspaces list/create/get | Supported in local-state. | Supported through a repo-bound admin workspace bearer. |
| Workspace token issue/revoke | Supported in local-state. Token issue is not idempotent unless secret replay KMS is configured; token revoke is not idempotent. | Supported through the hosted admin bearer. Token issue names the hosted agent by Agent ID, which maps to `principal_uid` in the durable API. |
| Audit listing | Supported for user-admin sessions. | Unsupported right now. |
| Runs | Record-only route is supported locally; it does not schedule execution. | Unsupported right now. |
| Execute | Unavailable by default and outside the beta contract. | Unsupported right now. |

## Unsupported Surfaces

The private beta does not promise:

- durable-cloud audit listing
- hosted durable `/runs` or `/execute`
- OIDC or SAML login readiness
- SCIM provisioning readiness
- semantic search as a beta-ready product surface
- audit export productization or hosted audit UI
- durable MCP, durable FUSE, or direct durable-cloud REPL access
- hosted workspace administration UI
- provider mounts, remote blob mounts, or backing path exposure
- raw execution, shell execution, hosted runners, or scheduled jobs

Clients should read `/v1/capabilities` and fail closed when a route is
advertised as unavailable.

## Demo Path And Close-Out Gaps

The checked-in quick demo in `docs/agent-workspace-demo.md` proves the local
workspace basics: initialize local state, create the admin user and agent, start
`stratum-server`, run `stratumctl workspace seed-demo`, search, write, commit,
inspect history, and revert.

The private-beta golden path extends that local route surface with the existing
change-request and audit routes: create a workspace, issue a scoped workspace
token, edit files through that token, open a change request from the session
ref to the target ref, inspect the diff, comment, approve or reject, merge when
requirements pass, view audit events, and revert if needed. Local seeding is
packaged as the local-state-only `stratumctl workspace seed-demo`, which writes
the issued workspace token to a chmod-600 env file and prints safe next commands
without leaking secrets.

Hosted durable close-out remains narrower:

- create the hosted workspace before the customer flow starts
- issue scoped agent workspace tokens through an operator-provisioned,
  repo-bound hosted admin bearer
- pass explicit workspace and repo context on every hosted durable request
- use a session-ref for mounted durable mutations
- keep OIDC, SAML, SCIM, audit listing, runs, execute, semantic search, MCP,
  FUSE, and hosted admin screens out of the promised demo

The implementation plan for durable workspace and token parity is tracked in
`docs/plans/2026-06-08-durable-workspace-token-parity.md`.

## Operator Notes

- Do not paste, log, screenshot, or check in raw tokens, token hashes, refresh
  tokens, SCIM bearer values, SAML assertions, OIDC codes, DB URLs, or request
  bodies that may contain secrets.
- Treat workspace bearer tokens as customer secrets. Use secret-manager or
  operator-controlled storage outside this repo for real deployments.
- Do not put hosted admin bearer tokens in browser local storage or screenshots.
  Use short-lived/operator-controlled handling and issue narrower scoped agent
  tokens for the actual agent edit session.
- When discussing examples, name the credential type instead of showing a fake
  value.
- If durable-cloud returns an unsupported-route response, do not fall back to
  local `.vfs` state. That route is outside the hosted durable contract.
