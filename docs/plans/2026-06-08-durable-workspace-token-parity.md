# Durable Workspace And Token Parity Plan

Status: implemented for Task 4.
Date: 2026-06-08.

This plan closes the durable workspace/token gap without weakening the hosted
safety boundary.

## Current State

Durable mode already has the needed Postgres storage for workspaces and
workspace tokens. It can store durable workspaces, scoped token hashes, token
versions, revocation state, repo identity, org identity, and durable
principals.

The hosted HTTP router now mounts `/workspaces`. Local token issuance still
uses `agent_token`; durable token issuance uses a hosted Agent ID and rejects
local agent-token request bodies.

For private beta, hosted durable workspace setup remains an operator path. The
hosted route must not fall back to local `.vfs` state.

## Supported Beta Path

Hosted durable beta setup is handled by an operator:

1. Create the repo binding in the durable control plane.
2. Create the durable workspace with org, repo, base ref, and session ref.
3. Bind an active durable principal for that org and repo.
4. Issue a workspace bearer token for that durable principal.
5. Give the agent only the workspace bearer token, workspace id, org id, repo
   id, and session ref it needs.

Raw secrets are returned once and must not be logged, checked in, or shown in
screenshots.

## Implementation Plan

1. Add a durable principal lookup to the workspace metadata store.

   The lookup should take org id, repo id, and principal uid. It should return
   only active principals that belong to that exact org and repo.

2. Split token issuance by runtime.

   Local mode keeps the current `agent_token` request path.

   Durable mode accepts a durable principal identity instead. The route should
   reject local agent tokens in durable mode and reject durable principal
   issuance in local mode.

3. Use the durable admin gate for workspace routes.

   Durable workspace list, create, get, issue, and revoke should require a
   workspace bearer session backed by an active root or wheel durable principal
   for the requested repo. `Authorization: User root` remains local-only.

4. Wire the durable router only after the new issuance path is ready.

   Merge the workspace routes into the durable router and remove `/workspaces`
   from the durable unsupported-route wildcard. The wildcard must not shadow
   `/workspaces/{id}` or token paths.

5. Update capabilities and conformance together.

   The durable manifest should advertise workspace list, create, get, issue,
   and revoke only when the route is actually wired. Token issuance should
   advertise idempotency only when secret replay KMS is configured.

6. Add tests before enabling the route.

   Required tests:

   - durable workspace list/create/get works with a repo-bound admin workspace
     bearer
   - local `User root` is rejected in durable mode
   - cross-repo and cross-org headers are rejected
   - durable token issue uses a durable principal and does not call local core
     token auth
   - revoke only affects the selected workspace token
   - token issue with `Idempotency-Key` fails before mutation when KMS is not
     configured
   - capability and conformance fixtures match the wired behavior

## Done Criteria

This task is complete when a fresh checkout can run one command and prove:

- durable workspace list/create/get is available
- durable token issue/revoke is available through the durable admin path
- raw token secrets are returned once and never stored in clear text
- unsupported durable routes still return the stable unsupported response
- `/v1/capabilities` and checked-in contract fixtures match the server
