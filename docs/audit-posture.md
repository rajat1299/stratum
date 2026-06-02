# Audit Posture

Last updated: 2026-06-02

## Source Of Record

Stratum treats local audit persistence and the Postgres `audit_events` table as the durable audit system of record. Event-bus export is a secondary delivery path. If export is disabled or a best-effort export fails, the durable audit record remains the local/Postgres audit event.

## Current Coverage

Current audit events cover mutating filesystem operations, VCS commit/revert/ref mutations, route policy allow/deny decisions, idempotency quota failures, protected-rule and change-request workflows, workspace creation, workspace-token lifecycle events, run-record creation, hosted OIDC/SAML refresh-token lifecycle events, and provider-free SCIM provisioning events.

Read audit coverage, hosted audit operations, customer audit consoles, long-term retention/export productization, and production broker delivery are not complete.

## Redaction Guarantees

Audit details and exported payloads are metadata-only. They must not include file contents, request bodies, raw idempotency keys, raw tokens, token hashes, access tokens, refresh tokens, SCIM bearer tokens, raw external ids, SAML assertions/XML, OIDC authorization codes, provider error bodies, DB URLs, encrypted replay plaintext, commit messages, or generic secret material.

The event-bus foundation redacts export payload details separately from persisted audit details. Persisted audit details remain route-owned bounded metadata, and tests cover the current no-secret export posture.

## Event-Bus Export Status

`STRATUM_AUDIT_EVENT_EXPORT_PROVIDER` defaults to `disabled`. The only accepted enabled provider in this foundation is `provider-free-dev`, and it requires `STRATUM_AUDIT_EVENT_EXPORT_ENABLE_DEV=1`. Optional settings are bounded retry attempts and explicit mandatory audit classes.

The provider-free export wrapper publishes only after primary audit persistence returns a server-assigned `AuditEvent`. Best-effort export failures do not change route behavior. Mandatory export failures fail closed only for explicitly configured mandatory classes.

Real NATS, Kafka, Kinesis, cloud credentials, broker URLs, durable external delivery productization, and hosted audit UI remain future work.

## Rollback Stance

The rollback boundary is to leave audit export disabled. Local/Postgres audit persistence continues as the durable source of record, and existing route-level audit failure behavior remains the correctness boundary.
