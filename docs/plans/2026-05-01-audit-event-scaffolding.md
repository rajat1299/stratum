# Audit Event Scaffolding

Date: 2026-05-01
Branch: `v2/foundation`

## Goal

Add the first production-shaped audit-event foundation for Stratum mutating operations. This slice is intentionally local/file-backed and does not try to complete the future Postgres/event-bus audit pipeline.

## Context

The CTO plan calls out audit events as a production blocker and milestone-2 requirement. The current product has scoped sessions, workspace tokens, refs, run records, and CI, but no durable audit event stream for writes, commits, ref updates, token issuance, or run creation.

## Scope

- Add an `audit` module with a structured event model, in-memory store, and durable local store.
- Wire the HTTP server state to open a local audit store at `.vfs/audit.bin` by default.
- Emit successful mutation events for:
  - filesystem write, mkdir, delete, copy, and move operations
  - VCS commit, revert, ref create, and ref update operations
  - workspace creation and workspace-token issuance
  - run-record creation
- Keep secrets, raw tokens, request bodies, file contents, run prompt/command/stdout/stderr/result contents, and commit messages out of audit details.
- Add a minimal admin-gated `GET /audit` endpoint for local verification and future console/SDK work.
- Document the endpoint and status of this slice.

## Out Of Scope

- Read/auth/policy-decision auditing.
- Cloud/Postgres audit tables or event-bus ingestion.
- Tamper-evident hashes, retention policy, export jobs, pagination, and query filters beyond a bounded recent-event list.
- Change-request, approval, protected-ref, or protected-path workflows.

## Design Notes

- `AuditStore::append` assigns event ID, sequence, and timestamp so handlers cannot spoof those fields.
- Events record actor UID/username, optional delegate, mounted workspace context, action, resource kind/path/id, outcome, and a small string-keyed detail map.
- The local store follows the existing workspace/idempotency pattern: decode on open, rewrite through a temporary file, then atomic rename and directory sync.
- Handler audit writes happen after the underlying mutation succeeds. If audit persistence fails, the HTTP mutation returns `500` so successful API responses are not unaudited.

## Test Plan

- Unit-test audit model/store behavior:
  - local store reloads appended events in sequence order
  - corrupt store bytes fail with `CorruptStore`
  - persisted bytes do not contain supplied secret-like detail values when details are built through the safe route helpers
- Route tests:
  - filesystem write emits an event without body content
  - VCS commit/ref mutation emits commit/ref metadata without commit message
  - workspace-token issuance emits token ID/agent UID without raw agent token or workspace token
  - run creation emits run ID/path metadata without prompt, command, stdout, stderr, or result content
  - scoped workspace bearer sessions cannot list audit events; admin sessions can

## Verification

Run focused tests while developing, then the standard full verification set before merging:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```
