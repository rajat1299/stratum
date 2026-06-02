# Event-Bus Audit Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a provider-free event-sink foundation for audit export with delivery status, bounded retry, redacted payloads, lag metrics, and explicit runtime gates while keeping local/Postgres audit persistence as the durable system of record.

**Architecture:** Preserve the existing `AuditStore` contract and wrap primary audit persistence with a secondary export layer that only receives server-assigned `AuditEvent` values after append succeeds. Delivery state and metrics live outside `audit_events`, so local/Postgres append/list semantics, VCS dedupe, and recovery identity checks remain unchanged. Runtime export configuration is disabled by default and only accepts provider-free/dev-safe modes in this slice; real NATS/Kafka/Kinesis adapters remain follow-on work.

**Tech Stack:** Rust 2024, `async-trait`, Tokio tests, existing local/in-memory/Postgres audit stores, existing runtime config/startup gates, provider-free in-memory test sinks.

---

## Current Audit State

- `src/audit.rs` owns `AuditStore`, `NewAuditEvent`, `AuditEvent`, audit actors/resources/actions/outcomes, `InMemoryAuditStore`, and `LocalAuditStore`.
- `AuditStore::append` assigns or returns the server-owned `AuditEvent`. Existing local/in-memory stores dedupe exact VCS commit/revert audit events; Postgres does the same over `audit_events`.
- `PostgresMetadataStore` already implements `AuditStore` over the existing `audit_events` table from `migrations/postgres/0001_durable_backend_foundation.sql`. The requested `migrations/postgres/0014_audit_event_foundation.sql` path is stale; migration 0014 is now the secret-bearing idempotency replay migration.
- Current route behavior treats audit persistence as mandatory for sensitive flows. Policy audit failure is pre-mutation fail-closed; post-mutation audit failures return redacted committed-but-unaudited responses or route-specific audit errors.
- Redaction is currently enforced by route/event construction and tests, not by `NewAuditEvent::with_detail`. Export must not widen payloads or export raw request bodies, tokens, token hashes, provider errors, DB URLs, raw external ids, encrypted replay plaintext, commit messages, file contents, or secret material.

## Slice Boundaries

In scope:

- Add an audit event sink abstraction and provider-free in-memory/test sink.
- Add an `ExportingAuditStore` wrapper that preserves primary store append/list/query behavior.
- Track export delivery status, bounded retry attempts, last redacted error code, pending count, oldest pending age, delivered count, failed count, and lag.
- Default every audit class to best-effort export. Add explicit mandatory class configuration for provider-free tests and fail closed only for configured mandatory classes.
- Add runtime config parsing for audit event export that is disabled by default and rejects malformed or unsupported provider config with env-name-only, redacted errors.
- Add documentation and a one-page audit posture note explaining Postgres/local audit persistence is the durable system of record until event-bus export is deployed.

Out of scope:

- Real NATS, Kafka, Kinesis, cloud credentials, network clients, or broker deployment.
- Hosted audit console or broader audit listing in durable-cloud.
- Broad audit coverage expansion for reads or new auth/provider behavior.
- Retention/export productization, external worker processes, Redis, production KMS/secrets-manager integration, SDK releases, or customer-facing hosted beta launch.

Rollback boundary:

- Disable audit event export and keep existing local/Postgres audit persistence behavior. Best-effort sink failures must not change route behavior; mandatory export failures may fail only when a class is explicitly configured mandatory.

## Task 1: Add Failing Provider-Free Export Tests

**Files:**
- Modify: `src/audit.rs`

**Step 1: Add tests before production code**

Add focused tests under `#[cfg(test)] mod tests`:

- `exporting_store_delivers_after_primary_append`
- `exporting_store_records_best_effort_failure_without_blocking_append`
- `exporting_store_retries_until_bounded_success`
- `exporting_store_fails_closed_for_configured_mandatory_class`
- `exporting_store_tracks_pending_lag_and_delivery_status`
- `exporting_store_exports_redacted_payload_without_sensitive_material`

The tests should construct an `ExportingAuditStore` around `InMemoryAuditStore` and a provider-free controllable sink. The redaction test should use sensitive-looking strings in details and prove exported payloads omit or replace them, while persisted audit details remain the existing caller-provided metadata.

**Step 2: Verify RED**

Run:

```bash
cargo test --locked audit::tests::exporting_store --lib -- --nocapture
```

Expected: FAIL to compile because `ExportingAuditStore`, sink traits, delivery status, and metrics do not exist.

## Task 2: Implement Audit Sink Domain Model

**Files:**
- Modify: `src/audit.rs`

**Step 1: Add minimal types**

Implement:

- `AuditExportClass` with conservative class mapping from `AuditAction`.
- `AuditExportMode` or policy that defaults all classes to best-effort and supports test-only mandatory class configuration.
- `AuditEventSink` trait with async publish over a redacted payload type.
- `AuditExportPayload`, `AuditExportDeliveryStatus`, `AuditExportAttempt`, and `AuditExportMetrics`.
- Provider-free in-memory/controllable sink used by tests.

Keep parameters borrowed where practical, return `Result` for fallible sink work, and avoid `unwrap`/`expect` in production code.

**Step 2: Verify GREEN for type-focused tests**

Run:

```bash
cargo test --locked audit::tests::exporting_store_delivers_after_primary_append --lib -- --nocapture
```

Expected: PASS after minimal implementation.

## Task 3: Add ExportingAuditStore Wrapper With Bounded Retry

**Files:**
- Modify: `src/audit.rs`

**Step 1: Implement wrapper behavior**

`ExportingAuditStore` should:

- Call the primary store first.
- Export only the returned `AuditEvent`.
- Preserve primary `list_recent`, `contains_vcs_commit_event`, and `contains_fs_mutation_recovery_event`.
- Retry sink publish up to a bounded configured attempt count.
- Record status and metrics for delivered, pending, failed, attempts, oldest pending age, and sequence lag.
- For best-effort classes, return the persisted audit event even when export fails.
- For mandatory configured classes, return a redacted `VfsError` after primary persistence if export cannot be delivered within the bounded retry policy.

**Step 2: Verify behavior tests**

Run:

```bash
cargo test --locked audit::tests::exporting_store --lib -- --nocapture
```

Expected: PASS.

**Step 3: Regression check existing audit contracts**

Run:

```bash
cargo test --locked audit::tests --lib -- --nocapture
```

Expected: PASS.

## Task 4: Add Runtime Config Gates Disabled By Default

**Files:**
- Modify: `src/backend/runtime.rs`
- Modify: `tests/server_startup.rs`

**Step 1: Write failing config tests**

Add tests proving:

- Missing audit export env means disabled.
- `STRATUM_AUDIT_EVENT_EXPORT_PROVIDER` with unsupported value fails closed before local files are created.
- Provider-free dev/test export cannot be enabled without an explicit dev gate.
- Invalid retry/mandatory-class values return env-name-only errors and do not leak raw values.

Use names such as:

- `audit_event_export_defaults_to_disabled`
- `audit_event_export_rejects_unsupported_provider_before_local_files`
- `audit_event_export_requires_dev_gate_for_provider_free_sink`
- `audit_event_export_rejects_invalid_retry_config_without_leaking_values`

**Step 2: Verify RED**

Run:

```bash
cargo test --locked backend::runtime::tests::audit_event_export --lib -- --nocapture
cargo test --locked --test server_startup audit_event_export -- --nocapture
```

Expected: FAIL because runtime config does not parse audit export env.

**Step 3: Implement parser**

Add env constants and parser in `src/backend/runtime.rs`:

- `STRATUM_AUDIT_EVENT_EXPORT_PROVIDER`
- `STRATUM_AUDIT_EVENT_EXPORT_ENABLE_DEV`
- `STRATUM_AUDIT_EVENT_EXPORT_MAX_ATTEMPTS`
- `STRATUM_AUDIT_EVENT_EXPORT_MANDATORY_CLASSES`

Allowed values for this slice:

- provider omitted or `disabled`: disabled.
- provider-free test/dev provider only when explicit dev gate is `1`; no external network/client setup.

Do not parse external broker URLs or secrets in this slice.

**Step 4: Verify GREEN**

Run the same focused runtime and startup tests. Expected: PASS.

## Task 5: Wire Disabled Export Into Server Store Construction

**Files:**
- Modify: `src/server/mod.rs`
- Modify: `src/backend/runtime.rs`
- Modify: `src/audit.rs`

**Step 1: Add failing server construction tests**

Add tests proving local and durable store construction preserve existing audit behavior when export is disabled, and that provider-free/dev config wraps the primary audit store only in test-safe construction paths.

Run:

```bash
cargo test --locked server::tests::audit_event_export --lib -- --nocapture
```

Expected: FAIL until wiring exists.

**Step 2: Implement wiring conservatively**

Create a helper that accepts `SharedAuditStore` plus runtime export config and returns either the original store or an `ExportingAuditStore` wrapper. Normal runtime remains disabled by default. Do not add external broker clients or network dependencies.

**Step 3: Verify focused server tests**

Run:

```bash
cargo test --locked server::tests::audit_event_export --lib -- --nocapture
cargo test --locked server::routes_audit --lib -- --nocapture
```

Expected: PASS.

## Task 6: Extend Postgres/Migration Coverage Additively If Needed

**Files:**
- Modify if needed: `migrations/postgres/0019_event_bus_audit_pipeline.sql`
- Modify if needed: `src/backend/postgres_migrations.rs`
- Modify if needed: `src/backend/postgres.rs`

**Step 1: Decide from failing tests**

Prefer no schema migration if delivery status can remain provider-free/in-memory for this foundation. If durable delivery status needs a table to satisfy acceptance, add only additive tables such as `audit_event_export_deliveries` keyed by `audit_event_id` / sequence with bounded status fields and no payload/body/secret columns.

**Step 2: Add failing adoption tests before migration code**

Run:

```bash
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
```

Expected: new focused tests fail before implementation, then pass after additive migration/adoption code.

## Task 7: Documentation And Audit Posture Note

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`
- Create: `docs/audit-posture.md`

**Step 1: Update docs**

Document:

- Audit event export is disabled by default.
- Local/Postgres audit persistence remains the durable system of record.
- Provider-free sink foundation covers status, retry, lag, and redacted payload testing only.
- Durable-cloud `/audit` listing remains unsupported.
- Real broker adapters and customer-facing export rollout are out of scope.

**Step 2: Add one-page posture note**

Create `docs/audit-posture.md` with:

- Source-of-record statement.
- Current audited classes.
- Redaction guarantees.
- Event-bus export status and limitations.
- Operator rollback stance.

**Step 3: Verify docs are included in diff checks**

Run:

```bash
git diff --check
```

Expected: PASS.

## Task 8: Reviews And Final Verification

**Files:**
- All files touched in Tasks 1-7

**Step 1: Run spec/correctness review**

Ask a reviewer subagent to verify the implementation against this plan, including:

- Primary audit persistence remains mandatory and unchanged.
- Export failures do not masquerade as primary append failures.
- Mandatory class behavior is explicit and tested.
- Export payload redaction is bounded and does not leak secrets.
- Runtime gates are disabled by default and fail closed with redacted errors.

**Step 2: Run code-quality/security review**

Ask a reviewer subagent to inspect Rust quality, concurrency, retry bounds, error handling, redaction, docs accuracy, and test adequacy.

**Step 3: Fix findings and rerun focused tests**

Rerun the tests related to any fixes.

**Step 4: Final verification commands**

Run:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked audit::tests --lib -- --nocapture
cargo test --locked server::routes_audit --lib -- --nocapture
cargo test --locked server::routes_auth --lib -- --nocapture
cargo test --locked server::routes_scim --lib -- --nocapture
cargo test --locked server::routes_workspace::tests --lib -- --nocapture
cargo test --locked server::middleware --lib -- --nocapture
cargo test --locked server::repo_context --lib -- --nocapture
cargo test --locked workspace::tests --lib -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo check --locked -p stratum-core
cargo test --locked -p stratum-core
cargo check --locked
cargo check --locked --features postgres
cargo check --locked --features fuser --bin stratum-mount
cargo test --locked --features fuser fuse_mount --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_PRE_CUTOVER_LIVE= ./scripts/check-pre-cutover-load-chaos.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

Expected: PASS, with live Postgres/R2 portions skipped only where env is unset.

## Commit Plan

Use small commits:

1. `docs: plan event-bus audit pipeline`
2. `feat: add provider-free audit event export`
3. `feat: gate audit event export runtime config`
4. `docs: record audit export posture`
5. `fix: harden event-bus audit review findings` if reviews find issues
6. `docs: record event-bus audit final verification`
