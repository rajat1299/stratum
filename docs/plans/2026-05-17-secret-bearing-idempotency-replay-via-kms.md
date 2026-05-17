# Secret-Bearing Idempotency Replay Via KMS Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make workspace-token issuance safely idempotent by storing only encrypted/KMS-backed replay payloads, while closing the review-detail contract gaps needed by frontend D2.

**Architecture:** Keep the existing idempotency store contract for `secret_free` and `partial` replay untouched, and add a narrow secret-replay envelope path used only by routes that intentionally return one-time secrets. Encryption lives behind a runtime-injected KMS/secrets seam; stores persist ciphertext envelopes plus redacted metadata, never raw workspace tokens. The review contract precursor stays independent: expose the resolved file-view policy on change-request responses and make `/vcs/diff` accept explicit commit pairs so frontend can build D2 without inferring policy from the manifest.

**Tech Stack:** Rust 2024, Axum, Tokio, serde/serde_json, existing `IdempotencyStore` traits, Postgres migration catalog, `ring` AEAD or equivalent reviewed AEAD dependency, TypeScript/Python SDK contract fixtures.

---

## Context Read

- `docs/plans/2026-05-15-backend-roadmap.md`, section `4.5. Secret-Bearing Idempotency Replay Via KMS`
- `docs/plans/2026-05-17-pre-slice45-review-contract-coordination.md`
- `docs/plans/2026-05-16-real-postgres-pool-secret-seam-migration-adoption.md`
- `docs/project-status.md`, especially Idempotency Retention/Quota, Durable Auth/Session, Real Postgres Pool, and live-gate status
- `docs/http-api-guide.md`, workspace-token, idempotency, durable-cloud, and review sections
- `src/idempotency.rs`
- `src/server/idempotency.rs`
- `src/server/routes_workspace.rs`
- `src/workspace/mod.rs`
- `src/backend/postgres.rs`
- `src/backend/postgres_migrations.rs`
- `src/server/routes_review.rs`
- `src/server/routes_vcs.rs`
- `src/server/core.rs`
- SDK types/tests under `sdk/typescript` and `sdk/python`

## Current Baseline

- `v2/foundation` is clean at plan time and already contains Slice 4 plus live-gate hardening commits.
- Main checkout is intentionally dirty and behind remote; do not use it for implementation.
- Live Postgres/R2 gates have a green protected-main manual run, so future wording should say provider-verified green rather than awaiting first green.
- `POST /workspaces/{id}/tokens` currently rejects `Idempotency-Key` before authenticating the backing agent token.
- The success response contains raw `workspace_token`; current idempotency persistence rejects `SecretBearing` classification before storing any response body.
- `idempotency_records` can store `secret_free` and `partial` replay classifications, but the Postgres constraint rejects `secret_bearing`.
- Local idempotency v2 records containing `SecretBearing` intentionally fail to load.
- `GET /change-requests/{id}` currently returns only `{ change_request, approval_state }`; it does not return a resolved `require_all_files_viewed`.
- `GET /vcs/diff` currently accepts only `path`; it does not accept explicit `base` and `head` commit query parameters.

## Acceptance Criteria

- `POST /workspaces/{id}/tokens` accepts `Idempotency-Key` when secret replay KMS is configured.
- A matching retry replays the original token response with `x-stratum-idempotent-replay: true`.
- No raw workspace token, raw agent token, raw idempotency key, plaintext replay body, encryption key, nonce material, DB URL, SQL text, endpoint, or provider failure detail is persisted, logged, serialized in `Debug`, or returned on error.
- Secret-bearing replay records persist encrypted envelopes only, with authenticated metadata binding the envelope to scope, key hash, request fingerprint, route, status, and replay classification.
- Missing KMS config, decrypt failure, unknown key id, key rotation/removal, ciphertext corruption, malformed envelope, and idempotency completion failure all fail closed with fixed redacted errors and do not issue duplicate tokens on matching retry.
- Existing `secret_free` and `partial` idempotency behavior, quotas, stale-pending takeover, retention sweeps, and non-token routes remain unchanged.
- Retention deletion removes encrypted secret-bearing replay records and metadata.
- Local and Postgres idempotency stores both support encrypted secret-bearing replay.
- Postgres migration adoption verifies the new secret-replay schema shape and refuses unverifiable schemas.
- `GET /change-requests/{id}` and list/mutation responses return a CR-level resolved `require_all_files_viewed: bool`.
- `GET /vcs/diff?base=<base_commit>&head=<head_commit>[&path=...]` works for local and durable routes without adding a CR-scoped diff endpoint.
- SDK TypeScript/Python types, fixtures, and tests reflect the new review and workspace-token idempotency contracts.

## Out Of Scope

- Cloud-provider-specific KMS calls beyond the first seam and test/local provider.
- Idempotent workspace-token revocation.
- Secret-bearing replay for routes other than workspace-token issuance.
- Broad durable default flip.
- Distributed locks.
- Recovery scheduler productionization beyond verifying retention/deletion interactions.
- CR-scoped `GET /change-requests/{id}/diff`.

## Task 1: Frontend/Backend Review Contract Precursor

**Files:**
- Modify: `src/review.rs`
- Modify: `src/server/routes_review.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/core.rs`
- Modify: `src/db.rs`
- Modify: `src/vcs/mod.rs`
- Modify: `src/backend/committed_read.rs`
- Modify: `sdk/typescript/src/types.ts`
- Modify: `sdk/python/src/stratum_sdk/types.py`
- Modify: `sdk/typescript/src/client.ts`
- Modify: `sdk/python/src/stratum_sdk/client.py`
- Modify tests in `src/server/routes_review.rs`, `src/server/routes_vcs.rs`, `sdk/typescript/tests/client.test.ts`, `sdk/python/tests/test_client.py`
- Update fixtures if generated by existing SDK/capability test helpers

**Step 1: Write failing tests for resolved file-view policy**

Add route tests proving:

- A CR with no matching protected rules returns `"require_all_files_viewed": false`.
- A matching protected ref rule with default `true` returns `true`.
- A matching protected ref rule with explicit `false` and no matching path rule returns `false`.
- A matching protected path rule with default `true` returns `true`.
- If both ref and path rules match, the resolved value is `true` when any matched rule requires all files viewed and `false` only when every matched rule sets `false`.
- The field is included in `GET /change-requests/{id}`, `GET /change-requests`, create, approval, reviewer, comment, dismiss, reject, and merge responses wherever `ChangeRequestResponse` is returned.

Run:

```bash
cargo test --locked server::routes_review::tests::approval_state_is_included_in_change_request_read_and_list_responses --lib -- --nocapture
```

Expected before implementation: tests that assert `require_all_files_viewed` exists fail.

**Step 2: Implement resolved policy on the backend**

Preferred shape:

```json
{
  "change_request": { "...": "..." },
  "approval_state": { "...": "..." },
  "require_all_files_viewed": true
}
```

Implementation guidance:

- Add `require_all_files_viewed: bool` to `ApprovalPolicyDecision` or a small adjacent policy summary helper, but expose it top-level in `ChangeRequestResponse`.
- Compute it from the already matched active protected ref/path rules:
  - no matched rules => `false`
  - any matched rule with `require_all_files_viewed == true` => `true`
  - all matched rules explicitly `false` => `false`
- If approval-state computation fails, return `approval_state.available = false` as today and set top-level `require_all_files_viewed = true` fail-closed so frontend does not under-enforce its display state.
- Do not add file-view tracking or backend enforcement in this slice.

Run:

```bash
cargo test --locked server::routes_review::tests::approval_state --lib -- --nocapture
cargo test --locked server::routes_review::tests::protected_rule_file_view_flag_defaults_true_and_round_trips --lib -- --nocapture
```

Expected: focused review response tests pass.

**Step 3: Write failing tests for explicit commit-pair diff**

Add route/core tests proving:

- `GET /vcs/diff?base=<base>&head=<head>` renders the diff between those commits rather than current worktree/session state.
- `path` filtering still works with explicit `base`/`head`.
- Supplying exactly one of `base` or `head` returns `400` with a fixed redacted error.
- Invalid commit ids return redacted errors without echoing raw input.
- Durable-cloud/guarded durable diff uses durable commit/tree/object stores for the explicit pair and does not fall back to local state.

Run:

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_diff --lib -- --nocapture
cargo test --locked vcs::diff --lib -- --nocapture
```

Expected before implementation: explicit `base`/`head` tests fail because `DiffQuery` ignores those fields.

**Step 4: Implement explicit commit-pair diff**

Implementation guidance:

- Extend `DiffQuery` with `base: Option<String>` and `head: Option<String>`.
- Add a route helper that requires `base` and `head` to be both present or both absent.
- For local `StratumDb`, add a method that parses both ids, loads committed path maps for each commit, computes `diff_path_maps`, and renders the existing text diff. Keep current `vcs_diff_as(path)` behavior unchanged when `base/head` are absent.
- For durable `CoreDb`, add a method that parses durable commit ids, loads both commit records and their root trees, computes durable committed path maps under the caller's auth/mount scope, and renders through `render_durable_diff`.
- Keep `/vcs/diff?path=...` backward-compatible.
- Do not introduce `GET /change-requests/{id}/diff`.

Run:

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable_diff --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::vcs_routes_use_local_core_runtime --lib -- --nocapture
```

Expected: explicit and legacy diff tests pass.

**Step 5: Update SDK types and clients**

Implementation guidance:

- Add `require_all_files_viewed: boolean` / `bool` to `ChangeRequestResponse`.
- Add optional `base` and `head` options to TypeScript/Python `diff` client methods while preserving the old single-path call shape if practical.
- Add tests that the generated URL can be `/vcs/diff?base=...&head=...&path=...`.

Run:

```bash
cd sdk/typescript && npm test -- --runInBand
cd sdk/python && python -m pytest
```

If the repo uses different local SDK commands, follow the existing package scripts and record the exact commands in the implementation notes.

**Step 6: Commit**

```bash
git add src/review.rs src/server/routes_review.rs src/server/routes_vcs.rs src/server/core.rs src/db.rs src/vcs/mod.rs src/backend/committed_read.rs sdk
git commit -m "feat: expose review detail policy and commit-pair diff"
```

## Task 2: Secret Replay KMS Seam

**Files:**
- Create: `src/secret_replay.rs`
- Modify: `src/lib.rs`
- Modify: `src/backend/runtime.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/bin/stratum_server.rs`
- Modify: `tests/server_startup.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

**Step 1: Write failing KMS seam tests**

Add tests for:

- Encrypt/decrypt round trip returns the original JSON only with the same AAD.
- Wrong AAD fails with a fixed redacted error.
- Unknown key id fails with a fixed redacted error.
- Provider `Debug` redacts key bytes and plaintext.
- Runtime config rejects malformed local/test key material by env var name only.
- Runtime config treats missing secret replay KMS as disabled, not as an invalid durable config unless idempotent token issuance is attempted.

Suggested env names:

- `STRATUM_SECRET_REPLAY_KMS_PROVIDER=disabled|local-aead`
- `STRATUM_SECRET_REPLAY_KMS_KEY_ID`
- `STRATUM_SECRET_REPLAY_KMS_KEY_B64`

Run:

```bash
cargo test --locked backend::runtime::tests::secret_replay --lib -- --nocapture
cargo test --locked secret_replay --lib -- --nocapture
```

Expected before implementation: module/config tests fail to compile.

**Step 2: Add a small reviewed AEAD dependency**

Preferred: use `ring` AEAD directly because it is already in the lockfile transitively. Add it as a direct dependency if needed.

Implementation requirements:

- 256-bit AEAD key material only.
- Random nonce per encryption.
- AAD must include scope, key hash, request fingerprint, route, status code, and replay classification.
- Ciphertext envelope must include version, key id, nonce, ciphertext, and AAD hash, but not plaintext.
- `Debug` for envelope/provider prints only version/key id presence/ciphertext length.

Run:

```bash
cargo check --locked
```

If adding a direct dependency requires a lockfile update, run the minimum non-locked check needed to update `Cargo.lock`, inspect the lockfile, then return to `--locked` gates.

**Step 3: Implement the seam**

Create a narrow API, for example:

```rust
pub trait SecretReplayKms: Send + Sync {
    fn encrypt_json(
        &self,
        aad: &SecretReplayAad,
        body: &serde_json::Value,
    ) -> Result<SecretReplayEnvelope, VfsError>;

    fn decrypt_json(
        &self,
        aad: &SecretReplayAad,
        envelope: &SecretReplayEnvelope,
    ) -> Result<serde_json::Value, VfsError>;
}
```

Implementation requirements:

- Initial provider can be local/env-backed for tests and development.
- The trait must be runtime-injected into `AppState`; route code must not read raw env variables.
- Missing provider must preserve the current route behavior: token issuance without `Idempotency-Key` still works, but idempotent token issuance fails closed.
- Error strings must be fixed and redacted, for example `secret replay KMS is unavailable` and `secret replay decrypt failed`.

Run:

```bash
cargo test --locked secret_replay --lib -- --nocapture
cargo test --locked backend::runtime --lib -- --nocapture
```

Expected: seam and runtime tests pass.

**Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock src/secret_replay.rs src/lib.rs src/backend/runtime.rs src/server/mod.rs src/bin/stratum_server.rs tests/server_startup.rs
git commit -m "feat: add secret replay kms seam"
```

## Task 3: Idempotency Store Support For Encrypted Secret Replay

**Files:**
- Modify: `src/idempotency.rs`
- Modify: `src/server/idempotency.rs`
- Modify: `src/backend/postgres.rs`
- Modify: `src/backend/postgres_migrations.rs`
- Create: `migrations/postgres/0014_secret_bearing_idempotency_replay.sql`
- Modify: `tests/postgres/0001_durable_backend_foundation_smoke.sql`

**Step 1: Write failing store tests**

Add tests proving:

- `SecretBearing` completion is still rejected by the old generic `complete_with_classification` helper.
- A new explicit encrypted-secret completion API persists only an encrypted envelope and secret metadata.
- Generic `idempotency_json_replay_response` never returns a `SecretBearing` record body directly.
- Begin/replay for a secret-bearing record returns a record whose `Debug` does not include ciphertext, plaintext, raw token, key hash, or scope material.
- Local retention sweep deletes encrypted secret-bearing completed records.
- Local v2 raw `SecretBearing` records still fail to load with a redacted corruption error.

Run:

```bash
cargo test --locked idempotency::tests::classification_rejects_secret_bearing --lib -- --nocapture
```

Expected before implementation: new encrypted-secret APIs fail to compile.

**Step 2: Extend idempotency domain types**

Implementation guidance:

- Add a metadata struct such as `SecretReplayMetadata { envelope_version, key_id, aad_hash, encrypted_at_unix_seconds }`.
- Add an explicit method on `IdempotencyStore`, for example `complete_with_encrypted_secret_replay(...)`, instead of relaxing generic secret-bearing completion.
- Preserve existing `complete_with_classification(..., SecretBearing)` rejection so accidental secret persistence remains impossible.
- Add a route-only decrypt helper that checks `record.classification == SecretBearing` and requires KMS before replaying.

Run:

```bash
cargo test --locked idempotency --lib -- --nocapture
cargo test --locked server::idempotency --lib -- --nocapture
```

Expected: local in-memory and local-file tests pass.

**Step 3: Add Postgres migration 0014**

Migration requirements:

- Allow `replay_classification = 'secret_bearing'`.
- Add secret replay metadata columns such as:
  - `secret_replay_envelope_version INTEGER`
  - `secret_replay_key_id TEXT`
  - `secret_replay_aad_hash TEXT CHECK (...64 hex...)`
  - `secret_replay_encrypted_at TIMESTAMPTZ`
- Add constraints:
  - non-secret completed rows must have all secret replay columns `NULL`
  - secret-bearing completed rows must have all secret replay columns present
  - pending rows must not carry status, body, or secret replay metadata
- Keep existing response body JSON required for completed rows, but require secret-bearing response body to be an encrypted envelope object, not plaintext route JSON.

Run:

```bash
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
```

Expected: no-URL smoke skips; feature tests pass locally with live portions skipped when URL is unset.

**Step 4: Implement Postgres store support**

Implementation guidance:

- Select/insert/update the new metadata columns in `PostgresMetadataStore` idempotency methods.
- Keep all errors redacted through existing `postgres_error` mapping.
- Extend `replay_classification_to_db` and `replay_classification_from_db` safely.
- Add Postgres optional tests for encrypted secret-bearing completion/replay and retention deletion.

Run:

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
```

Expected: Postgres feature tests pass; live portions skip cleanly without `STRATUM_POSTGRES_TEST_URL`.

**Step 5: Update adoption verifier and smoke harness**

Implementation guidance:

- Add migration `0014` to the static catalog.
- Teach adoption verification to require the new idempotency columns and constraints.
- Add a focused adoption refusal test for schemas missing the encrypted secret replay constraint shape.
- Extend rollback-only smoke SQL to verify malformed secret-bearing rows are rejected and a valid encrypted-envelope-shaped row is accepted.

Run:

```bash
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
```

Expected: migration tests pass; smoke skips without URL.

**Step 6: Commit**

```bash
git add src/idempotency.rs src/server/idempotency.rs src/backend/postgres.rs src/backend/postgres_migrations.rs migrations/postgres/0014_secret_bearing_idempotency_replay.sql tests/postgres/0001_durable_backend_foundation_smoke.sql
git commit -m "feat: store encrypted secret idempotency replay"
```

## Task 4: Workspace-Token Idempotency

**Files:**
- Modify: `src/server/routes_workspace.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/workspace/mod.rs` only if helper types are needed
- Modify SDK tests/types if token issuance options expose idempotency support
- Modify `docs/http-api-guide.md`

**Step 1: Write failing route tests**

Add tests proving:

- `POST /workspaces/{id}/tokens` with `Idempotency-Key` succeeds when KMS is configured.
- A matching retry returns the same `workspace_token`, same `token_id`, and replay header.
- Same key with a different request returns the existing idempotency conflict response and does not issue a new token.
- Same key while pending returns in-progress and does not issue a token.
- Missing KMS with `Idempotency-Key` returns a fixed redacted error and does not authenticate/issue/persist a token.
- Invalid backing agent token still returns `401` and does not create an idempotency row.
- Fingerprint includes route, admin actor, repo id, workspace id, token name, authenticated agent uid, and normalized read/write prefixes, but not raw `agent_token` or raw workspace token.
- Audit events never include raw agent token, raw workspace token, encrypted envelope, key id, nonce, or secret hash.
- If decrypt fails on replay, the route fails closed and does not issue a duplicate token.

Run:

```bash
cargo test --locked server::routes_workspace::tests::issue_workspace_token --lib -- --nocapture
```

Expected before implementation: tests expecting idempotent token issuance fail.

**Step 2: Add begin/replay helper for token issuance**

Implementation guidance:

- Remove the unconditional idempotency-key rejection for issuance only.
- Keep revocation rejection unchanged.
- Parse idempotency key after admin auth, but do not begin until the backing agent token, repo, workspace, and normalized prefixes are known.
- Scope suggestion:
  - local singleton: `workspace:<workspace_id>:tokens:issue`
  - explicit repo: `repo:<repo_id>:workspace:<workspace_id>:tokens:issue`
- Use idempotency quota identity for workspace plus admin/delegate principal where available.
- On replay, require `record.classification == SecretBearing`, decrypt with KMS using the same AAD, and return the decrypted JSON with the replay header.
- If a non-secret record exists under the token-issue scope, treat it as corrupt/fail-closed rather than returning it.

Run:

```bash
cargo test --locked server::routes_workspace::tests::issue_workspace_token --lib -- --nocapture
```

Expected: begin/replay tests progress to completion/encryption failures until the next step.

**Step 3: Complete encrypted replay after successful issuance**

Implementation guidance:

- Build the success JSON body exactly once.
- Encrypt that body before completing idempotency.
- Persist only the encrypted envelope JSON plus secret replay metadata through the new idempotency API.
- Return the plaintext body to the caller only after encrypted replay persistence succeeds.
- If idempotency completion fails after token creation, return a fixed redacted failure body that does not include the raw token. Record/abort behavior must not allow a matching retry to issue duplicate tokens under the same key.
- Preserve existing non-idempotent issuance behavior when no idempotency key is present.

Run:

```bash
cargo test --locked server::routes_workspace::tests::issue_workspace_token --lib -- --nocapture
cargo test --locked workspace::tests::issued_workspace_token --lib -- --nocapture
```

Expected: token issuance route tests pass.

**Step 4: Add Postgres-backed route coverage**

If existing optional Postgres route tests cover workspace token issuance, extend them. Otherwise add focused feature-gated coverage that:

- applies migrations through `PostgresMigrationRunner`
- opens durable server stores
- issues a token with an idempotency key
- proves the stored idempotency row is `secret_bearing`, contains encrypted envelope metadata, and does not contain the raw token
- replays the response successfully

Run:

```bash
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
```

Expected: feature tests pass; live provider portions skip unless credentials are present.

**Step 5: Commit**

```bash
git add src/server/routes_workspace.rs src/server/mod.rs src/workspace/mod.rs sdk docs/http-api-guide.md
git commit -m "feat: replay workspace token issuance through encrypted idempotency"
```

## Task 5: SDK, Capabilities, Docs, And Status

**Files:**
- Modify: `src/server/routes_capabilities.rs`
- Modify: `sdk/contracts/capabilities.v1.json`
- Modify: `sdk/contracts/capabilities.v1.durable-cloud.json`
- Modify: `sdk/typescript/src/types.ts`
- Modify: `sdk/typescript/src/client.ts`
- Modify: `sdk/typescript/tests/client.test.ts`
- Modify: `sdk/python/src/stratum_sdk/types.py`
- Modify: `sdk/python/src/stratum_sdk/client.py`
- Modify: `sdk/python/tests/test_client.py`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Step 1: Update capability manifest**

Implementation guidance:

- Advertise workspace-token issuance idempotency only when secret replay KMS support is available in the contract.
- If the manifest is static, add a clear field such as `workspaces.token_issuance.idempotency: "secret_replay_kms"` or extend the existing endpoint idempotency list consistently.
- Preserve old clients by adding optional fields in SDK types.

Run:

```bash
cargo test --locked server::routes_capabilities::tests --lib -- --nocapture
```

Expected: manifest tests pass.

**Step 2: Update SDK tests and fixtures**

Implementation guidance:

- TypeScript/Python clients should allow `Idempotency-Key` on workspace-token issuance.
- Add tests that idempotency options are sent for token issuance.
- Add tests that diff options can include `base` and `head`.
- Add `require_all_files_viewed` to CR response fixtures.

Run:

```bash
cd sdk/typescript && npm test
cd sdk/python && python -m pytest
```

Expected: SDK tests pass.

**Step 3: Update operator docs**

Documentation must include:

- Env/config for secret replay KMS.
- Failure modes: missing KMS, decrypt failure, unknown key id, rotation/removal, malformed envelope, idempotency completion failure.
- Redaction guarantees.
- Retention behavior for encrypted replay records.
- `POST /workspaces/{id}/tokens` idempotency examples.
- `GET /change-requests/{id}` resolved `require_all_files_viewed`.
- `GET /vcs/diff?base=...&head=...` frontend contract.
- Live gates are provider-verified green as of the latest protected-main run.

Run:

```bash
git diff -- docs/http-api-guide.md docs/project-status.md
git diff --check
```

Expected: docs reflect the new behavior and no whitespace errors.

**Step 4: Commit**

```bash
git add src/server/routes_capabilities.rs sdk docs/http-api-guide.md docs/project-status.md
git commit -m "docs: advertise encrypted token idempotency"
```

## Task 6: Review Fixes And Final Verification

**Files:**
- Any files touched by review findings.

**Step 1: Run focused gates**

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked idempotency --lib -- --nocapture
cargo test --locked server::idempotency --lib -- --nocapture
cargo test --locked server::routes_workspace::tests::issue_workspace_token --lib -- --nocapture
cargo test --locked server::routes_review::tests::approval_state --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable_diff --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
```

Expected: all pass; optional live scripts skip cleanly without local credentials.

**Step 2: Request spec/correctness review**

Ask reviewer to inspect:

- KMS/AAD binding and replay envelope design.
- Whether any raw workspace token or agent token can persist through idempotency, audit, logs, `Debug`, SDK fixtures, or errors.
- Duplicate-token behavior on decrypt/completion failure.
- Retention deletion of encrypted records.
- Postgres constraints and adoption verifier completeness.
- Review contract field semantics.
- Explicit commit-pair diff authorization/scope semantics.

**Step 3: Request code-quality/security review**

Ask reviewer to inspect:

- Crypto misuse, nonce reuse risk, key parsing, key rotation failure behavior.
- Async lock ordering and idempotency race behavior.
- Postgres transaction boundaries.
- Backward compatibility for non-secret idempotency and old local idempotency files.
- SDK optional-field compatibility.
- Error redaction.

**Step 4: Fix findings locally**

For every accepted finding:

```bash
cargo fmt --all -- --check
git diff --check
<focused failing/passing test>
```

Commit review fixes in small commits, for example:

```bash
git add <files>
git commit -m "fix: harden encrypted token replay"
```

**Step 5: Run full final gates**

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked backend::runtime --lib -- --nocapture
cargo test --locked idempotency --lib -- --nocapture
cargo test --locked server::idempotency --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo test --locked --features postgres backend::postgres_migrations --lib -- --nocapture
cargo test --locked --test server_startup durable -- --nocapture
cargo test --locked --features postgres --test server_startup durable -- --nocapture
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
```

If live credentials are present locally, also run required live gates. If not, record that protected-main CI has already produced a provider-verified green run and do not block on manual local live verification.

**Step 6: Push and integrate**

Only after review fixes and final gates:

```bash
git status --short --branch
git push origin v2/foundation
```

Then merge to main from the main worktree without reverting unrelated dirty files. If local main still has unrelated changes, either preserve them through a normal merge if Git allows it, or coordinate before stashing/checking out.

