# Run Creation Idempotency Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add the first reusable HTTP idempotency foundation and wire it into `POST /runs`, so execution clients can safely retry run-record creation without creating duplicate run records.

**Architecture:** Add a small durable idempotency store separate from `StratumDb` and workspace metadata. Store hashed idempotency keys, request fingerprints, and completed JSON responses. Keep raw keys out of persistence. Wire the store through `ServerState` so later slices can reuse it for file writes, commits, and workspace-token creation.

**Non-goals for this slice:**
- no idempotency for `/fs`, `/vcs/commit`, or workspace-token routes yet
- no distributed locking
- no transactional coupling between run file writes and idempotency completion
- no retention/expiry policy

---

## Task 1: Add Durable Idempotency Store

**Files:**
- Add: `src/idempotency.rs`
- Modify: `src/lib.rs`
- Modify: `src/config.rs`
- Modify: `src/server/mod.rs`
- Modify tests that construct `ServerState`

**Requirements:**
- Parse conventional `Idempotency-Key` header values with validation:
  - non-empty
  - at most 255 bytes
  - visible ASCII only
- Store only SHA-256 hashes of raw idempotency keys.
- Store completed responses as status code plus JSON body.
- Track in-process pending keys so concurrent duplicate requests return a conflict instead of both executing.
- Return conflict when a key is reused with a different request fingerprint.
- Add a durable local store at `<STRATUM_DATA_DIR>/.vfs/idempotency.bin`, with env override `STRATUM_IDEMPOTENCY_PATH`.
- Follow the workspace metadata store's atomic write pattern.
- Add tests for key validation, same-key replay, same-key different-request conflict, in-progress conflict, durable reload, corrupt store handling, and no live mutation on failed persist.

**Verification:**

```bash
cargo test --locked idempotency::tests -- --nocapture
```

**Commit:**

```bash
git add src/idempotency.rs src/lib.rs src/config.rs src/server/mod.rs src/server/*.rs
git commit -m "feat: add idempotency store foundation"
```

---

## Task 2: Wire `POST /runs`

**Files:**
- Modify: `src/server/routes_runs.rs`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

**Requirements:**
- `POST /runs` accepts optional `Idempotency-Key`.
- The request fingerprint must include:
  - route namespace
  - workspace id
  - authenticated agent uid
  - normalized JSON request body
- Same key plus same fingerprint replays the original completed `201 Created` JSON response and does not create another run directory.
- Same key plus different fingerprint returns `409 Conflict` without mutation.
- Duplicate run IDs without a matching idempotency replay keep existing `409 Conflict` behavior.
- Invalid idempotency keys return `400 Bad Request` before mutation.
- Replayed responses include `X-Stratum-Idempotent-Replay: true`.
- Add tests for omitted `run_id` replay, explicit `run_id` replay, different-body conflict, invalid key rejection, missing-key duplicate behavior, and no backing path leakage.

**Verification:**

```bash
cargo test --locked server::routes_runs::tests -- --nocapture
```

**Commit:**

```bash
git add src/server/routes_runs.rs docs/http-api-guide.md docs/project-status.md
git commit -m "feat: make run creation idempotent"
```

---

## Task 3: Review And Verification

**Files:**
- Modify only files required by review findings.

**Requirements:**
- Dispatch a fresh spec reviewer against this plan.
- Dispatch a fresh quality/security reviewer for key handling, persistence, replay behavior, conflict behavior, and secret/path leakage.
- Fix reviewer findings and commit separately.

**Verification:**

```bash
cargo test --locked
git diff --check HEAD~3..HEAD
rustfmt --edition 2024 --check src/idempotency.rs src/server/routes_runs.rs src/server/mod.rs src/config.rs
```

**Commit:**

```bash
git add <changed-files>
git commit -m "fix: address idempotency review findings"
```
