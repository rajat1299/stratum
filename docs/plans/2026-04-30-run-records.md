# Run Records Foundation Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add the Phase 1 execution foundation from `docs/execution-roadmap.md`: durable, workspace-native run records under `/runs/<run-id>/`, without executing commands or adding a job runner.

**Architecture:** Introduce a small run-record model that defines the canonical layout and safe run-id validation. Add HTTP routes that require workspace-mounted bearer auth, write the standard run files through existing `StratumDb` scoped write APIs, and return workspace-relative paths. Existing mounted session scope and Unix permissions remain the security boundary.

**Tech Stack:** Rust, Tokio, Axum, serde, uuid, existing `Session`, `StratumDb`, and mounted workspace HTTP auth.

---

### Task 1: Core Run Record Model

**Files:**
- Add: `src/runs.rs`
- Modify: `src/lib.rs`

**Requirements:**
- Define constants for the reserved run root and standard files:
  - `/runs/<run-id>/prompt.md`
  - `/runs/<run-id>/command.md`
  - `/runs/<run-id>/stdout.md`
  - `/runs/<run-id>/stderr.md`
  - `/runs/<run-id>/result.md`
  - `/runs/<run-id>/metadata.md`
  - `/runs/<run-id>/artifacts/`
- Add safe run-id validation:
  - allow only ASCII letters, digits, `_`, and `-`
  - reject empty, `.`/`..`, slash-containing, or overlong IDs
  - generate UUID-based IDs when the request omits one
- Add a pure model for request content and generated file paths/content.
- Metadata should include at least run id, workspace id, agent uid, agent username, created timestamp, optional exit code, optional source commit, and optional started/ended timestamps.
- Add unit tests for ID validation, layout paths, default empty output files, and metadata content.

**Verification:**

```bash
cargo test --locked runs::tests -- --nocapture
```

**Commit:**

```bash
git add src/runs.rs src/lib.rs
git commit -m "feat: add run record model"
```

---

### Task 2: HTTP Run Record Routes

**Files:**
- Add: `src/server/routes_runs.rs`
- Modify: `src/server/mod.rs`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/execution-roadmap.md` only if the implementation changes documented Phase 1 semantics

**Requirements:**
- Add `POST /runs`.
- Require a workspace-mounted session. Plain `User root`, global bearer tokens, and malformed workspace auth must not create run records.
- Request body:
  - `run_id` optional
  - `prompt` required
  - `command` required
  - `stdout`, `stderr`, and `result` optional and default to empty strings
  - `exit_code`, `source_commit`, `started_at`, and `ended_at` optional metadata
- Write the canonical layout inside the mounted workspace using existing scoped DB methods:
  - create `/runs/<run-id>/artifacts/`
  - write the six markdown files
- Return `201 Created` with workspace-relative paths.
- On DB errors, project/redact mounted paths before returning JSON errors.
- If any write fails, return non-2xx. Full multi-file atomicity is not required in this slice.
- Add tests that:
  - workspace bearer auth creates the full record layout under the workspace root
  - response paths are workspace-relative
  - unsafe run IDs are rejected before writes
  - unmounted/global auth is rejected
  - insufficient write scope under `/runs` is rejected without leaking backing paths

**Verification:**

```bash
cargo test --locked server::routes_runs::tests -- --nocapture
```

**Commit:**

```bash
git add src/server/routes_runs.rs src/server/mod.rs docs/http-api-guide.md docs/execution-roadmap.md
git commit -m "feat: create workspace run records over http"
```

---

### Task 3: Review Fixes And Full Verification

**Files:**
- Modify only files needed for reviewer findings.

**Requirements:**
- Dispatch a fresh spec reviewer to compare the implementation against this plan.
- Dispatch a fresh code-quality/security reviewer for path traversal, mounted path projection, partial-write behavior, and auth boundaries.
- Fix reviewer findings and commit review fixes separately.

**Verification:**

```bash
cargo test --locked
git diff --check HEAD~3..HEAD
rustfmt --edition 2024 --check src/runs.rs src/server/routes_runs.rs src/server/mod.rs
```

**Commit:**

```bash
git add <changed-files>
git commit -m "fix: address run record review findings"
```
