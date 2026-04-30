# Run Status And Read API Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Start Execution Roadmap Phase 2 by adding a durable run status model and read APIs for existing run records, without executing commands, scheduling jobs, or implementing cancellation.

**Architecture:** Keep run records workspace-native under `/runs/<run-id>/`. Extend `metadata.md` with a validated status field and helper model APIs. Add workspace-mounted HTTP read routes that project mounted paths back to workspace-relative responses and reuse existing scoped DB reads. Creation remains append-only by run id.

**Non-goals for this slice:**
- no command runner
- no queue or background worker
- no stdout/stderr streaming
- no cancellation mutation
- no transaction layer

---

## Task 1: Add Run Status Model

**Files:**
- Modify: `src/runs.rs`
- Modify: `docs/execution-roadmap.md` if semantics need clarification

**Requirements:**
- Add a `RunStatus` enum with the roadmap states:
  - `queued`
  - `running`
  - `succeeded`
  - `failed`
  - `cancelled`
  - `timed_out`
- Add `status` to run metadata.
- Creation defaults status to `queued` unless the request provides an explicit status.
- Keep serde wire values lowercase snake case.
- Metadata frontmatter must remain YAML-safe.
- Add tests for default status, explicit status, serialization/deserialization, and metadata output.

**Verification:**

```bash
cargo test --locked runs::tests -- --nocapture
```

**Commit:**

```bash
git add src/runs.rs docs/execution-roadmap.md
git commit -m "feat: add run status model"
```

---

## Task 2: Add Run Read APIs

**Files:**
- Modify: `src/server/routes_runs.rs`
- Modify: `docs/http-api-guide.md`

**Requirements:**
- Add:
  - `GET /runs/{id}`
  - `GET /runs/{id}/stdout`
  - `GET /runs/{id}/stderr`
- Require a workspace-mounted session for all run routes.
- Validate `{id}` with the same safe run-id rules as creation.
- Resolve all paths through the session mount before DB reads.
- Use scoped DB read APIs only.
- `GET /runs/{id}` returns JSON with workspace-relative root/artifacts/file paths and file metadata/content summary sufficient for clients to discover the record. It must not leak the backing workspace root.
- `GET /runs/{id}/stdout` and `/stderr` return raw markdown/plain text content.
- Missing runs return `404`.
- Read-scope failures return `403` without leaking backing paths.
- Add tests for success, missing run, unsafe id, unmounted auth rejection, insufficient read scope, and path projection.

**Verification:**

```bash
cargo test --locked server::routes_runs::tests -- --nocapture
```

**Commit:**

```bash
git add src/server/routes_runs.rs docs/http-api-guide.md
git commit -m "feat: read workspace run records over http"
```

---

## Task 3: Review And Verification

**Files:**
- Modify only files required by review findings.

**Requirements:**
- Dispatch a fresh spec reviewer against this plan.
- Dispatch a fresh quality/security reviewer for auth, path projection, status semantics, and path leak regressions.
- Fix reviewer findings and commit separately.

**Verification:**

```bash
cargo test --locked
git diff --check HEAD~3..HEAD
rustfmt --edition 2024 --check src/runs.rs src/server/routes_runs.rs
```

**Commit:**

```bash
git add <changed-files>
git commit -m "fix: address run status review findings"
```
