# Backend Engineering Workflow Scratchpad

Last updated: 2026-06-06

## 2026-06-06 Shipping Closeout Continuation

### Current Worktree Decision

- Use clean detached worktree `/private/tmp/lattice-push.wONPfE` at `origin/main` (`4ca0f4a`) for verification and any final integration changes.
- Leave original `/Users/rajattiwari/virtualfilesystem/lattice` worktree untouched because it is dirty and still intentionally diverged from `origin/main`.
- Source live-gate environment from the original `.env.live-gates` only inside gate commands; do not print secret values.

### Closeout Checklist

- [ ] Run live Postgres gate when credentials are present.
- [x] Run live R2/object-store gate when credentials are present.
- [x] Run local formatting/diff checks.
- [x] Run Rust test gates and release perf gate; record concrete numbers.
- [x] Review read-only subagent findings for performance and recovery/audit ship risks.
- [x] Implement only small, low-risk optimizations or correctness fixes with focused verification.
- [x] Document any blocked live gates, residual ship risks, and final performance posture.

### Subagent Assignments

- Performance explorer: inspect hot paths and perf tests for safe final optimizations.
- Recovery/audit explorer: inspect terminal recovery reasons, repo-scoped audit sequencing, live Postgres behavior, and redaction/partial-failure risks.

### Closeout Results

- Live R2/object-store gate passed.
- Live Postgres gate was blocked by the configured database project limit: Postgres reported the 512 MB project size limit was exceeded.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh`: skipped live SQL because the test URL was intentionally unset for local verification.
- `cargo test --locked --all-targets`: passed, including library `1370/1370`, integration `143/143`, debug perf `38/38`, permissions `72/72`, and server startup `28/28`.
- `cargo test --locked --release --test perf -- --test-threads=1 --nocapture`: passed `38/38` in `0.94s`.
- Performance change: `perf_commit_large_tree_single_file_change` baseline `245.15ms total / 24.51ms/op`; final `194.47ms total / 19.45ms/op`, about `20.7%` faster. `perf_commit_10k_files` final `20.28ms`.

## Goal

Complete a backend engineering review and improvement pass for Stratum, maintain `docs/backend-engineering-running-log.md`, inspect SMFS and Mirage for borrowable ideas, implement clear scoped fixes with focused tests, and keep commits clean.

## Guardrails

- Do not touch unrelated dirty user work.
- Prefer read-only subagents for parallel audit slices; keep final edits and commits centralized unless a later implementation task has a disjoint write set.
- Preserve existing behavior unless fixing a clear bug.
- Record uncertain or blocked issues with evidence rather than guessing.
- Run focused tests for each fix and broader gates before finalizing.

## Work Plan

- [x] Establish Stratum product/backend architecture context from docs and source.
- [x] Audit durable backend, migrations, object cleanup, idempotency, and recovery.
- [x] Audit server routes, auth/policy/redaction, API seams, SDK contracts, and operational behavior.
- [x] Inspect SMFS and Mirage for borrowable backend/product/devex patterns.
- [x] Update the running log with factual findings and suggested actions.
- [x] Implement small, clear fixes where the evidence and scope are strong.
- [x] Run focused tests after each committed change.
- [x] Commit each scoped change separately, excluding unrelated dirty user work.
- [x] Run broader verification gates before final response.

## Subagent Assignments

- Durable backend/migrations/security-critical slice: completed by read-only subagent; findings summarized in running log.
- Server/auth/API/SDK seam slice: completed by read-only subagent; findings summarized in running log.
- SMFS/Mirage borrowable-pattern slice: completed by read-only subagent; findings summarized in running log.
- Whole-product performance slice: completed by read-only subagent; main findings summarized in the running log. Local fixes covered matcher compilation, borrowed path components, and tree traversal allocation.
