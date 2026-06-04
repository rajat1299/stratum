# Backend Engineering Workflow Scratchpad

Last updated: 2026-06-04

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
