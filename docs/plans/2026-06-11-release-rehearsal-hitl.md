# Task 16 / Release Rehearsal HITL

**Goal:** Run the private-beta release rehearsal against the checked-in local
golden path, record the evidence boundary, and keep hosted durable rehearsal
blocked rather than simulated when operator credentials are unavailable.

## Result

Local rehearsal passed on 2026-06-11 using a throwaway
`/tmp/stratum-release-rehearsal.*` data directory and
`CARGO_TARGET_DIR=/tmp/stratum-target-task16`. Raw CLI token output stayed in
temporary logs and was removed by the cleanup trap.

Hosted durable rehearsal was not run. Required operator state was not available:
Cloudflare deploy target, Postgres/R2 credentials, tenant/repo context, and a
repo-bound hosted admin bearer.

## Fixes From Rehearsal

- `workspace seed-demo` chmods the demo workspace root after root creates it so
  the scoped workspace token can seed and later write under the projected root.
- Metadata PATCH accepts a bounded octal `mode` field and reports redacted
  changed booleans.
- The incident agent example uses concrete baseline/update commit IDs for diff
  preview.
- `seed-demo` writes `STRATUM_WORKSPACE_ROOT` and the agent example emits
  `reviewPaths` for exact change-request file-view evidence.
- `scripts/run-local-golden-path-demo.sh` handles empty optional repo headers
  under Bash `set -u`.
- Demo docs now match the actual `addagent` token transcript and `reviewPaths`
  flow.

## Verification

- `scripts/run-local-golden-path-demo.sh` passed end to end in the temp local
  rehearsal.
- Focused Rust, TypeScript, and script checks are recorded in
  `docs/project-status.md`.
