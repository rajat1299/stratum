# Policy Enforcement Below Route Layer Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move protected ref/path decisions from HTTP-only helpers into a shared policy decision seam required by guarded durable mutation, ref, and review execution paths.

**Architecture:** Keep existing HTTP status/body behavior while converting route policy evaluation into a reusable decision plus token that lower durable execution APIs must receive before protected-aware mutations. Local runtime and broad `STRATUM_CORE_RUNTIME=durable-cloud` stay unchanged and fail-closed where they are already fail-closed.

**Tech Stack:** Rust, Axum, Tokio, existing `ReviewStore`, `AuditStore`, guarded durable `CoreDb`/`GuardedDurableCommitRoute`, durable mutation/ref stores, existing route idempotency and recovery helpers.

---

## Reference Material

- `/Users/rajattiwari/virtualfilesystem/lattice/markdownfs_v2_cto_architecture_plan.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/project-status.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/.worktrees/v2-foundation/docs/plans/2026-05-10-durable-auth-session-routing-foundation.md`
- `/Users/rajattiwari/virtualfilesystem/extract pieces.md`
- Mirage VFP capability vocabulary, shape only: `vfp/types.ts`, `vfp/capability.ts`, `2026-05-04-vfp-extraction-design.md`

## Current Baseline

- `src/server/policy.rs` evaluates protected ref/path decisions and builds redacted policy audit details.
- FS, VCS, and review HTTP routes call route helpers before mutation and append allow/deny policy audit events.
- Guarded durable FS mutation methods in `GuardedDurableCommitRoute` and `DurableCoreRuntime` can currently execute through method calls that do not require a policy token.
- Durable ref create/update and durable review merge/ref advancement still trust the caller to have performed route policy checks.
- MCP and FUSE direct mutation parity is out of scope; this slice must not claim those surfaces are protected by the new seam.

## Non-Goals

- Do not enable broad `STRATUM_CORE_RUNTIME=durable-cloud`.
- Do not build MCP/FUSE durable policy parity.
- Do not introduce an external policy engine, reviewer groups, code owners, tenant routing, or a web console.
- Do not change local runtime behavior except for compiling through shared types.
- Do not persist file content, raw paths beyond existing bounded denial context, raw tokens, idempotency keys, database URLs, object-store errors, approval/comment bodies, or commit messages in policy audit details.

## Task 1: Promote Route Policy Into Shared Decision Types

**Files:**
- Modify: `src/server/policy.rs`
- Modify if needed: `src/audit.rs`
- Test: `src/server/policy.rs`

**Steps:**

1. Rename or wrap `RoutePolicyAction`, `RoutePolicyActor`, `RoutePolicyCorrelation`, `RoutePolicyRequest`, `RoutePolicyDecision`, and `RoutePolicyDecisionDetails` into shared names such as `PolicyAction`, `PolicyActor`, `PolicyCorrelation`, `PolicyRequest`, `PolicyDecision`, and `PolicyDecisionDetails`.
2. Keep compatibility aliases if that makes route refactor smaller, but new lower APIs should depend on the shared names.
3. Extend details to include bounded shared fields required below the route layer: actor uid and username presence, repo id, workspace id, target ref, action, changed-path count, matched ref/path rule counts, allow/deny, reason code, session/workspace/ref presence, correlation/idempotency presence, and optional change request id.
4. Add `PolicyDecisionToken`, created only from an allowed `PolicyEvaluation` or an approved review merge evaluation. The token should expose only bounded details/counts and private path digests, not raw changed paths or actor names.
5. Add a fail-closed helper such as `PolicyDecisionToken::require_allowed_for(action, target_ref)` returning `VfsError` on missing, denied, wrong action, or wrong target ref.
6. Keep `audit_event_from_policy_evaluation()` deterministic and content-free.
7. Add focused tests proving deny reasons/details remain redacted, allow tokens cannot be created from denied decisions, action/target mismatch fails, and review-approved merge tokens carry matched rule counts.

Run:

```bash
cargo test --locked server::policy --lib -- --nocapture
```

## Task 2: Require Policy Tokens In Durable FS Mutation Execution

**Files:**
- Modify: `src/backend/durable_mutation.rs`
- Modify: `src/server/core.rs`
- Test: `src/backend/durable_mutation.rs`
- Test: `src/server/routes_fs.rs`

**Steps:**

1. Add a policy-token field to `DurableMutationInput`, or change `DurableMutationEngine::apply()` to take an explicit `&PolicyDecisionToken`.
2. Require the token before any object write, commit metadata insert, or ref CAS in `DurableMutationEngine::apply()`.
3. Validate token repo/action/target against `base_ref`, the operation kind, and the exact policy path scope. Copy/move must use effective destination paths, and recursive delete or directory move must require descendant-aware token scope.
4. Keep preflight permission checks, cleanup-claim behavior, redacted `Debug`, and CAS-lost cleanup unchanged.
5. Update `DurableCoreRuntime` guarded mutation methods to accept/pass a token for `write`, `mkdir`, `delete`, `copy`, `move`, and metadata update.
6. Update route calls in `routes_fs.rs` to pass the token created from the already audited policy evaluation into the guarded durable output methods.
7. Add lower-level tests proving durable mutation fails before object writes/commit/ref update when no token or a denied/mismatched token is supplied.
8. Keep local `StratumDb` mutation methods token-free so local runtime behavior is unchanged.

Run:

```bash
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
```

## Task 3: Require Policy Tokens In Durable VCS Ref, Commit, And Revert Paths

**Files:**
- Modify: `src/server/core.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify if needed: `src/backend/core_transaction.rs`
- Test: `src/server/routes_vcs.rs`
- Test if touched: `src/backend/core_transaction.rs`

**Steps:**

1. Add policy-token-taking guarded durable methods for ref create/update. Direct durable ref update must require a token for `VcsRefUpdate`; ref create should continue matching existing HTTP behavior.
2. Add token requirements before guarded durable commit promotion and durable revert execution. The route should compute durable changed paths before mutation, audit the policy decision before mutation, and pass the resulting token into the lower durable path.
3. Keep protected ref denial before durable revert path checks, preserving existing status/body ordering.
4. Ensure durable revert uses `DurableCoreRevertPlan::changed_path_strings()` as the changed-path source before mutation.
5. Preserve post-CAS and pre-visibility recovery behavior; only pre-mutation policy/audit failure should fail closed normally.
6. Add tests proving direct lower durable ref update/revert cannot mutate without an allow token, and existing HTTP protected-ref/path tests keep their status/body/audit semantics.

Run:

```bash
cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture
```

## Task 4: Require Policy Tokens In Durable Review Merge

**Files:**
- Modify: `src/server/routes_review.rs`
- Modify: `src/server/core.rs`
- Test: `src/server/routes_review.rs`

**Steps:**

1. Keep approval-policy computation as the source of review-approved context.
2. Convert the review merge policy evaluation into a review-approved token only when approval state matches the recorded source/target/head and required rules.
3. Require that token before the durable target ref CAS used by review merge, including the target ref, change request id, and approved changed-path scope.
4. Keep reject behavior unchanged except for shared policy type naming.
5. Preserve existing merge response and terminal-state/idempotency ordering.
6. Add tests proving durable review merge cannot advance the protected target ref without an approved policy token, and that protected path changed paths are computed from durable commit metadata before mutation.

Run:

```bash
cargo test --locked server::routes_review::tests --lib -- --nocapture
```

## Task 5: Audit And Redaction Pass

**Files:**
- Modify: `src/server/policy.rs`
- Modify: `src/server/routes_fs.rs`
- Modify: `src/server/routes_vcs.rs`
- Modify: `src/server/routes_review.rs`
- Modify if needed: `src/audit.rs`

**Steps:**

1. Ensure every protected-aware allow/deny decision is appended before durable mutation/ref execution.
2. Make audit append failure before mutation fail closed.
3. Do not change existing recovery-managed post-visible audit failure paths.
4. Verify redacted policy details are deterministic and contain counts/presence flags, not file content or secret-bearing values.
5. Add or update tests that serialize policy audit events and assert absence of file content, idempotency key values, raw tokens, and object-store/database details.

Run:

```bash
cargo test --locked server::policy --lib -- --nocapture
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture
```

## Task 6: Documentation And Cutover Boundary

**Files:**
- Modify: `docs/project-status.md`
- Modify if useful: `docs/plans/2026-05-10-broad-durable-runtime-auth-policy-cutover-planning.md`

**Steps:**

1. Update status to say HTTP guarded durable FS/VCS/review mutation execution now requires a shared policy decision token below route helpers.
2. State explicitly that MCP/FUSE direct durable mutation parity is still not implemented and those surfaces must remain local-only or disabled for durable-cloud.
3. State explicitly that broad durable runtime remains fail-closed.

Run:

```bash
git diff --check
```

## Required Reviews

1. Spec/correctness review focus:
   - No bypass path from guarded durable mutation/ref/review execution around the policy token.
   - Existing HTTP protected ref/path behavior is unchanged.
   - Protected path changed paths are computed before durable commit/revert/review merge mutation.
   - Broad durable runtime remains fail-closed.

2. Code-quality/security review focus:
   - No raw paths beyond bounded existing error context.
   - No file content, token, idempotency key, DB URL, object-store error, approval/comment body, or commit message leakage.
   - Token API is small and hard to misuse.
   - Local runtime, MCP, and FUSE behavior are not falsely represented as protected by this slice.

## Final Verification

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked server::policy --lib -- --nocapture
cargo test --locked server::routes_fs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_vcs::tests::guarded_durable --lib -- --nocapture
cargo test --locked server::routes_review::tests --lib -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```
