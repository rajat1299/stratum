# Guarded Live Durable VCS Commit Routing

Date: 2026-05-07
Branch: v2/foundation

## Goal

Wire a narrow, guarded durable execution path for `POST /vcs/commit` by composing the durable commit sequence that now exists internally:

1. metadata preflight;
2. object/tree write plan;
3. planned object convergence;
4. commit metadata insert;
5. ref CAS visibility;
6. post-CAS workspace-head, audit, and idempotency completion envelope.

This slice should make one commit route executable under explicit runtime gates while keeping the broad durable core runtime and all other durable FS/VCS route execution fail-closed.

## Current Facts

- `src/server/routes_vcs.rs` currently performs HTTP auth, protected-ref checks, idempotency reservation/replay, `state.core.commit_as`, workspace-head update, audit append, and idempotency completion in the route.
- `src/server/core.rs` keeps `DurableCoreRuntime::commit_as` fail-closed and the durable runtime has no live auth/session/filesystem route path.
- `src/backend/core_transaction.rs` already exposes the internal durable commit stages and the post-CAS envelope.
- `DurableCoreCommitObjectTreeWritePlan::build` still needs a `VirtualFs` source snapshot. Until durable FS mutation routing exists, the guarded route must treat the local `StratumDb` worktree snapshot as an explicit source prerequisite, not as full durable core cutover.
- `STRATUM_CORE_RUNTIME=durable-cloud` must remain rejected by startup until durable auth/session and FS source routing are separately designed.

## Scope

- Add a typed route capability for guarded durable commit routing, disabled by default.
- Route `POST /vcs/commit` through that capability only when present; otherwise preserve the current local route behavior.
- Compose the durable sequence in one place so route-level post-CAS side effects are not duplicated.
- Build the committed HTTP response before post-CAS completion and let the durable post-CAS envelope complete idempotency with either the committed response or the redacted partial response.
- Keep protected-ref checks and idempotency replay/conflict before durable mutation.
- Abort idempotency reservations for all failures before ref CAS visibility.
- Return sanitized CAS and durable-store errors without leaking message, author, path, raw idempotency token, R2 key, or Postgres detail.
- Keep `GET /vcs/log`, `POST /vcs/revert`, `GET /vcs/status`, `GET /vcs/diff`, FS routes, auth/session durable runtime methods, and `STRATUM_CORE_RUNTIME=durable-cloud` startup fail-closed.

## Non-Goals

- No durable auth/session path.
- No durable FS write/read routing.
- No background recovery worker or persisted post-CAS recovery claims in this slice.
- No broad durable route serving or `vcs_log` over durable commit metadata.
- No SMFS queue import; SMFS remains pattern input for the later persisted recovery worker.

## Test Plan

Add RED tests first around the route/capability seam:

- default durable commit capability absent preserves current local commit behavior;
- guarded durable commit route writes objects, commit metadata, main ref, workspace head, audit event, and idempotency replay response;
- same idempotency key replays after the durable ref moved and does not write a second commit or audit event;
- stale parent/ref CAS race aborts idempotency and returns sanitized conflict without workspace/audit/post-CAS mutation;
- post-CAS workspace-head failure returns the redacted committed partial replay body and does not roll back the visible ref;
- post-CAS audit failure and idempotency completion failure are surfaced as committed partial outcomes;
- durable route does not expose commit message, author, paths, raw idempotency reservation token, or backend error details in debug/error output;
- other durable runtime route methods and `STRATUM_CORE_RUNTIME=durable-cloud` startup remain fail-closed.

## Verification

Run focused route/core tests first, then the existing startup and durable core gates:

```sh
cargo test --locked server::routes_vcs::tests --lib -- --nocapture
cargo test --locked server::core::tests::durable_core_runtime --lib -- --nocapture
cargo test --locked server::tests::open_ --lib -- --nocapture
cargo test --locked --test server_startup durable_core_runtime -- --nocapture
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
sleep 10 && /usr/bin/time -l cargo test --locked --release --test perf -- --test-threads=1 --nocapture
```
