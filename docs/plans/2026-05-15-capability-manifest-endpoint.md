# Capability Manifest Endpoint Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `GET /v1/capabilities` as a safe unauthenticated server-owned manifest for frontend and SDK feature detection.

**Architecture:** Introduce a dedicated `src/server/routes_capabilities.rs` module that owns the manifest schema, deterministic manifest construction, and route handler. Wire it into both local-state and durable-cloud routers so the endpoint reports the mounted route surface truthfully without changing existing route behavior. Generate SDK contract fixtures from the Rust manifest response so TypeScript and Python tests validate the same checked-in shape instead of hand-maintained copies.

**Tech Stack:** Rust 2024, Axum, Serde, tower/oneshot route tests, TypeScript SDK Vitest, Python pytest, and repo docs.

---

### Task 1: Add Rust manifest route tests first

**Files:**
- Create: `src/server/routes_capabilities.rs`
- Modify: `src/server/mod.rs`

**Step 1: Write failing tests**

Add tests under `src/server/routes_capabilities.rs` for:
- local router `GET /v1/capabilities` returns `200` without auth and `Cache-Control: max-age=60, must-revalidate`.
- durable-cloud router `GET /v1/capabilities` returns `200` without auth.
- serde round-trip preserves the manifest.
- durable-cloud manifest marks FS/VCS mutations plus audit/workspace/review/runs as unavailable or unsupported while leaving durable read routes available.
- local-state route table surfaces advertise mounted local routes, including recovery only when served by the current router.

**Step 2: Run red test**

Run:

```bash
cargo test --locked server::routes_capabilities --lib -- --nocapture
```

Expected: fails because the module and route do not exist.

### Task 2: Implement server-owned manifest types and local/durable construction

**Files:**
- Modify: `src/server/routes_capabilities.rs`
- Modify: `src/server/mod.rs`

**Step 1: Implement minimal schema**

Define `CapabilityManifest` and nested `Serialize`/`Deserialize`/`PartialEq` structs for:
- `revision`
- `server`
- `auth`
- `routes`
- `diff`
- `protection`
- `idempotency`
- `recovery`
- `limits`
- `hints`

Keep values content-free: no paths, repo ids, tokens, request bodies, DB URLs, R2 endpoints, object keys, raw backend errors, commit messages, or per-user fields.

**Step 2: Implement route**

Expose:

```rust
pub fn routes() -> Router<AppState>;
pub(crate) fn manifest_for_state(state: &ServerState) -> CapabilityManifest;
```

The handler must always return a JSON body and cache header. Unknown or inconsistent state must fail closed inside the manifest values, not leak backend errors or return `5xx`.

**Step 3: Wire routers**

Merge `routes_capabilities::routes()` into:
- `build_router_with_stores_and_guarded_durable_commit`
- `build_durable_core_router`

Run:

```bash
cargo test --locked server::routes_capabilities --lib -- --nocapture
```

Expected: pass.

### Task 3: Generate SDK contract fixture from Rust

**Files:**
- Create: `sdk/contracts/capabilities.v1.json`
- Create: `sdk/contracts/capabilities.v1.durable-cloud.json`
- Modify: `sdk/typescript/src/types.ts`
- Modify: `sdk/typescript/src/client.ts`
- Test: `sdk/typescript/tests/*.test.ts`
- Modify: `sdk/python/src/stratum_sdk/types.py`
- Modify: `sdk/python/src/stratum_sdk/client.py`
- Test: `sdk/python/tests/*.py`

**Step 1: Add a single regeneration path**

Add a focused Rust test that writes deterministic local and durable-cloud manifest fixtures when `STRATUM_UPDATE_CAPABILITY_FIXTURES=1` is set. The checked-in `sdk/contracts/` fixtures are the source for SDK contract tests.

**Step 2: Add SDK types and client methods**

Add `getCapabilities()`/`get_capabilities()` helpers plus typed manifest shapes derived from the fixture fields.

**Step 3: Add SDK tests**

TypeScript and Python tests load the local and durable-cloud fixtures, assert required shape fields and unsupported durable-cloud surfaces, and verify client methods call `GET /v1/capabilities` without auth requirements.

### Task 4: Update docs

**Files:**
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`

Document:
- unauthenticated `GET /v1/capabilities`
- cache header
- manifest revision
- durable-cloud unsupported surfaces are explicit
- frontend can replace its mock with the server endpoint

### Task 5: Verification

Run focused gates first:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --locked server::routes_capabilities --lib -- --nocapture
cargo test --locked server::routes_auth::tests --lib -- --nocapture
cargo test --locked --test server_startup -- --nocapture
```

Then broader gates as time allows:

```bash
cargo test --locked --lib --tests
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cd sdk && bun run typecheck && bun run test:run
cd sdk/python && pytest
cargo audit --deny warnings
```

Do not commit, merge, or push.
