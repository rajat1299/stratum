# Pre-Slice 3 Backend Follow-Ups Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the SDK package-boundary, live-gate credential-status, and typed capability-banner backend follow-ups before Slice 3 starts.

**Architecture:** Keep generated TypeScript SDK `dist/` out of git, but make package installs/builds produce it automatically through package lifecycle scripts that do not require Bun specifically. Keep live Postgres/R2 CI wired and fail-closed in protected contexts while documenting that the jobs are wired but not provider-verified until repo/environment secrets are provisioned and scheduled/protected runs pass. Tighten the Rust-owned capability manifest v1 banner from untyped JSON to a closed `info | warn` shape, mirror that type in both SDKs, and document the v1/v2 shape policy.

**Tech Stack:** Rust 2024, Axum, Serde, GitHub Actions, Bash, TypeScript SDK, Python TypedDict SDK, SDK contract fixtures, Cargo, Bun/npm-compatible package lifecycle scripts.

---

## Reference Material Used

- `/Users/rajattiwari/virtualfilesystem/lattice/docs/plans/2026-05-15-capability-manifest-v1-lock.md`
- `/Users/rajattiwari/virtualfilesystem/lattice/docs/plans/2026-05-15-backend-roadmap.md`
- `docs/project-status.md`
- `docs/http-api-guide.md`
- `docs/plans/2026-05-15-live-gates-in-ci.md`
- `.github/workflows/rust-ci.yml`
- `scripts/ci-live-postgres-gate.sh`
- `scripts/ci-live-r2-gate.sh`
- `src/server/routes_capabilities.rs`
- `sdk/typescript/package.json`
- `sdk/typescript/src/types.ts`
- `sdk/typescript/tests/client.test.ts`
- `sdk/python/src/stratum_sdk/types.py`
- `sdk/python/tests/test_client.py`
- `sdk/contracts/capabilities.v1.json`
- `sdk/contracts/capabilities.v1.durable-cloud.json`

## Current Baseline

- `sdk/typescript/dist/` is intentionally ignored, while `@stratum/sdk` exports `./dist/index.js` and `./dist/index.d.ts`.
- `sdk/typescript/package.json` has `build`, `prepack`, `typecheck`, and test scripts. `prepack` currently shells through Bun, which is brittle for npm/pnpm package consumption.
- Live-gate wrappers exist and fail closed in `STRATUM_LIVE_GATE_REQUIRED=1`, but the workflow currently names GitHub secrets with a `STRATUM_LIVE_*` prefix while the team follow-up asks repo admins to provision the standard test env names.
- Live Postgres and R2 providers have not been verified with real repo/environment secrets. The status should say "wired but not provider-verified."
- `CapabilityHints.banner` is `Option<serde_json::Value>` in Rust, `unknown | null` in TypeScript, and `object | None` in Python.
- SDK tests read checked-in capability fixtures, but some fixture assertions still reference older revision/route expectations and should be brought back in sync while touching the manifest.

## Task 1: Plan And Commit The Follow-Up Boundary

**Files:**
- Create: `docs/plans/2026-05-16-pre-slice3-backend-followups.md`

**Step 1: Save this plan**

Write this plan before implementation so the follow-up scope is explicit.

**Step 2: Verify and commit**

Run:

```bash
git diff --check
```

Commit:

```bash
git add docs/plans/2026-05-16-pre-slice3-backend-followups.md
git commit -m "docs: plan pre-slice3 backend followups"
```

## Task 2: Make `@stratum/sdk` Build Its Ignored Dist Reliably

**Files:**
- Modify: `sdk/typescript/package.json`
- Test: `sdk/typescript/tests/client.test.ts` or a focused package-boundary test if useful
- Optional docs: `docs/project-status.md`

**Step 1: Add a package-consumption build lifecycle**

Keep the `dist` export and ignored output model, but add a package lifecycle build that works under npm, pnpm, and Bun:

```json
{
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "prepare": "tsc -p tsconfig.json",
    "prepack": "tsc -p tsconfig.json"
  }
}
```

Do not make lifecycle scripts call `bun run build`; the frontend workspace uses pnpm and should not need Bun merely to materialize `@stratum/sdk/dist`.

**Step 2: Add or update a focused test**

Assert the package boundary remains intentional:

- package export points at `./dist/index.js` and `./dist/index.d.ts`
- `files` includes only `dist`
- `prepare` and `prepack` exist and invoke the TypeScript build directly or through the package build script

**Step 3: Verify**

Run:

```bash
cd sdk
bun install --frozen-lockfile
bun run --cwd typescript typecheck
bun run --cwd typescript test:run
bun run --cwd typescript build
cd typescript
npm pack --dry-run
```

Expected:

- Typecheck and tests pass.
- Build creates ignored `sdk/typescript/dist/`.
- Dry-run pack includes package metadata and `dist`, not `src` as the published import boundary.

**Step 4: Commit**

```bash
git add sdk/typescript/package.json sdk/typescript/tests
git commit -m "fix: build sdk dist during package install"
```

## Task 3: Align Live-Gate Secret Contract And Status

**Files:**
- Modify: `.github/workflows/rust-ci.yml`
- Modify: `scripts/ci-live-postgres-gate.sh`
- Modify: `docs/http-api-guide.md`
- Modify: `docs/project-status.md`
- Modify: `docs/plans/2026-05-15-live-gates-in-ci.md` only if the historical plan's secret contract would otherwise conflict with current CI

**Step 1: Use the standard repo/environment secret names**

Map the live workflow to the standard names requested by the team:

```yaml
STRATUM_POSTGRES_TEST_URL: ${{ secrets.STRATUM_POSTGRES_TEST_URL }}
STRATUM_POSTGRES_TEST_PASSWORD: ${{ secrets.STRATUM_POSTGRES_TEST_PASSWORD }}
PGPASSWORD: ${{ secrets.STRATUM_POSTGRES_TEST_PASSWORD }}
STRATUM_R2_BUCKET: ${{ secrets.STRATUM_R2_BUCKET }}
STRATUM_R2_ENDPOINT: ${{ secrets.STRATUM_R2_ENDPOINT }}
STRATUM_R2_ACCESS_KEY_ID: ${{ secrets.STRATUM_R2_ACCESS_KEY_ID }}
STRATUM_R2_SECRET_ACCESS_KEY: ${{ secrets.STRATUM_R2_SECRET_ACCESS_KEY }}
```

`STRATUM_POSTGRES_TEST_PASSWORD` remains optional for providers that use password auth, because `STRATUM_POSTGRES_TEST_URL` must stay password-free. Missing URL must fail closed in required live mode; a bad or insufficiently credentialed URL must fail as a live provider failure with redacted logs.

**Step 2: Loosen the Postgres wrapper's missing-env check**

Required live Postgres configuration should require `STRATUM_POSTGRES_TEST_URL`. Keep masking `STRATUM_POSTGRES_TEST_PASSWORD`, `PGPASSWORD`, `PGPASSFILE`, and `PGSERVICE` when present. Preserve the migration script's password-bearing URL rejection.

**Step 3: Document current provider status**

In `docs/http-api-guide.md` and `docs/project-status.md`, list required repo/environment secrets:

- `STRATUM_POSTGRES_TEST_URL`
- `STRATUM_R2_BUCKET`
- `STRATUM_R2_ENDPOINT`
- `STRATUM_R2_ACCESS_KEY_ID`
- `STRATUM_R2_SECRET_ACCESS_KEY`

Also document optional `STRATUM_POSTGRES_TEST_PASSWORD` for password-auth databases. State explicitly that live gates are wired but not provider-verified until scheduled/protected runs pass with real secrets.

**Step 4: Verify**

Run:

```bash
bash -n scripts/ci-live-postgres-gate.sh scripts/ci-live-r2-gate.sh scripts/check-postgres-migrations.sh scripts/check-r2-object-store.sh
STRATUM_LIVE_GATE_REQUIRED=0 STRATUM_POSTGRES_TEST_URL= ./scripts/ci-live-postgres-gate.sh
STRATUM_LIVE_GATE_REQUIRED=1 STRATUM_POSTGRES_TEST_URL= ./scripts/ci-live-postgres-gate.sh
STRATUM_LIVE_GATE_REQUIRED=0 STRATUM_R2_TEST_ENABLED= ./scripts/ci-live-r2-gate.sh
STRATUM_LIVE_GATE_REQUIRED=1 env -u STRATUM_R2_BUCKET ./scripts/ci-live-r2-gate.sh
git diff --check
```

Expected:

- Optional mode skips.
- Required mode fails closed with fixed, secret-safe messages when required configuration is missing.
- Docs no longer imply provider-verified live green.

**Step 5: Commit**

```bash
git add .github/workflows/rust-ci.yml scripts/ci-live-postgres-gate.sh docs/http-api-guide.md docs/project-status.md docs/plans/2026-05-15-live-gates-in-ci.md
git commit -m "ci: align live gate secret contract"
```

## Task 4: Type Capability Manifest Banner

**Files:**
- Modify: `src/server/routes_capabilities.rs`
- Modify: `sdk/typescript/src/types.ts`
- Modify: `sdk/typescript/tests/client.test.ts`
- Modify: `sdk/python/src/stratum_sdk/types.py`
- Modify: `sdk/python/tests/test_client.py`
- Modify: `sdk/contracts/capabilities.v1.json`
- Modify: `sdk/contracts/capabilities.v1.durable-cloud.json`
- Modify: `docs/http-api-guide.md`

**Step 1: Add Rust banner types**

Add closed, serde-owned types:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BannerKind {
    Info,
    Warn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Banner {
    pub kind: BannerKind,
    pub text: String,
}
```

Add a constructor that bounds `text` to 280 Unicode scalar values at construction time. Use the constructor anywhere a non-null banner is created.

Change:

```rust
pub banner: Option<serde_json::Value>
```

to:

```rust
pub banner: Option<Banner>
```

**Step 2: Add Rust tests**

Add tests proving:

- `Banner::new(BannerKind::Info, text)` serializes to `{"kind":"info","text":"..."}`
- `Banner::new(BannerKind::Warn, long_text)` stores at most 280 characters
- manifest serde round-trip still works and fixtures stay generated from Rust-owned types

**Step 3: Update SDK types and fixture assertions**

TypeScript:

```ts
export type CapabilityBannerKind = "info" | "warn";
export interface CapabilityBanner {
  readonly kind: CapabilityBannerKind;
  readonly text: string;
}
export interface CapabilityHints {
  readonly banner: CapabilityBanner | null;
  readonly branding: unknown | null;
  readonly support_url: string | null;
}
```

Python:

```python
class CapabilityBanner(TypedDict):
    kind: Literal["info", "warn"]
    text: str

class CapabilityHints(TypedDict):
    banner: CapabilityBanner | None
    branding: object | None
    support_url: str | None
```

Update TS/Python fixture tests to assert `banner is null` in current fixtures and to match the current revision/durable-cloud filesystem mutation shape.

**Step 4: Regenerate fixtures**

Run:

```bash
STRATUM_UPDATE_CAPABILITY_FIXTURES=1 \
  cargo test --locked server::routes_capabilities::tests::update_checked_in_sdk_contract_fixture_when_requested --lib -- --nocapture
```

Do not edit frontend mirror fixtures in the main worktree as part of this backend branch unless they are tracked in this branch. Record in the final handoff that frontend mirror fixtures must be intentionally regenerated because the backend contract type changed.

**Step 5: Document v1/v2 shape policy**

In `docs/http-api-guide.md`, document:

- `hints.banner` shape is `{ "kind": "info" | "warn", "text": string } | null`
- banner text is server-bounded to 280 characters
- no markdown, action URLs, or extra keys are part of v1
- additive optional fields can bump manifest `revision`
- renames/removals/type changes/enum widening under v1 are breaking and require `/v2/capabilities`
- `GET /v1/capabilities` remains live for at least 60 days after a v2 ships

**Step 6: Verify**

Run:

```bash
cargo fmt --all -- --check
cargo test --locked server::routes_capabilities --lib -- --nocapture
cd sdk
bun run --cwd typescript typecheck
bun run --cwd typescript test:run
cd python
python -m pytest tests/test_client.py
python -m mypy src/stratum_sdk
git diff --check
```

**Step 7: Commit**

```bash
git add src/server/routes_capabilities.rs sdk/typescript/src/types.ts sdk/typescript/tests/client.test.ts sdk/python/src/stratum_sdk/types.py sdk/python/tests/test_client.py sdk/contracts docs/http-api-guide.md
git commit -m "feat: type capability manifest banner"
```

## Task 5: Final Review And Verification

**Files:**
- All changed files

**Step 1: Request spec/correctness review**

Review against the team message:

- SDK package boundary is not ad hoc.
- Live secrets are named and status says wired but not provider-verified.
- Typed banner is closed and bounded.
- v1/v2 policy is documented.
- Frontend drift test is not bypassed.

**Step 2: Request code-quality/security review**

Review for:

- secret-safe CI/doc behavior
- shell-script missing-env logic
- package lifecycle portability
- manifest compatibility and v1-breaking implications

**Step 3: Fix review findings locally**

Inspect every diff and make required fixes in the main session.

**Step 4: Run final gates**

Run:

```bash
cargo fmt --all -- --check
git diff --check
bash -n scripts/check-postgres-migrations.sh scripts/check-r2-object-store.sh scripts/ci-live-postgres-gate.sh scripts/ci-live-r2-gate.sh
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
cargo test --locked server::routes_capabilities --lib -- --nocapture
cargo test --locked --features postgres backend::postgres --lib -- --nocapture
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --all-targets --features postgres -- -D warnings
cargo test --locked --lib --tests
cargo audit --deny warnings
cd sdk
bun install --frozen-lockfile
bun run --cwd typescript typecheck
bun run --cwd typescript test:run
bun run --cwd typescript build
cd typescript
npm pack --dry-run
cd ../python
python -m pytest
python -m mypy src/stratum_sdk
python -m ruff check src tests
python -m ruff format --check src tests
```

If real live credentials are available, also run:

```bash
STRATUM_LIVE_GATE_REQUIRED=1 ./scripts/ci-live-postgres-gate.sh
STRATUM_LIVE_GATE_REQUIRED=1 ./scripts/ci-live-r2-gate.sh
```

If live credentials are unavailable, record that provider verification remains pending.

**Step 5: Push and merge**

```bash
git status --short --branch
git push origin v2/foundation
cd /Users/rajattiwari/virtualfilesystem/lattice
git status --short --branch
git merge --no-ff v2/foundation
git push origin main
```

Preserve unrelated untracked files in the main worktree.
