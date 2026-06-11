# SDK Beta Publishing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close Task 14 by making `@stratum/sdk`, `@stratum/agents`, and `stratum-sdk` publish-ready for private beta with a version matrix and CI package-boundary checks.

**Architecture:** Keep release execution manual and token-free in CI. Add a checked-in SDK version matrix, a provider-free publishing-boundary checker, and package tests that verify npm/PyPI metadata without publishing. Expand the existing SDK CI job so it builds, tests, and dry-runs package boundaries for both npm packages and builds the Python distributions into a temp directory.

**Tech Stack:** Bun/Vitest/TypeScript, npm pack dry-run, Python 3.11/Hatchling/pytest/build, GitHub Actions.

---

### Task 1: Add A Failing Publishing Contract Check

**Files:**
- Create: `sdk/scripts/check-publishing-boundaries.mjs`
- Create later: `sdk/version-matrix.json`

**Step 1: Write the failing checker**

The checker must require:

- `@stratum/sdk` at `0.0.0-beta.0`
- `@stratum/agents` at `0.0.0-beta.0`
- `@stratum/agents` depending exactly on `@stratum/sdk` at the same beta
- Python `stratum-sdk` at PEP 440 beta `0.0.0b0`
- package export boundaries from `dist`
- lifecycle build/prepack scripts that do not require Bun for package consumers
- `stratum_sdk.__version__` matching `pyproject.toml`
- `src/stratum_sdk/py.typed` present

**Step 2: Run it to verify it fails**

Run:

```bash
node sdk/scripts/check-publishing-boundaries.mjs
```

Expected: fails before the matrix and Python beta version are added.

### Task 2: Add Version Matrix And Python Package Boundary Test

**Files:**
- Create: `sdk/version-matrix.json`
- Create: `sdk/python/tests/test_package_boundary.py`
- Modify: `sdk/python/pyproject.toml`
- Modify: `sdk/python/src/stratum_sdk/__init__.py`
- Modify: `sdk/python/tests/test_package.py`

**Step 1: Add matrix**

Record the beta package set:

- npm `@stratum/sdk`: `0.0.0-beta.0`
- npm `@stratum/agents`: `0.0.0-beta.0`
- PyPI `stratum-sdk`: `0.0.0b0`

**Step 2: Update Python version**

Use PEP 440 beta spelling `0.0.0b0` in `pyproject.toml`, runtime
`__version__`, and package tests.

**Step 3: Add Python package-boundary test**

Verify metadata, package-data posture, `py.typed`, and runtime version parity
through stdlib `tomllib` and package file reads.

### Task 3: Update Publishing Docs

**Files:**
- Create: `docs/sdk-beta-publishing.md`
- Modify: `sdk/typescript/README.md`
- Modify: `sdk/agents/README.md`
- Modify: `sdk/python/README.md`
- Modify: `docs/project-status.md`

**Step 1: Document manual release order**

Document:

1. `npm publish --tag beta` for `@stratum/sdk`
2. `npm publish --tag beta` for `@stratum/agents`
3. Python build/upload for `stratum-sdk`

CI must not hold registry tokens or publish automatically in this slice.

**Step 2: Update package READMEs**

Add install snippets for matching beta versions and remove stale "not
published" wording from `@stratum/agents`.

### Task 4: Expand CI Package Boundary Coverage

**Files:**
- Modify: `.github/workflows/rust-ci.yml`
- Modify: `sdk/package.json`

**Step 1: Add root script**

Add `check:publishing` to run the Node checker.

**Step 2: Expand SDK CI**

In the SDK CI job:

- run the publishing checker
- typecheck/test/build `@stratum/sdk`
- typecheck/test/build `@stratum/agents`
- run `npm pack --dry-run` for both npm packages
- install Python SDK dev dependencies in a temp venv
- run Python tests
- build Python wheel/sdist into `$RUNNER_TEMP`

### Task 5: Verify And Commit

**Step 1: Run focused checks**

```bash
node sdk/scripts/check-publishing-boundaries.mjs
bun run --cwd sdk typecheck
bun run --cwd sdk test:run
bun run --cwd sdk build
cd sdk/typescript && npm pack --dry-run
cd sdk/agents && npm pack --dry-run
```

Python checks should run from a temp venv and build into `/tmp`.

**Step 2: Run final whitespace check**

```bash
git diff --check
```

**Step 3: Commit and push**

```bash
git add .github/workflows/rust-ci.yml docs/sdk-beta-publishing.md docs/plans/2026-06-11-sdk-beta-publishing.md docs/project-status.md sdk
git commit -m "chore: prepare sdk beta publishing"
git push origin HEAD:main
```
