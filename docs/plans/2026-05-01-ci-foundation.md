# CI Foundation Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add a GitHub Actions CI foundation that verifies formatting, clippy, normal tests, optional FUSE compilation, and dependency security audit for Stratum.

**Architecture:** Make the repository pass the CI commands first, then add explicit workflow jobs that run the same commands. Keep performance tests out of the default PR gate because they are intentionally long-running; expose them through manual and scheduled workflow triggers instead.

**Tech Stack:** GitHub Actions, Rust stable toolchain, Cargo, rustfmt, clippy, cargo-audit.

---

## Task 1: Make Format And Clippy CI-Clean

**Files:**
- Modify Rust source and tests touched by `cargo fmt --all`.
- Modify only Rust files needed to satisfy `cargo clippy --locked --all-targets -- -D warnings`.

**Requirements:**
- Run `cargo fmt --all` once and keep the mechanical formatting changes.
- Fix current clippy warnings without changing behavior:
  - `new_without_default`
  - `collapsible_if`
  - `filter_next`
  - `manual_strip`
  - `derivable_impls`
  - `needless_borrow`
  - `useless_format`
  - any equivalent clippy findings surfaced by the local toolchain
- Prefer small idiomatic rewrites over `#[allow(...)]`.
- Do not broaden feature work or refactor unrelated behavior.

**Verification:**

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --lib --bins
```

**Commit:**

```bash
git add .
git commit -m "chore: make rust checks ci-clean"
```

---

## Task 2: Add GitHub Actions Workflows

**Files:**
- Add: `.github/workflows/rust-ci.yml`
- Add: `.github/workflows/rust-perf.yml`
- Modify: `docs/project-status.md`

**Requirements:**
- Add default CI on pull requests and pushes to `main` and `v2/**`.
- Use least-privilege workflow permissions: `contents: read`.
- Use separate jobs for:
  - `fmt`: `cargo fmt --all -- --check`
  - `clippy`: `cargo clippy --locked --all-targets -- -D warnings`
  - `test`: normal non-perf tests with `--lib --bins`, integration, permissions, and doc tests
  - `fuser`: compile optional `stratum-mount` with `cargo check --locked --features fuser --bin stratum-mount`
  - `audit`: install and run `cargo audit --deny warnings`
- Keep perf tests out of default PR CI. Add a separate workflow for `workflow_dispatch` and a scheduled run with:
  - `cargo test --locked --release --test perf -- --nocapture`
  - `cargo test --locked --release --test perf_comparison -- --nocapture`
- Update `docs/project-status.md` to mark the CI foundation as the current/completed slice and note that perf is scheduled/manual, not a default PR gate.

**Verification:**

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --lib --bins
cargo test --locked --test integration --test permissions
cargo test --locked --doc
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check
```

**Commit:**

```bash
git add .github/workflows/rust-ci.yml .github/workflows/rust-perf.yml docs/project-status.md
git commit -m "ci: add rust verification workflows"
```

---

## Task 3: Review And Full Verification

**Files:**
- Modify only files required by review findings.

**Requirements:**
- Dispatch a fresh spec reviewer against this plan.
- Dispatch a fresh quality/security reviewer focused on workflow safety, trigger scope, supply-chain/security audit behavior, platform compatibility, and whether the workflow commands match local verification.
- Fix reviewer findings and commit separately.

**Verification:**

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo check --locked --features fuser --bin stratum-mount
cargo audit --deny warnings
git diff --check HEAD~3..HEAD
```

**Commit:**

```bash
git add <changed-files>
git commit -m "fix: address ci review findings"
```
