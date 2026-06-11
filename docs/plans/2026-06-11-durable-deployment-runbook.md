# Durable Deployment Runbook Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close Task 13 by documenting one private-beta Cloudflare/Postgres/R2 durable deployment posture with env vars, migrations, live gates, rollback, and unsupported route evidence.

**Architecture:** Add a docs-only runbook backed by a small shell contract check. The checker asserts that the operator-critical facts stay present without reading secret-bearing env files or requiring live infrastructure. Existing runtime gates, live gate scripts, and capability fixtures remain the source of truth.

**Tech Stack:** Markdown docs, Bash verification, existing Rust/Postgres/R2 live gate scripts.

---

### Task 1: Add The Failing Runbook Contract Check

**Files:**
- Create: `scripts/check-durable-deployment-runbook.sh`

**Step 1: Write the failing check**

Add a Bash script that requires:

- `docs/durable-deployment-runbook.md`
- `docs/project-status.md`
- `docs/private-beta-contract.md`
- durable-cloud runtime env names
- Postgres migration modes
- R2 env names
- Cloudflare local operator env names from `.env.live-gates`
- live gate script names
- unsupported route names from `sdk/contracts/capabilities.v1.durable-cloud.json`
- the stable durable-cloud unsupported JSON body
- rollback and signoff checklist headings

The script must not source `.env.live-gates` or print env values.

**Step 2: Run it to verify it fails**

Run:

```bash
./scripts/check-durable-deployment-runbook.sh
```

Expected: fails because the runbook, project-status entry, and private-beta link are missing.

### Task 2: Add The Durable Deployment Runbook

**Files:**
- Create: `docs/durable-deployment-runbook.md`
- Read: `docs/http-api-guide.md`
- Read: `docs/private-beta-contract.md`
- Read: `sdk/contracts/capabilities.v1.durable-cloud.json`
- Read: `scripts/ci-live-durable-cloud-gate.sh`
- Read: `scripts/check-postgres-migrations.sh`
- Read: `scripts/check-r2-object-store.sh`

**Step 1: Write the runbook**

Document:

- the single private-beta posture: Cloudflare account/deploy boundary, Postgres control plane, R2 object store, durable-cloud runtime
- secret handling rules and local `.env.live-gates` limits
- runtime, readiness, storage, idempotency, live gate, and Cloudflare operator env vars
- migration status/apply/adopt behavior
- startup and live verification commands
- health and `GET /v1/capabilities` checks
- unsupported route list and stable `501` response
- rollback checklist for app config, migrations, R2, routes, audit/export, and local-state fallback boundaries
- signoff evidence checklist

**Step 2: Keep unsupported scope explicit**

State that hosted durable audit listing, runs, execution, VCS recovery operator routes, provider mounts, semantic search when the index is unavailable, durable MCP/FUSE/direct REPL, hosted auth login, SCIM, and hosted admin UI remain out of private-beta deployment scope.

### Task 3: Wire Status And Contract Docs

**Files:**
- Modify: `docs/project-status.md`
- Modify: `docs/private-beta-contract.md`

**Step 1: Add project status**

Add `Task 13 / Durable Deployment Runbook` near the top with factual completed scope and verification commands.

**Step 2: Link from private beta contract**

Add a concise operator note pointing to `docs/durable-deployment-runbook.md` as the durable deployment posture and rollback checklist.

### Task 4: Verify The Slice

**Files:**
- Run: `scripts/check-durable-deployment-runbook.sh`
- Run: `scripts/ci-live-durable-cloud-gate.sh`
- Run: `scripts/check-postgres-migrations.sh`
- Run: `scripts/check-r2-object-store.sh`

**Step 1: Run syntax and contract checks**

```bash
bash -n scripts/check-durable-deployment-runbook.sh
./scripts/check-durable-deployment-runbook.sh
```

Expected: both pass.

**Step 2: Run no-secret gate skip checks**

```bash
STRATUM_LIVE_GATE_REQUIRED= ./scripts/ci-live-durable-cloud-gate.sh
STRATUM_POSTGRES_TEST_URL= ./scripts/check-postgres-migrations.sh
STRATUM_R2_TEST_ENABLED= ./scripts/check-r2-object-store.sh
```

Expected: each exits zero and skips because required live provider env is unset.

**Step 3: Check formatting**

```bash
git diff --check
```

Expected: no whitespace errors.

### Task 5: Commit And Push

**Files:**
- Stage the runbook, checker, plan, status, and private-beta contract.

**Step 1: Review diff**

```bash
git diff -- docs/durable-deployment-runbook.md docs/project-status.md docs/private-beta-contract.md scripts/check-durable-deployment-runbook.sh
```

**Step 2: Commit**

```bash
git add docs/durable-deployment-runbook.md docs/plans/2026-06-11-durable-deployment-runbook.md docs/project-status.md docs/private-beta-contract.md scripts/check-durable-deployment-runbook.sh
git commit -m "docs: add durable deployment runbook"
```

**Step 3: Push**

```bash
git push origin HEAD:main
```
