# Product Docs Demo Script Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the private-beta getting-started path a copyable under-15-minute local demo that reaches agent edit, change request review, merge, audit evidence, and rollback.

**Architecture:** Keep the golden path local-state-only and reuse the checked-in `@stratum/agents` incident example for the stateful agent edit and change-request creation. Add a small checker to keep docs from regressing back to a fake session ref, and add an operator script for the exact commands after the server and backing agent token exist.

**Tech Stack:** Bash, Rust CLI/server binaries, `stratumctl`, `bun`, `jq`, `curl`, local Stratum HTTP API.

---

### Task 1: Golden Path Doc Checker

**Files:**
- Create: `scripts/check-getting-started-golden-path-doc.sh`
- Modify: none

**Step 1: Write the failing checker**

Create a Bash checker that asserts:

- `docs/getting-started.md` contains a `15-Minute Private Beta Golden Path` section.
- `docs/getting-started.md` references `scripts/run-local-golden-path-demo.sh`.
- `docs/getting-started.md` includes the exact setup commands for `STRATUM_DATA_DIR`, `STRATUM_LISTEN`, `workspace seed-demo`, `.stratum-demo/incident-workspace.env`, `bun run --cwd sdk/agents example:incident`, `viewed-files`, `/approvals`, `/merge`, `/audit?limit=25`, and `/vcs/revert`.
- `docs/agent-workspace-demo.md` uses `bun run --cwd sdk/agents example:incident`.
- `docs/agent-workspace-demo.md` no longer uses the fragile `--session-ref agent/incident-demo/session` change-request command.
- `scripts/run-local-golden-path-demo.sh` exists and is executable.

**Step 2: Run it to verify it fails**

Run:

```bash
bash scripts/check-getting-started-golden-path-doc.sh
```

Expected: FAIL because the new section and runner do not exist yet.

### Task 2: Local Golden Path Runner

**Files:**
- Create: `scripts/run-local-golden-path-demo.sh`

**Step 1: Implement the minimal runner**

Create a Bash script that:

- Requires `STRATUM_AGENT_TOKEN`.
- Defaults `STRATUM_URL=http://127.0.0.1:3000`, `STRATUM_ADMIN_USER=root`, and `STRATUM_REVIEWER_USER=alice`.
- Runs `cargo run --release --bin stratumctl -- ... workspace seed-demo`.
- Sources `.stratum-demo/incident-workspace.env` with auto-export so child
  processes receive the workspace variables.
- Runs `bun run --cwd sdk/agents example:incident` and stores JSON at `.stratum-demo/incident-change-request.json`.
- Extracts `STRATUM_CHANGE_REQUEST_ID`, `STRATUM_BASELINE_COMMIT`, and `STRATUM_UPDATE_COMMIT` with `jq`.
- Marks both generated files viewed.
- Approves the change request as `alice`.
- Merges as `root`.
- Lists bounded local audit evidence.
- Reverts to `STRATUM_BASELINE_COMMIT`.
- Lists audit evidence again.

**Step 2: Check Bash syntax**

Run:

```bash
bash -n scripts/run-local-golden-path-demo.sh
```

Expected: PASS.

### Task 3: Getting Started Golden Path

**Files:**
- Modify: `docs/getting-started.md`

**Step 1: Add prerequisites**

Mention that the private-beta golden path also needs `bun`, `jq`, and a local terminal pair.

**Step 2: Add the 15-minute section**

Add a copyable section that:

- Initializes `STRATUM_DATA_DIR`.
- Creates admin user `alice` and agent `incident-bot`.
- Starts `stratum-server`.
- Runs `scripts/run-local-golden-path-demo.sh`.
- Names the evidence file `.stratum-demo/incident-change-request.json`.
- Calls out that the first cold Rust build may take longer, while the demo path after dependencies/build cache is under 15 minutes.

**Step 3: Replace brittle remote CLI example**

Replace the old `change-request create --session-ref agent/incident-demo/session` example with the checked-in agent example and a note that direct CR creation requires a real source ref.

### Task 4: Agent Workspace Demo

**Files:**
- Modify: `docs/agent-workspace-demo.md`

**Step 1: Replace manual CR creation**

Use `bun run --cwd sdk/agents example:incident` and parse its JSON output instead of manually writing and creating a CR from the seed session ref.

**Step 2: Add review/merge/audit/revert commands**

Document the exact curl commands for file-viewed state, approval, merge, audit, and revert.

**Step 3: Keep hosted durable boundaries explicit**

Keep durable hosted preview out of the local demo and point to the private-beta contract/runbook for that posture.

### Task 5: Status And Verification

**Files:**
- Modify: `docs/project-status.md`

**Step 1: Add Task 15 status**

Summarize the new runner, docs, and static checker.

**Step 2: Run verification**

Run:

```bash
bash -n scripts/check-getting-started-golden-path-doc.sh scripts/run-local-golden-path-demo.sh
./scripts/check-getting-started-golden-path-doc.sh
bun run --cwd sdk/agents test:run -- incident-example.test.ts
git diff --check
```

Expected: all pass. Do not run the full live demo in this slice because it requires an interactive backing agent token; Task 16 owns the live release rehearsal.
