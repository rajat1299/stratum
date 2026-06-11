# Agent Workspace Demo

This is the local-state private-beta product demo script. It shows the full
golden path in under 15 minutes after dependencies are installed and the release
build is warm: setup, scoped workspace token, agent edit, change request review,
file-view evidence, approval, merge, local audit evidence, and rollback.

The current demo uses:

- the `stratum` CLI for one-time admin and backing-agent setup
- the `stratum-server` HTTP API as the single writer
- `stratumctl workspace seed-demo` for local incident fixture setup
- `bun run --cwd sdk/agents example:incident` for the checked-in agent edit and
  change-request creation
- `scripts/run-local-golden-path-demo.sh` for the exact review, merge, audit,
  and revert commands

## Demo Goal

Show that agents need more than raw filesystem access. They need a persistent
workspace they can:

- inspect
- search
- update
- commit
- review
- audit
- revert

## Demo Setup

### 1. Initialize a fresh demo data directory

```bash
export STRATUM_DATA_DIR="$PWD/.demo/incident-workspace"
rm -rf "$STRATUM_DATA_DIR" .stratum-demo
mkdir -p "$STRATUM_DATA_DIR"
```

### 2. Create the admin user and agent token

Run the CLI once:

```bash
cargo run --release --bin stratum
```

Create an admin user when prompted:

```text
Admin username: alice
```

Then create the backing agent token:

```text
alice@stratum:~ $ su root
root@stratum:~ $ addagent incident-bot
Created agent: incident-bot (uid=2)
Token: REPLACE_WITH_REAL_TOKEN
root@stratum:~ $ exit
```

Save the token in another terminal:

```bash
export STRATUM_AGENT_TOKEN="REPLACE_WITH_REAL_TOKEN"
```

Exit the CLI before starting the HTTP server.

### 3. Start the HTTP server

```bash
STRATUM_DATA_DIR="$STRATUM_DATA_DIR" \
STRATUM_LISTEN=127.0.0.1:3000 \
cargo run --release --bin stratum-server
```

### 4. Run the golden path

In another terminal, with the server running and the backing agent token from
`addagent` exported:

```bash
export STRATUM_AGENT_TOKEN="<existing-agent-token>"
export STRATUM_REVIEWER_USER=alice
./scripts/run-local-golden-path-demo.sh
```

The script creates `.stratum-demo/incident-workspace.env` with `chmod 600`,
sources it with auto-export, runs the checked-in agent example, and writes
`.stratum-demo/incident-change-request.json`. No raw workspace token is printed.

If you want to run the core commands manually, use the same sequence:

```bash
cargo run --release --bin stratumctl -- \
  --url http://127.0.0.1:3000 \
  --user root \
  workspace seed-demo

set -a
source .stratum-demo/incident-workspace.env
set +a

bun run --cwd sdk/agents example:incident | tee .stratum-demo/incident-change-request.json

export STRATUM_CHANGE_REQUEST_ID="$(jq -r '.changeRequestId' .stratum-demo/incident-change-request.json)"
export STRATUM_BASELINE_COMMIT="$(jq -r '.baselineCommit' .stratum-demo/incident-change-request.json)"
export STRATUM_UPDATE_COMMIT="$(jq -r '.updateCommit' .stratum-demo/incident-change-request.json)"
```

The agent example reads the incident evidence, runbook, and memory through the
workspace token; writes `root-cause.md` and `remediation.md`; commits the update;
resets `main` to the baseline; creates a real source ref; and opens a change
request against `main`.

## 15-Minute Script

### Minute 0-1: Frame the problem

Say:

> Most agent systems still leave behind transcripts. We want a workspace:
> persistent memory, inspectable files, commits, rollback, and permissioned
> access.

Show health:

```bash
curl -s http://localhost:3000/health | jq
```

Expected: `status` is `ok`, and `inodes` is nonzero after seeding.

### Minute 1-3: Show workspace state through CLI tools

Load the workspace env into exported variables:

```bash
set -a
source .stratum-demo/incident-workspace.env
set +a
```

List the incident folder as the scoped workspace token:

```bash
stratumctl ls /incidents/checkout-latency/
```

Expected entries:

```text
evidence.md
hypotheses.md
timeline.md
```

Show the whole tree and search for incident signals:

```bash
stratumctl tree /
stratumctl grep "timeout|retry" /
```

Narration:

> The agent is using `stratumctl` against a workspace-scoped token. It can inspect
> the incident files without receiving a global admin credential.

### Minute 3-6: Let the agent create a reviewable change

Run the checked-in deterministic agent example:

```bash
bun run --cwd sdk/agents example:incident | tee .stratum-demo/incident-change-request.json

export STRATUM_CHANGE_REQUEST_ID="$(jq -r '.changeRequestId' .stratum-demo/incident-change-request.json)"
export STRATUM_BASELINE_COMMIT="$(jq -r '.baselineCommit' .stratum-demo/incident-change-request.json)"
export STRATUM_UPDATE_COMMIT="$(jq -r '.updateCommit' .stratum-demo/incident-change-request.json)"
```

Point out the safe output fields:

- `filesRead`: the evidence the agent inspected
- `filesWritten`: generated incident artifacts
- `sourceRef`: the real source ref backing the change request
- `changeRequestId`: the review object used by the operator
- `diffPreview`: the bounded diff preview

### Minute 6-9: Review and approve

Mark the generated files viewed:

```bash
curl -s -X PUT "$STRATUM_URL/change-requests/$STRATUM_CHANGE_REQUEST_ID/viewed-files" \
  -H "Authorization: User root" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: incident-demo-view-root-cause" \
  -d '{"path":"/incidents/checkout-latency/root-cause.md","viewed":true}' | jq

curl -s -X PUT "$STRATUM_URL/change-requests/$STRATUM_CHANGE_REQUEST_ID/viewed-files" \
  -H "Authorization: User root" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: incident-demo-view-remediation" \
  -d '{"path":"/incidents/checkout-latency/remediation.md","viewed":true}' | jq
```

Approve as the human reviewer. This must be a different admin from the CR
creator; the setup creates `alice` for this purpose.

```bash
curl -s -X POST "$STRATUM_URL/change-requests/$STRATUM_CHANGE_REQUEST_ID/approvals" \
  -H "Authorization: User alice" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: incident-demo-approve-1" \
  -d '{"comment":"Reviewed for demo."}' | jq
```

### Minute 9-11: Merge and show audit evidence

Merge as the operator:

```bash
curl -s -X POST "$STRATUM_URL/change-requests/$STRATUM_CHANGE_REQUEST_ID/merge" \
  -H "Authorization: User root" \
  -H "Idempotency-Key: incident-demo-merge-1" | jq
```

Show local audit evidence:

```bash
curl -s "$STRATUM_URL/audit?limit=25" \
  -H "Authorization: User root" | \
  jq '.events[] | {action, resource, route, details}'
```

Call out the file-view, approval, merge-policy, and merge audit events. Local
audit listing is the private-beta evidence surface; hosted durable audit listing
remains out of scope for this demo.

### Minute 11-13: Revert and show rollback evidence

Revert `main` back to the baseline commit captured before the agent update:

```bash
curl -s -X POST "$STRATUM_URL/vcs/revert" \
  -H "Authorization: User root" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: incident-demo-revert-1" \
  -d "{\"hash\":\"$STRATUM_BASELINE_COMMIT\"}" | jq
```

Show audit evidence again:

```bash
curl -s "$STRATUM_URL/audit?limit=25" \
  -H "Authorization: User root" | \
  jq '.events[] | {action, resource, route, details}'
```

Confirm the restored state through the workspace:

```bash
stratumctl ls /incidents/checkout-latency/
```

### Minute 13-15: Close with the product statement

Say:

> This is the shift from files to workspaces. The agent did not just write
> output. It searched durable memory, produced inspectable artifacts, opened a
> reviewable change, left audit evidence, and rolled back cleanly.

## Prompts To Use In Cursor

These are good live prompts while the shell commands are visible:

- `Inspect the incident workspace before making changes. Use CLI tools first.`
- `Search for timeout and retry evidence, then summarize the likely root cause.`
- `Write root-cause and remediation files in the incident folder.`
- `Open the work as a change request instead of pushing directly to main.`
- `Show the audit trail, then roll the merged change back to the baseline.`

## Demo Notes

- Use the HTTP server as the single writer during the live demo.
- Do not run the CLI, MCP server, and HTTP server as concurrent writers against
  the same `state.bin`.
- Keep hosted durable deployment out of this local demo. Use
  `docs/durable-deployment-runbook.md` and `docs/private-beta-contract.md` for
  hosted posture and unsupported-route evidence.
- If `.stratum-demo/incident-workspace.env` already exists, remove
  `.stratum-demo` and rerun from a fresh `STRATUM_DATA_DIR`; seed-demo refuses to
  overwrite token files.
