# Agent Workspace Demo

This guide gives a runnable 7-minute demo for positioning `stratum` as an agent workspace.

The current demo uses:

- the `stratum` CLI for one-time setup
- the `stratum-server` HTTP API as the single writer
- `stratumctl workspace seed-demo` plus `stratumctl` after `source` for agent-facing commands

## Demo Goal

Show that agents need more than raw filesystem access. They need a persistent workspace they can:

- inspect
- search
- update
- commit
- review
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

### 4. Seed the workspace with `stratumctl`

In another terminal, with the server running and the backing agent token from
`addagent` exported:

```bash
export STRATUM_AGENT_TOKEN="<existing-agent-token>"

cargo run --release --bin stratumctl -- \
  --url http://127.0.0.1:3000 \
  --user root \
  workspace seed-demo
```

This local-state-only command creates the `incident-demo` workspace at
`/demo/incident-workspace`, creates the workspace root, seeds the files from
`examples/incident-workspace`, issues a scoped workspace token, and writes it to
`.stratum-demo/incident-workspace.env` with `chmod 600`. The command prints safe
next steps only; no raw tokens are printed.

Load the env file and inspect the workspace:

```bash
source .stratum-demo/incident-workspace.env
stratumctl tree /
stratumctl grep timeout /
```

## 7-Minute Script

### Minute 0-1: Frame the problem

Say:

> Most agent systems still leave behind transcripts. We want a workspace: persistent memory, inspectable files, commits, rollback, and permissioned access.

Show health:

```bash
curl -s http://localhost:3000/health | jq
```

Expected: `status` is `ok`, `commits` is `0` before the first demo commit,
and `inodes` is nonzero after seeding.

### Minute 1-2: Show workspace state through CLI tools

List the incident folder as the agent:

```bash
source .stratum-demo/incident-workspace.env
stratumctl ls /incidents/checkout-latency/
```

Expected entries:

```text
evidence.md
hypotheses.md
timeline.md
```

Show the whole tree:

```bash
source .stratum-demo/incident-workspace.env
stratumctl tree /
```

### Minute 2-3: Let the agent inspect evidence before writing

Read the runbook:

```bash
source .stratum-demo/incident-workspace.env
stratumctl cat /runbooks/payment-service.md
```

Search for prior timeout and retry signals:

```bash
source .stratum-demo/incident-workspace.env
stratumctl grep "timeout|retry" /
```

Expected stdout:

```text
incidents/checkout-latency/evidence.md:6: - `payment_service_timeout_rate`: 7.4%, baseline < 0.5%
incidents/checkout-latency/evidence.md:7: - `checkout_retry_rate`: 3.1x baseline
incidents/checkout-latency/evidence.md:13: ERROR payment confirmation request exceeded timeout budget
incidents/checkout-latency/hypotheses.md:5: ### 1. Payment-service timeout regression
incidents/checkout-latency/hypotheses.md:7: Latest rollout likely changed timeout handling or retry behavior, causing checkout to block on confirmation.
...
runbooks/payment-service.md:9: If checkout latency spikes immediately after a payment-service deploy, inspect timeout and retry changes first.
```

`stratumctl grep` writes the match count to stderr.

Narration:

> The agent is using `stratumctl` against the workspace after sourcing the secure env file.

### Minute 3-4: Create new agent output

Write a root-cause summary:

```bash
source .stratum-demo/incident-workspace.env
cat <<'EOF' | stratumctl write /incidents/checkout-latency/root-cause.md --stdin
# Root Cause

The most likely root cause is a payment-service timeout and retry regression introduced by the latest deploy.

## Why

- Evidence shows elevated confirmation timeouts.
- Prior memory connects this pattern to payment-service rollout changes.
- Checkout appears to be blocked on payment confirmation rather than failing independently.
EOF
```

Commit the investigation state:

```bash
curl -s -X POST http://localhost:3000/vcs/commit \
  -H "Authorization: User root" \
  -H "Content-Type: application/json" \
  -d '{"message":"initial investigation"}' | jq
```

Then show history:

```bash
curl -s http://localhost:3000/vcs/log \
  -H "Authorization: User root" | jq
```

Open a change request from an existing source/session ref to `main`:

```bash
cargo run --release --bin stratumctl -- \
  --url http://127.0.0.1:3000 \
  --user root \
  change-request create \
  --session-ref agent/incident-demo/session \
  --target-ref main \
  --title "Incident update" \
  --description "Agent changes from the mounted incident workspace." \
  --idempotency-key incident-demo-cr-1
```

For the local demo, use `--user root` for admin change-request creation. In a hosted durable preview, use the repo-bound workspace bearer form with `--workspace-id`, `--workspace-token`, and `--repo` instead. If the source ref does not exist, the server returns a bounded error and `stratumctl` exits non-zero.

### Minute 4-5: Show rollback

Make a bad edit:

```bash
source .stratum-demo/incident-workspace.env
cat <<'EOF' | stratumctl write /incidents/checkout-latency/root-cause.md --stdin
# Root Cause

Everything looks healthy. No action required.
EOF
```

Commit the bad state:

```bash
curl -s -X POST http://localhost:3000/vcs/commit \
  -H "Authorization: User root" \
  -H "Content-Type: application/json" \
  -d '{"message":"bad incident conclusion"}' | jq
```

Identify the previous good hash:

```bash
curl -s http://localhost:3000/vcs/log \
  -H "Authorization: User root" | jq '.commits[:2]'
```

Revert to the earlier commit:

```bash
curl -s -X POST http://localhost:3000/vcs/revert \
  -H "Authorization: User root" \
  -H "Content-Type: application/json" \
  -d '{"hash":"REPLACE_WITH_PREVIOUS_HASH"}' | jq
```

Confirm the restored file:

```bash
source .stratum-demo/incident-workspace.env
stratumctl cat /incidents/checkout-latency/root-cause.md
```

### Minute 5-6: Show permissioned agent access

Point out that the workspace is not just shared storage. It has identity and access.

Use the scoped workspace token from `.stratum-demo/incident-workspace.env` for all agent reads and writes in the demo. Then show what a named human user sees:

```bash
curl -s http://localhost:3000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"alice"}' | jq
```

If you want a stronger permission story, tighten permissions on one directory during setup and show a `403` response for the token user.

### Minute 6-7: Close with the product statement

Say:

> This is the shift from files to workspaces. The agent did not just write output. It searched durable memory, produced inspectable artifacts, committed state, and rolled back a bad conclusion.

## Prompts To Use In Cursor

These are good live prompts while the shell commands are visible:

- `Inspect the incident workspace before making changes. Use CLI tools first.`
- `Search for timeout and retry evidence, then summarize the likely root cause.`
- `Write a root-cause markdown file in the incident folder.`
- `Commit the current investigation state with a clear message.`
- `Now simulate a bad conclusion and show how to recover by reverting it.`

## Demo Notes

- Use the HTTP server as the single writer during the live demo.
- Do not run the CLI, MCP server, and HTTP server as concurrent writers against the same `state.bin`.
- Keep the live commands grounded in the existing `stratumctl ls`, `cat`, `tree`, `grep`, and `write` surface.
