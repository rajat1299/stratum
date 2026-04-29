# CLI Cloud Bridge

This guide describes how to connect `stratum` to cloud execution environments while keeping the user experience CLI-first.

## Recommendation

Build a hosted workspace gateway and a small `stratumctl` CLI wrapper before attempting a real OS mount.

This is the best first bridge because:

- agents already know how to use command-line tools
- the current product already maps cleanly to CLI verbs
- a gateway centralizes concurrency and persistence
- the same gateway can later support MCP, HTTP, hosted execution, and mounts

## User Experience

The end-user mental model should be:

```bash
stratumctl ls /incidents
stratumctl cat /incidents/checkout-latency/evidence.md
stratumctl grep timeout /runbooks
stratumctl search "what changed after the payment deploy?"
stratumctl commit "initial investigation"
stratumctl log
stratumctl revert abcd1234
```

Later:

```bash
stratumctl run "python analyze.py"
```

## Why Not Start With FUSE

FUSE or another mount layer can come later, but it is a poor first bridge for cloud agents.

Problems with leading with a mount:

- more platform-specific implementation work
- harder auth and lifecycle management
- harder to reason about concurrency
- lower demo velocity than a focused CLI wrapper

The first bridge should optimize for product clarity and speed of adoption, not perfect filesystem transparency.

## Architecture

```mermaid
flowchart LR
  localUser[Local User Or Agent] --> cli[stratumctl CLI]
  remoteIde[Remote IDE Or Codespace] --> cli
  ci[CI Or Automation] --> cli
  cli --> gateway[Workspace Gateway]
  gateway --> auth[Auth And Policy]
  auth --> core[stratum Core]
  core --> persistence[Workspace State]
  core --> search[Search And VCS]
  futureExec[Future Sandboxed Runner] --> core
```

## Gateway Responsibilities

The hosted gateway should:

- authenticate users and agents
- map CLI commands to workspace operations
- serialize or coordinate writes safely
- expose HTTP APIs for automation
- expose future execution APIs
- provide a stable endpoint for remote clients

This is where cloud-specific integration belongs, not inside every client.

## Cloud Targets

The bridge should support four environments with the same CLI:

### 1. Local laptop

Best for development and human review.

### 2. Remote IDE or cloud development environment

Examples:

- GitHub Codespaces
- remote Cursor or VS Code workspace
- cloud VM accessed over SSH

### 3. CI job

Use the CLI to inspect, update, and commit workspace state in automation pipelines.

### 4. Hosted sandbox

Future execution layer that runs commands against a workspace from the same gateway.

## Suggested Command Mapping

### Existing capabilities

- `stratumctl ls` -> `GET /fs/{path}`
- `stratumctl cat` -> `GET /fs/{path}`
- `stratumctl write` -> `PUT /fs/{path}`
- `stratumctl grep` -> `GET /search/grep`
- `stratumctl find` -> `GET /search/find`
- `stratumctl tree` -> `GET /tree/{path}`
- `stratumctl commit` -> `POST /vcs/commit`
- `stratumctl log` -> `GET /vcs/log`
- `stratumctl revert` -> `POST /vcs/revert`
- `stratumctl status` -> `GET /vcs/status`

### Future capabilities

- `stratumctl search` -> semantic index endpoint
- `stratumctl diff` -> future diff/status endpoint
- `stratumctl run` -> future runs API

## Authentication Model

Use short-lived tokens or agent tokens issued by the gateway.

The CLI should avoid direct state-file access in cloud mode. It should act as a thin client over the gateway.

Benefits:

- safer remote usage
- central auditability
- cleaner policy enforcement
- consistent behavior across environments

## First Release Scope

The first `stratumctl` release should do only three things well:

1. connect to a remote workspace endpoint
2. expose the current file/search/version verbs
3. use bearer-token authentication cleanly

The current implementation also supports hosted workspace records and workspace-scoped bearer tokens through the gateway.

That is enough to power:

- live demos
- agent shells
- CI automation
- remote terminals

## Migration Path

### Now

Use shell commands like `curl` and `jq` against `stratum-server`.

### Next

Wrap the current HTTP API in `stratumctl`.

### Later

Add semantic search, diff, and run commands.

### Much later

Add optional mounts where platform integration truly matters.

## Product Statement

The cloud bridge should be sold as:

> Bring your agent terminal anywhere. The same workspace works from local shells, remote IDEs, CI, and future hosted runners.

That message is clearer and more defensible than “we mounted a cloud filesystem.”
