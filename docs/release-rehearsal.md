# Release Rehearsal

Status: Task 16 / release rehearsal HITL, 2026-06-11.

## Local Golden Path

The local-state private-beta rehearsal passed end to end first in the active
worktree with a throwaway `/tmp/stratum-release-rehearsal.*` data directory,
then again from a fresh clone of `origin/main` at
`af5e7144968d1ef2ad0b7c18a0cb2f9a202bfc23` with a fresh temp data dir and temp
Cargo target dir. Raw setup logs and token material stayed in `/tmp` and were
removed by the cleanup trap.

Covered flow:

- interactive CLI setup for admin `alice` and backing agent `incident-bot`
- `stratum-server` on a temporary localhost port
- `stratumctl workspace seed-demo`
- checked-in `bun run --cwd sdk/agents example:incident`
- file-view evidence using example-returned `reviewPaths`
- reviewer approval as `alice`
- merge as `root`
- `/audit?limit=25` evidence after merge
- `/vcs/revert` to the captured baseline commit
- `/audit?limit=25` evidence after revert

The rehearsal exposed and fixed four local blockers:

- seed-demo had to make the demo workspace root writable by scoped workspace
  tokens after root creates it
- the agent example had to request diff preview by concrete commit IDs, not ref
  names
- file-view evidence had to use review changed paths, not workspace-projected
  agent paths
- the Bash demo script had to tolerate an empty optional repo header under
  macOS Bash with `set -u`
- a fresh checkout had no SDK dependencies installed, so the script now runs
  `bun install` under `sdk/` when `sdk/node_modules/.bin/tsc` is missing

## Hosted Durable

Hosted durable rehearsal was not executed in this repository session. It needs
an operator-provisioned Cloudflare deploy target, Postgres/R2 credentials,
tenant/repo context, and a repo-bound hosted admin bearer. Until those are
available, durable-cloud routes must stay fail-closed and must not fall back to
local `.vfs` state for evidence.
