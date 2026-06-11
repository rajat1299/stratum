# Agent Adapter Examples

## Incident Change Request

This example runs after `stratumctl workspace seed-demo`. It reads the seeded incident files through the Stratum workspace token, applies an OpenAI Agents editor adapter patch, commits the update, resets `main` to the baseline, creates a source ref, and opens a Stratum change request.

Start `stratum-server`, run the seed demo, then:

```bash
set -a
source .stratum-demo/incident-workspace.env
set +a
bun run --cwd sdk/agents example:incident
```

The command prints workspace id, file paths, refs, change-request id, and a short diff preview. It never prints the workspace token.

Default mode is deterministic and does not call any model provider.
