# stratum-sdk (Python)

Synchronous Python client (`httpx`) for the **current** Stratum HTTP API. Mirrors the TypeScript `@stratum/sdk` resource layout: `filesystem`, `search`, `vcs`, `reviews`, `runs`, and `workspaces`.

**Semantic search** is deliberately unsupported: calling `StratumClient.search.semantic(...)` raises `UnsupportedFeatureError` until the backend exposes a derived index.

Requires **Python 3.11+**. This slice ships **sync only**—no async client yet.

## Install from repo

From the repo root (path may vary):

```bash
python3 -m venv sdk/python/.venv
. sdk/python/.venv/bin/activate
pip install -e ./sdk/python
```

Development install with checks:

```bash
cd sdk/python
python -m pip install -e ".[dev]"
pytest
python -m mypy src/stratum_sdk
python -m ruff check src tests && python -m ruff format --check src tests
python -m build
```

## User auth

Named users authenticate with `UserAuth` (`Authorization: User <username>`):

```python
from stratum_sdk import StratumClient, UserAuth

client = StratumClient("http://127.0.0.1:3000/", UserAuth("alice"))
print(client.read_file("/notes/hello.md"))
client.close()

# Context manager closes the SDK-owned httpx.Client
with StratumClient("http://127.0.0.1:3000/", UserAuth("alice")) as c:
    c.write_file("/tmp/x.txt", "hello")
```

## Workspace bearer auth

Hosted workspace mounts use Bearer token plus `X-Stratum-Workspace` (either `WorkspaceAuth`, or shorthand `workspace_id` + `workspace_token`):

```python
from stratum_sdk import StratumClient

client = StratumClient(
    "http://127.0.0.1:3000/",
    workspace_id="YOUR_WORKSPACE_UUID",
    workspace_token="YOUR_WORKSPACE_TOKEN",
)
listing = client.list_directory("")
```

Equivalent explicit form:

```python
from stratum_sdk import StratumClient, WorkspaceAuth

auth = WorkspaceAuth("YOUR_WORKSPACE_UUID", "YOUR_WORKSPACE_TOKEN")
StratumClient("http://127.0.0.1:3000/", auth).fs.list_directory("")
```

## Filesystem (read/write/list/stat)

```python
text = client.read_file("/doc.md")
blob = client.read_file_bytes("/bin/data")
client.write_file("/doc.md", "# Title\n", mime_type="text/markdown")
client.mkdir("/out")
listing = client.list_directory("/")  # path optional, default ""

info = client.stat("/doc.md")  # root "/" is unsupported; list_directory instead

client.fs.patch_metadata("/doc.md", {"mime_type": "text/markdown"})
client.copy_path("/a.txt", "/b.txt")
client.move_path("/b.txt", "/c.txt")
client.delete_path("/c.txt", recursive=False)
```

Mutating filesystem calls attach `Idempotency-Key` automatically (caller can pass `idempotency_key=` to override).

## Search (grep, find, tree)

```python
matches = client.grep("TODO", path="/specs", recursive=True)
found = client.find("*.md", path="/notes")
outline = client.tree("/docs")

# Raises UnsupportedFeatureError intentionally:
# client.search.semantic("nearest architecture decision")
```

## VCS (status / diff / commit)

```python
dirty = client.status()
patch = client.diff()  # or client.diff(path="/src/lib.rs")

from stratum_sdk import UserAuth

c = StratumClient("http://127.0.0.1:3000/", UserAuth("root"))
committed = c.commit("checkpoint before experiment")
history = c.vcs.log()
c.vcs.revert(committed["hash"])

refs = c.vcs.list_refs()
```

## Reviews & change requests

Administrative callers can manage protected refs/paths and change workflows (match your server’s ACLs):

```python
c = StratumClient("http://127.0.0.1:3000/", UserAuth("root"))

rules = c.reviews.list_protected_refs()

cr = c.reviews.create_change_request(
    {
        "title": "Doc sync",
        "source_ref": "agent/bot/session-1",
        "target_ref": "main",
    }
)
cid = cr["change_request"]["id"]

c.reviews.assign_reviewer(cid, {"reviewer_uid": 42, "required": True})
c.reviews.create_comment(cid, {"body": "LGTM"})
c.reviews.approve(cid)
merged = c.reviews.merge(cid)
```

## Runs (workspace-scoped execution records)

Runs require workspace bearer auth against a mounted workspace (`POST /runs` per API guide):

```python
from stratum_sdk import StratumClient, WorkspaceAuth

runner = StratumClient(
    "http://127.0.0.1:3000/",
    WorkspaceAuth(workspace_id, workspace_token),
)
created = runner.runs.create(
    {
        "prompt": "Analyze latency report",
        "command": "./scripts/report.sh",
    }
)
run_id = created["run_id"]
record = runner.runs.get(run_id)
stdout = runner.runs.stdout(run_id)
```

## Workspaces & token issuance

Administrative callers create workspaces and mint scoped tokens (responses include a plaintext `workspace_token`; **no Idempotency-Key on token issuance**, matching the HTTP API):

```python
from stratum_sdk import StratumClient, UserAuth

ws = StratumClient("http://127.0.0.1:3000/", UserAuth("root"))
created = ws.workspaces.create(
    {
        "name": "incident-demo",
        "root_path": "/incidents/checkout-latency",
        "session_ref": "agent/legal-bot/session-123",
    }
)
issued = ws.workspaces.issue_token(
    created["id"],
    {"name": "ci-token", "agent_token": "existing-agent-token-here"},
)
workspace_token = issued["workspace_token"]
# Store or pass workspace_token through your secret manager/environment.
# Avoid printing it or writing it to logs.

all_ws = ws.workspaces.list()
one = ws.workspaces.get(created["id"])
```

## Types

Typed request/response shapes live in `stratum_sdk.types` (`TypedDict` definitions). Responses are deserialized JSON without runtime schema validation—the same pragmatic approach as `@stratum/sdk`.

## Unsupported in this slice

- Async APIs (`AsyncStratumClient`).
- Semantic search (`search.semantic`).
- Integration tests that spawn the Rust binary (use httpx mocking or a live staging server externally).
