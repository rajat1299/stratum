"""
Run from the repo after installing the package in editable mode::

    cd sdk/python
    pip install -e ".[dev]"
    STRATUM_SDK_LIVE=1 STRATUM_SDK_LIVE_BASE_URL=... STRATUM_SDK_LIVE_ADMIN_USER=root \\
      STRATUM_SDK_LIVE_AGENT_TOKEN=... python examples/live_workspace.py

Uses the same environment variables as the opt-in pytest live smoke test.
Prints workspace id and read verification only (never the workspace token).
"""

from __future__ import annotations

import json
import os
import random
import string
import sys
import time

from stratum_sdk import StratumClient, UserAuth, WorkspaceAuth


def _require(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise SystemExit(f"Missing required environment variable: {name}")
    return v


def main() -> None:
    if os.environ.get("STRATUM_SDK_LIVE") != "1":
        sys.exit("Refusing to run: set STRATUM_SDK_LIVE=1")

    base = _require("STRATUM_SDK_LIVE_BASE_URL")
    admin_user = _require("STRATUM_SDK_LIVE_ADMIN_USER")
    agent_token = _require("STRATUM_SDK_LIVE_AGENT_TOKEN")

    suffix = (
        f"{int(time.time() * 1000)}-"
        f"{''.join(random.choices(string.ascii_lowercase + string.digits, k=8))}"
    )
    ws_name = f"example-py-{suffix}"
    ws_root = f"/sdk-smoke/{suffix}"

    with StratumClient(base, UserAuth(admin_user)) as admin:
        ws = admin.workspaces.create({"name": ws_name, "root_path": ws_root})
        issued = admin.workspaces.issue_token(
            ws["id"],
            {
                "name": f"{ws_name}-token",
                "agent_token": agent_token,
                "read_prefixes": [ws_root],
                "write_prefixes": [ws_root],
            },
        )
        workspace_id = ws["id"]
        workspace_token = issued["workspace_token"]

    with StratumClient(base, WorkspaceAuth(workspace_id, workspace_token)) as client:
        client.mkdir("/docs")
        body = "hello from Python live example"
        client.write_file("/docs/README.md", body, mime_type="text/markdown")
        read_back = client.read_file("/docs/README.md")

    print(
        json.dumps(
            {
                "workspace_id": workspace_id,
                "workspace_root": ws_root,
                "read_ok": read_back == body,
                "bytes_read": len(read_back),
            }
        )
    )


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)
