"""Opt-in live smoke against a running stratum-server (STRATUM_SDK_LIVE=1)."""

from __future__ import annotations

import os
import random
import string
import time

import pytest

from stratum_sdk import StratumClient, UserAuth, WorkspaceAuth
from stratum_sdk.errors import UnsupportedFeatureError


def _live_config() -> tuple[str, str, str, str, str]:
    if os.environ.get("STRATUM_SDK_LIVE") != "1":
        pytest.skip("STRATUM_SDK_LIVE is not set to 1")
    base = os.environ.get("STRATUM_SDK_LIVE_BASE_URL", "").strip()
    admin = os.environ.get("STRATUM_SDK_LIVE_ADMIN_USER", "").strip()
    agent = os.environ.get("STRATUM_SDK_LIVE_AGENT_TOKEN", "").strip()
    if not base or not admin or not agent:
        pytest.skip(
            "Missing STRATUM_SDK_LIVE_BASE_URL, STRATUM_SDK_LIVE_ADMIN_USER, "
            "or STRATUM_SDK_LIVE_AGENT_TOKEN",
        )
    suffix = (
        f"{int(time.time() * 1000)}-"
        f"{''.join(random.choices(string.ascii_lowercase + string.digits, k=8))}"
    )
    return base, admin, agent, f"sdk-smoke-{suffix}", f"/sdk-smoke/{suffix}"


def test_python_sdk_live_smoke() -> None:
    base, admin_user, agent_token, ws_name, ws_root = _live_config()

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
        readme = "hello from live smoke"
        client.mkdir("/docs")
        written = client.write_file("/docs/README.md", readme, mime_type="text/markdown")
        assert ws_root not in written["written"]

        assert client.read_file("/docs/README.md") == readme
        st = client.stat("/docs/README.md")
        assert st["kind"] == "file"

        listing = client.list_directory("/docs")
        assert ws_root not in listing["path"]
        assert any(e["name"] == "README.md" for e in listing["entries"])

        grep = client.grep("live smoke", path="/docs", recursive=True)
        assert grep["count"] > 0
        for m in grep["results"]:
            assert ws_root not in m["file"]

        found = client.find("README.md", path="/docs")
        assert found["count"] > 0
        for p in found["results"]:
            assert ws_root not in p

        tree_out = client.tree("/")
        assert "docs" in tree_out
        assert ws_root not in tree_out

        status = client.status()
        assert len(status) > 0

        diff = client.diff("/docs/README.md")
        assert isinstance(diff, str)

        with pytest.raises(UnsupportedFeatureError):
            client.search.semantic("anything")

        run_id = f"live-smoke-{int(time.time() * 1000)}"
        client.runs.create(
            {
                "run_id": run_id,
                "prompt": "live smoke",
                "command": "echo hello",
                "stdout": "hello\n",
                "stderr": "",
                "status": "succeeded",
                "exit_code": 0,
            }
        )
        record = client.runs.get(run_id)
        assert record["run_id"] == run_id
        assert "hello" in client.runs.stdout(run_id)
        assert isinstance(client.runs.stderr(run_id), str)
