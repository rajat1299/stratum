import json

import httpx
import pytest

from stratum_sdk import StratumClient
from stratum_sdk.errors import UnsupportedFeatureError


def test_write_file_put_fs_mime_idempotency() -> None:
    seen: list[tuple[httpx.Request, httpx.Response]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        resp = httpx.Response(200, json={"written": "/p", "size": 3})
        seen.append((request, resp))
        return resp

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumClient("http://example.test/", http_client=raw)
        client.fs.write_file("doc.md", b"# x", mime_type="text/markdown")

    req = seen[0][0]
    assert req.method == "PUT"
    assert req.url.path == "/fs/doc.md"
    assert req.headers["X-Stratum-Mime-Type"] == "text/markdown"
    assert "Idempotency-Key" in req.headers


def test_fs_copy_query() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"copied": "/a", "to": "/b"})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumClient("http://example.test/", http_client=raw)
        client.fs.copy_path("a", "/dest/b")

    req = seen[0]
    assert req.method == "POST"
    assert req.url.path == "/fs/a"
    qs = str(req.url).split("?", 1)[1]
    assert "op=copy" in qs and "dst=" in qs


def test_search_grep_query() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"results": [], "count": 0})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumClient("http://example.test/", http_client=raw)
        client.search.grep("needle", path="src", recursive=True)

    req = seen[0]
    assert req.url.path == "/search/grep"
    qs = httpx.URL(req.url).params
    assert qs["pattern"] == "needle"
    assert qs["path"] == "src"
    assert qs["recursive"] == "true"


def test_search_semantic_unsupported() -> None:
    transport = httpx.MockTransport(lambda r: httpx.Response(200))
    with httpx.Client(transport=transport) as raw:
        client = StratumClient("http://example.test/", http_client=raw)
        with pytest.raises(UnsupportedFeatureError):
            client.search.semantic("hi")


def test_vcs_update_ref_encoded_route() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={"name": "agent/a/../b", "target": "t", "version": 1},
        )

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumClient("http://example.test/", http_client=raw)
        client.vcs.update_ref(
            "agent/a/../b",
            {"target": "t2", "expected_target": "t", "expected_version": 1},
        )

    assert seen[0].url.raw_path == b"/vcs/refs/agent/a/%252E%252E/b"


def test_reviews_approve_dismiss_merge_routes() -> None:
    calls: list[httpx.Request] = []

    def approval_state() -> dict[str, object]:
        return {
            "change_request_id": "cr1",
            "required_approvals": 1,
            "approval_count": 0,
            "approved_by": [],
            "required_reviewers": [],
            "approved_required_reviewers": [],
            "missing_required_reviewers": [],
            "approved": False,
            "matched_ref_rules": [],
            "matched_path_rules": [],
        }

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        path = request.url.path
        if path.endswith("/merge"):
            return httpx.Response(
                200,
                json={
                    "change_request": {
                        "id": "cr1",
                        "title": "t",
                        "description": None,
                        "source_ref": "s",
                        "target_ref": "t",
                        "base_commit": "b",
                        "head_commit": "h",
                        "status": "merged",
                        "created_by": 1,
                        "version": 1,
                    },
                    "approval_state": approval_state(),
                },
            )
        if path.endswith("/dismiss"):
            return httpx.Response(
                200,
                json={
                    "approval": {
                        "id": "ap1",
                        "change_request_id": "cr1",
                        "head_commit": "h",
                        "approved_by": 1,
                        "comment": None,
                        "active": False,
                        "version": 2,
                    },
                    "dismissed": True,
                    "approval_state": approval_state(),
                },
            )
        if path.endswith("/approvals"):
            return httpx.Response(
                200,
                json={
                    "approval": {
                        "id": "ap1",
                        "change_request_id": "cr1",
                        "head_commit": "h",
                        "approved_by": 1,
                        "comment": None,
                        "active": True,
                        "version": 1,
                    },
                    "approval_state": approval_state(),
                },
            )
        raise AssertionError(f"unexpected {request.method} {path}")

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumClient("http://example.test/", http_client=raw)
        client.reviews.approve("cr1")
        client.reviews.dismiss_approval("cr1", "ap1")
        client.reviews.merge("cr1")

    assert calls[0].method == "POST"
    assert calls[0].url.path == "/change-requests/cr1/approvals"
    assert "Idempotency-Key" in calls[0].headers
    assert calls[1].url.path == "/change-requests/cr1/approvals/ap1/dismiss"
    assert "Idempotency-Key" in calls[1].headers
    assert calls[2].url.path == "/change-requests/cr1/merge"
    assert "Idempotency-Key" in calls[2].headers


def test_runs_create_has_idempotency_and_json_body() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={"run_id": "r1", "root": "/runs/r1", "artifacts": "", "files": {}},
        )

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumClient("http://example.test/", http_client=raw)
        client.runs.create({"prompt": "p", "command": "c"})

    req = seen[0]
    assert req.method == "POST"
    assert req.url.path == "/runs"
    assert "Idempotency-Key" in req.headers
    assert json.loads(req.content.decode()) == {"prompt": "p", "command": "c"}


def test_workspaces_issue_token_no_idempotency_header() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "workspace_id": "ws",
                "token_id": "t1",
                "name": "n",
                "workspace_token": "secret",
                "agent_uid": 1,
                "read_prefixes": [],
                "write_prefixes": [],
                "base_ref": "main",
                "session_ref": None,
            },
        )

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumClient("http://example.test/", http_client=raw)
        client.workspaces.issue_token(
            "ws-1",
            {"name": "bot", "agent_token": "at"},
        )

    req = seen[0]
    assert req.method == "POST"
    assert req.url.path == "/workspaces/ws-1/tokens"
    assert "Idempotency-Key" not in req.headers


def test_workspace_constructor_compatibility_auth() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        StratumClient(
            "http://example.test/",
            workspace_id="w9",
            workspace_token="sekret",
            http_client=raw,
        ).fs.read_file("f.txt")

    assert calls[0].headers["Authorization"] == "Bearer sekret"
    assert calls[0].headers["X-Stratum-Workspace"] == "w9"


def test_stratum_client_context_manager_closes_owned_http_client() -> None:
    with StratumClient("http://example.test/") as client:
        inner_http = client._http._client
        assert inner_http.is_closed is False
    assert inner_http.is_closed is True
