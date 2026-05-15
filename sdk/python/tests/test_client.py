import json
from pathlib import Path
from typing import cast

import httpx
import pytest

from stratum_sdk import BearerAuth, StratumClient
from stratum_sdk.errors import UnsupportedFeatureError
from stratum_sdk.types import CapabilityManifest

CONTRACT_FIXTURE = Path(__file__).resolve().parents[2] / "contracts" / "capabilities.v1.json"
DURABLE_CONTRACT_FIXTURE = (
    Path(__file__).resolve().parents[2] / "contracts" / "capabilities.v1.durable-cloud.json"
)


def load_capabilities_fixture() -> CapabilityManifest:
    return cast(CapabilityManifest, json.loads(CONTRACT_FIXTURE.read_text()))


def load_durable_capabilities_fixture() -> CapabilityManifest:
    return cast(CapabilityManifest, json.loads(DURABLE_CONTRACT_FIXTURE.read_text()))


def test_capabilities_contract_fixture_shape() -> None:
    fixture = load_capabilities_fixture()

    assert fixture["revision"] == "2026-05-15-1"
    assert fixture["routes"]["filesystem"]["write"]["idempotent"] is True
    assert fixture["routes"]["search"]["semantic"]["available"] is False
    assert fixture["routes"]["search"]["semantic"]["reason"] == "not implemented"
    assert fixture["routes"]["vcs"]["refs"]["list"]["available"] is True
    assert fixture["routes"]["vcs"]["refs"]["create"]["idempotent"] is True
    assert fixture["routes"]["workspaces"]["revoke_token"]["idempotent"] is False
    assert "text-unified" in fixture["diff"]["supported_fragment_kinds"]
    assert "POST /workspaces" in fixture["idempotency"]["endpoints_supported"]


def test_durable_capabilities_contract_fixture_shape() -> None:
    fixture = load_durable_capabilities_fixture()

    assert fixture["server"]["core_runtime"] == "durable-cloud"
    assert fixture["auth"]["modes"] == ["workspace"]
    assert fixture["routes"]["filesystem"]["read"]["available"] is True
    assert fixture["routes"]["filesystem"]["write"]["available"] is False
    assert fixture["routes"]["vcs"]["refs"]["list"]["available"] is True
    assert fixture["routes"]["vcs"]["refs"]["create"]["available"] is False
    assert fixture["routes"]["vcs"]["refs"]["update"]["available"] is False
    assert fixture["routes"]["vcs"]["commit"]["available"] is False
    assert fixture["routes"]["audit"]["available"] is False
    assert fixture["routes"]["workspaces"]["issue_token"]["reason"] == (
        "durable-cloud route is not supported yet"
    )
    assert fixture["routes"]["workspaces"]["revoke_token"]["reason"] == (
        "durable-cloud route is not supported yet"
    )
    assert fixture["recovery"]["scheduler_present"] is True


def test_get_capabilities_omits_configured_auth() -> None:
    fixture = load_capabilities_fixture()
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=fixture)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumClient(
            "http://example.test/",
            auth=BearerAuth("agent-token"),
            http_client=raw,
        )
        manifest = client.get_capabilities()

    assert manifest["revision"] == fixture["revision"]
    assert seen[0].method == "GET"
    assert seen[0].url.path == "/v1/capabilities"
    assert "Authorization" not in seen[0].headers
    assert "X-Stratum-Workspace" not in seen[0].headers
    assert "Idempotency-Key" not in seen[0].headers


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
