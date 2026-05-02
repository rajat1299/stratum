import json

import httpx
import pytest

from stratum_sdk.errors import StratumHttpError
from stratum_sdk.http import (
    BearerAuth,
    StratumHttpClient,
    UserAuth,
    WorkspaceAuth,
    generate_idempotency_key,
)


def test_user_auth_header() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumHttpClient("http://example.test", UserAuth("alice"), client=raw)
        client.request_json("workspaces", "GET")
    assert calls[0].headers["Authorization"] == "User alice"


def test_bearer_auth_header() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumHttpClient("http://example.test", BearerAuth("tok"), client=raw)
        client.request_json("workspaces", "GET")
    assert calls[0].headers["Authorization"] == "Bearer tok"


def test_workspace_auth_headers() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumHttpClient(
            "http://example.test",
            WorkspaceAuth("ws-1", "wsecret"),
            client=raw,
        )
        client.request_json("fs/read", "GET")
    assert calls[0].headers["Authorization"] == "Bearer wsecret"
    assert calls[0].headers["X-Stratum-Workspace"] == "ws-1"


def test_no_auth() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumHttpClient("http://example.test", None, client=raw)
        client.request_json("health", "GET")
    assert "Authorization" not in calls[0].headers


def test_caller_idempotency_key_preserved() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumHttpClient("http://example.test", client=raw)
        client.request_json(
            "fs/a",
            "PUT",
            body={"x": 1},
            idempotency_key="my-fixed-key",
            auto_idempotency=True,
        )
    assert calls[0].headers["Idempotency-Key"] == "my-fixed-key"


def test_auto_idempotency_key_for_mutations() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumHttpClient("http://example.test", client=raw, idempotency_key_prefix="pfx")
        client.request_json("fs/a", "PUT", body=b"hello", auto_idempotency=True)
    key = calls[0].headers["Idempotency-Key"]
    assert len(key.encode()) <= 255
    assert key.startswith("pfx-")
    assert all(32 <= ord(c) <= 126 for c in key)


def test_json_body_sets_content_type() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumHttpClient("http://example.test", client=raw)
        client.request_json(
            "vcs/commit",
            "POST",
            body={"message": "m"},
            auto_idempotency=False,
        )
    assert calls[0].headers["Content-Type"].startswith("application/json")
    assert json.loads(calls[0].content.decode()) == {"message": "m"}


def test_json_body_preserves_explicit_content_type() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumHttpClient("http://example.test", client=raw)
        client.request_json(
            "x",
            "POST",
            body={"a": 1},
            headers={"Content-Type": "application/vnd.test+json"},
        )
    assert calls[0].headers["Content-Type"] == "application/vnd.test+json"


def test_raw_byte_body_no_json_content_type() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumHttpClient("http://example.test", client=raw)
        client.request_json("fs/f", "PUT", body=b"raw-bytes\n", auto_idempotency=False)
    assert "content-type" not in {k.lower() for k in calls[0].headers.keys()}
    assert calls[0].content == b"raw-bytes\n"


def test_http_error_preserves_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, content="conflict-detail")

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumHttpClient("http://example.test", client=raw)
        with pytest.raises(StratumHttpError) as exc:
            client.request_json("runs", "POST", body={})
        assert exc.value.status_code == 409
        assert exc.value.body == "conflict-detail"


def test_empty_json_response_returns_none() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"")

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumHttpClient("http://example.test", client=raw)
        assert client.request_json("x", "GET") is None


def test_generate_idempotency_key_visible_ascii_bounded() -> None:
    weird = generate_idempotency_key("bad\x01\xffprefix")
    assert all(33 <= ord(c) <= 126 for c in weird)
    assert len(weird.encode()) <= 255


def test_stratum_http_closes_owned_httpx_client() -> None:
    client = StratumHttpClient("http://example.test")
    inner = client._client
    assert inner.is_closed is False
    client.close()
    assert inner.is_closed is True


def test_stratum_http_does_not_close_passed_httpx_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    inner = httpx.Client(transport=transport)
    client = StratumHttpClient("http://example.test", client=inner)
    client.close()
    assert inner.is_closed is False
    inner.close()
