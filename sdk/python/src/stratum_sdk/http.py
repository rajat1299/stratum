"""Low-level HTTP client (sync httpx); matches sdk/typescript/src/http.ts behavior."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping, MutableMapping
from typing import Any, Literal, cast
from urllib.parse import urlencode

import httpx

from stratum_sdk.errors import StratumHttpError

BodyType = Mapping[str, Any] | list[Any] | str | bytes | bytearray | None

ResponseKind = Literal["json", "text", "bytes"]


class UserAuth:
    __slots__ = ("username",)

    def __init__(self, username: str) -> None:
        self.username = username


class BearerAuth:
    __slots__ = ("token",)

    def __init__(self, token: str) -> None:
        self.token = token


class WorkspaceAuth:
    __slots__ = ("workspace_id", "workspace_token")

    def __init__(self, workspace_id: str, workspace_token: str) -> None:
        self.workspace_id = workspace_id
        self.workspace_token = workspace_token


AuthType = UserAuth | BearerAuth | WorkspaceAuth | None


def build_auth_headers(auth: AuthType) -> dict[str, str]:
    if isinstance(auth, UserAuth):
        return {"Authorization": f"User {auth.username}"}
    if isinstance(auth, BearerAuth):
        return {"Authorization": f"Bearer {auth.token}"}
    if isinstance(auth, WorkspaceAuth):
        return {
            "Authorization": f"Bearer {auth.workspace_token}",
            "X-Stratum-Workspace": auth.workspace_id,
        }
    return {}


def generate_idempotency_key(prefix: str = "stratum-python-sdk") -> str:
    safe_prefix = re.sub(r"[^\x21-\x7e]", "-", prefix)[:160] or "stratum-python-sdk"
    suffix = str(uuid.uuid4())
    return f"{safe_prefix}-{suffix}"[:255]


def _ensure_trailing_slash(base_url: str) -> str:
    return base_url if base_url.endswith("/") else f"{base_url}/"


def _strip_leading_slash(value: str) -> str:
    return value.lstrip("/")


def _merge_headers(
    extras: Mapping[str, str] | None,
    auth_headers: dict[str, str],
) -> dict[str, str]:
    out = dict(extras or {})
    out.update(auth_headers)
    return out


def _has_content_type(headers: MutableMapping[str, str]) -> bool:
    return any(k.lower() == "content-type" for k in headers)


def _prepare_content(
    body: BodyType,
    headers: MutableMapping[str, str],
) -> bytes | str | None:
    if body is None:
        return None
    if isinstance(body, (bytes, bytearray)):
        return bytes(body)
    if isinstance(body, str):
        return body
    if not _has_content_type(headers):
        headers["Content-Type"] = "application/json"
    return json.dumps(body, separators=(",", ":"), ensure_ascii=False)


def _build_url(base_url: str, route: str, query: list[tuple[str, str]] | None) -> str:
    path = _strip_leading_slash(route)
    url = f"{base_url}{path}"
    if query:
        url = f"{url}?{urlencode(query)}"
    return url


class StratumHttpClient:
    """Synchronous HTTP client with Stratum auth and idempotency semantics."""

    def __init__(
        self,
        base_url: str,
        auth: AuthType = None,
        *,
        client: httpx.Client | None = None,
        timeout: float | httpx.Timeout | None = None,
        idempotency_key_prefix: str = "stratum-python-sdk",
    ) -> None:
        self._base_url = _ensure_trailing_slash(base_url)
        self._auth = auth
        self._idempotency_key_prefix = idempotency_key_prefix
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> StratumHttpClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def request_json(
        self,
        route: str,
        method: str,
        *,
        query: list[tuple[str, str]] | None = None,
        headers: Mapping[str, str] | None = None,
        body: BodyType = None,
        idempotency_key: str | None = None,
        auto_idempotency: bool = False,
    ) -> Any:
        return self._request(
            route, method, query, headers, body, auto_idempotency, idempotency_key, "json"
        )

    def request_text(
        self,
        route: str,
        method: str,
        *,
        query: list[tuple[str, str]] | None = None,
        headers: Mapping[str, str] | None = None,
        body: BodyType = None,
        idempotency_key: str | None = None,
        auto_idempotency: bool = False,
    ) -> str:
        return cast(
            str,
            self._request(
                route, method, query, headers, body, auto_idempotency, idempotency_key, "text"
            ),
        )

    def request_bytes(
        self,
        route: str,
        method: str,
        *,
        query: list[tuple[str, str]] | None = None,
        headers: Mapping[str, str] | None = None,
        body: BodyType = None,
        idempotency_key: str | None = None,
        auto_idempotency: bool = False,
    ) -> bytes:
        return cast(
            bytes,
            self._request(
                route, method, query, headers, body, auto_idempotency, idempotency_key, "bytes"
            ),
        )

    def _request(
        self,
        route: str,
        method: str,
        query: list[tuple[str, str]] | None,
        headers: Mapping[str, str] | None,
        body: BodyType,
        auto_idempotency: bool,
        idempotency_key: str | None,
        response_kind: ResponseKind,
    ) -> Any:
        url = _build_url(self._base_url, route, query)
        merged = _merge_headers(headers, build_auth_headers(self._auth))

        if idempotency_key is not None:
            merged["Idempotency-Key"] = idempotency_key
        elif auto_idempotency:
            merged["Idempotency-Key"] = generate_idempotency_key(self._idempotency_key_prefix)

        content = _prepare_content(body, merged)

        response = self._client.request(method, url, headers=merged, content=content)
        if not response.is_success:
            raise StratumHttpError(response.status_code, response.text)

        if response_kind == "text":
            return response.text

        if response_kind == "bytes":
            return response.content

        text = response.text
        return None if text == "" else json.loads(text)
