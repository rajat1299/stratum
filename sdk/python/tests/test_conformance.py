import json
from pathlib import Path
from typing import TypedDict, cast

import httpx

from stratum_sdk import StratumClient, UserAuth

CONFORMANCE_FIXTURE = (
    Path(__file__).resolve().parents[2] / "contracts" / "conformance.routes.v1.json"
)


class SdkCoverage(TypedDict):
    python: str | None


class ConformanceCase(TypedDict):
    id: str
    method: str
    path: str
    sdk: SdkCoverage


class ConformanceFixture(TypedDict):
    version: int
    revision: str
    capability_revision: str
    modes: list[str]
    cases: list[ConformanceCase]


def load_conformance_fixture() -> ConformanceFixture:
    return cast(ConformanceFixture, json.loads(CONFORMANCE_FIXTURE.read_text()))


def test_conformance_fixture_loader() -> None:
    fixture = load_conformance_fixture()

    assert fixture["version"] == 1
    assert fixture["revision"] == "2026-06-08-1"
    assert fixture["modes"] == ["local-state", "durable-cloud"]
    case_ids = [case["id"] for case in fixture["cases"]]
    assert "capabilities.local.public" in case_ids
    assert "auth.durable.workspaces.missing" in case_ids
    assert "unsupported.durable.audit" in case_ids


def test_python_sdk_maps_get_capabilities_to_fixture() -> None:
    fixture = load_conformance_fixture()
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"revision": fixture["capability_revision"]})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as raw:
        client = StratumClient("https://stratum.example/", http_client=raw)

        mapped_methods = [
            case["sdk"]["python"] for case in fixture["cases"] if case["sdk"]["python"] is not None
        ]
        assert "get_capabilities" in mapped_methods
        assert "write_file" in mapped_methods

        for case in fixture["cases"]:
            method = case["sdk"]["python"]
            if method is None:
                continue

            if method == "get_capabilities":
                client.get_capabilities()
                assert seen[-1].method == "GET"
                assert seen[-1].url.path == "/v1/capabilities"
                assert "Authorization" not in seen[-1].headers
                continue

            if method == "write_file":
                sdk_idempotency_key = "sdk-conformance-write"
                client = StratumClient(
                    "https://stratum.example/",
                    auth=UserAuth("root"),
                    http_client=raw,
                )
                client.write_file(
                    case["path"].removeprefix("/fs/"),
                    "conformance-sdk-body",
                    idempotency_key=sdk_idempotency_key,
                )
                assert seen[-1].method == case["method"]
                assert seen[-1].url.path == case["path"]
                assert seen[-1].headers["Authorization"] == "User root"
                assert seen[-1].headers["Idempotency-Key"] == sdk_idempotency_key
                continue

            raise AssertionError(f"unsupported python sdk mapping: {method}")
