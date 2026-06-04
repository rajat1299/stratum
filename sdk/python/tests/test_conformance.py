import json
from pathlib import Path

import httpx

from stratum_sdk import StratumClient

CONFORMANCE_FIXTURE = (
    Path(__file__).resolve().parents[2] / "contracts" / "conformance.routes.v1.json"
)


def load_conformance_fixture() -> dict[str, object]:
    return json.loads(CONFORMANCE_FIXTURE.read_text())


def test_conformance_fixture_loader() -> None:
    fixture = load_conformance_fixture()

    assert fixture["version"] == 1
    assert fixture["revision"] == "2026-06-04-1"
    assert fixture["modes"] == ["local-state", "durable-cloud"]
    case_ids = [case["id"] for case in fixture["cases"]]
    assert "capabilities.local.public" in case_ids
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

        for case in fixture["cases"]:
            if case["sdk"]["python"] != "get_capabilities":
                continue

            client.get_capabilities()
            assert seen[-1].method == "GET"
            assert seen[-1].url.path == "/v1/capabilities"
            assert "Authorization" not in seen[-1].headers
