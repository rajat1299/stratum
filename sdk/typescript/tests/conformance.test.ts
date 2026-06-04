import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { StratumClient } from "../src/index.js";

type SdkCoverage = {
  typescript: string | null;
  python: string | null;
  bash: string | null;
  rust_client: string | null;
  cli: string | null;
};

type ConformanceCase = {
  id: string;
  mode: string;
  method: string;
  path: string;
  auth: string;
  idempotency?: string;
  expect: {
    status: number;
    headers?: Record<string, string>;
    json_paths?: Record<string, unknown>;
    body_contains?: string;
    replay_header?: string;
  };
  sdk: SdkCoverage;
};

type ConformanceFixture = {
  version: number;
  revision: string;
  capability_revision: string;
  modes: string[];
  cases: ConformanceCase[];
};

const fixture = JSON.parse(
  readFileSync(
    fileURLToPath(new URL("../../contracts/conformance.routes.v1.json", import.meta.url)),
    "utf8",
  ),
) as ConformanceFixture;

function recordFetch(response: Response = Response.json({ revision: fixture.capability_revision })) {
  const requests: Request[] = [];
  const fetchImpl: typeof fetch = async (input, init) => {
    requests.push(new Request(input, init));
    return response.clone();
  };
  return { fetchImpl, requests };
}

describe("conformance.routes.v1.json", () => {
  it("loads revision, modes, and case ids", () => {
    expect(fixture.version).toBe(1);
    expect(fixture.revision).toBe("2026-06-04-1");
    expect(fixture.modes).toEqual(["local-state", "durable-cloud"]);
    expect(fixture.cases.map((entry) => entry.id)).toEqual(
      expect.arrayContaining([
        "capabilities.local.public",
        "capabilities.durable.public",
        "unsupported.durable.execute",
      ]),
    );
  });

  it("maps typescript sdk methods to fixture route metadata", async () => {
    const mappedMethods = fixture.cases
      .map((entry) => entry.sdk.typescript)
      .filter((method): method is string => method !== null);
    expect(mappedMethods).toEqual(expect.arrayContaining(["getCapabilities", "writeFile"]));

    for (const entry of fixture.cases) {
      const method = entry.sdk.typescript;
      if (!method) {
        continue;
      }

      const { fetchImpl, requests } = recordFetch();
      const client = new StratumClient({
        baseUrl: "https://stratum.example",
        fetch: fetchImpl,
      });

      if (method === "getCapabilities") {
        await client.getCapabilities();
        expect(requests[0]?.method).toBe("GET");
        expect(requests[0]?.url).toBe("https://stratum.example/v1/capabilities");
        expect(requests[0]?.headers.get("authorization")).toBeNull();
        continue;
      }

      if (method === "writeFile") {
        const sdkIdempotencyKey = "sdk-conformance-write";
        const client = new StratumClient({
          baseUrl: "https://stratum.example",
          auth: { type: "user", username: "root" },
          fetch: fetchImpl,
        });
        await client.writeFile(entry.path.replace(/^\/fs\/?/, ""), "conformance-sdk-body", {
          idempotencyKey: sdkIdempotencyKey,
        });
        expect(requests[0]?.method).toBe(entry.method);
        expect(requests[0]?.url).toBe(`https://stratum.example${entry.path}`);
        expect(requests[0]?.headers.get("authorization")).toBe("User root");
        expect(requests[0]?.headers.get("idempotency-key")).toBe(sdkIdempotencyKey);
        continue;
      }

      throw new Error(`unsupported typescript sdk mapping: ${method}`);
    }
  });
});
