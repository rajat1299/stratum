import { describe, expect, it } from "vitest";
import {
  StratumClient,
  buildAuthHeaders,
  generateIdempotencyKey,
  fsRoute,
  refRoute,
  treeRoute,
} from "../src/index.js";

describe("auth headers", () => {
  it("generates user, bearer, and workspace auth headers", () => {
    expect(buildAuthHeaders({ type: "user", username: "alice" }).get("Authorization")).toBe("User alice");
    expect(buildAuthHeaders({ type: "bearer", token: "tok" }).get("Authorization")).toBe("Bearer tok");

    const workspaceHeaders = buildAuthHeaders({
      type: "workspace",
      workspaceId: "ws_1",
      workspaceToken: "secret",
    });
    expect(workspaceHeaders.get("Authorization")).toBe("Bearer secret");
    expect(workspaceHeaders.get("X-Stratum-Workspace")).toBe("ws_1");
  });

  it("omits auth headers when auth is not configured", () => {
    expect(Object.fromEntries(buildAuthHeaders(undefined))).toEqual({});
  });
});

describe("safe route helpers", () => {
  it("normalizes dot segments before constructing fs and tree URLs", () => {
    expect(fsRoute("/docs/../secret/./plan.md")).toBe("fs/secret/plan.md");
    expect(fsRoute("../../etc/passwd")).toBe("fs/etc/passwd");
    expect(treeRoute("/../docs/./api")).toBe("tree/docs/api");
  });

  it("encodes ref names segment-by-segment so slashes remain route separators", () => {
    expect(refRoute("agent/legal bot/session/feature#1")).toBe(
      "vcs/refs/agent/legal%20bot/session/feature%231",
    );
    expect(refRoute("agent/a/../b")).toBe("vcs/refs/agent/a/%252E%252E/b");
    expect(refRoute("/agent/a/b")).toBe("vcs/refs//agent/a/b");
  });
});

describe("idempotency keys", () => {
  it("generates visible ASCII keys with the configured prefix", () => {
    const key = generateIdempotencyKey("custom-prefix");

    expect(key.startsWith("custom-prefix-")).toBe(true);
    expect(/^[\x21-\x7e]+$/.test(key)).toBe(true);
    expect(key.length).toBeLessThanOrEqual(255);
  });

  it("uses the sdk prefix by default for mutating methods", async () => {
    const requests: Request[] = [];
    const client = new StratumClient({
      baseUrl: "https://stratum.example",
      auth: { type: "user", username: "alice" },
      fetch: async (input, init) => {
        requests.push(new Request(input, init));
        return Response.json({ written: "docs/a.txt", size: 2 });
      },
    });

    await client.writeFile("/docs/a.txt", "hi");

    expect(requests[0]?.headers.get("Idempotency-Key")).toMatch(/^stratum-sdk-/);
  });
});
