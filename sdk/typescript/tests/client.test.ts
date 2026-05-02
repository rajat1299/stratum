import { describe, expect, it } from "vitest";
import { StratumClient, UnsupportedFeatureError, type IssueWorkspaceTokenOptions } from "../src/index.js";

function jsonResponse(body: unknown): Response {
  return Response.json(body);
}

function textResponse(body: string): Response {
  return new Response(body);
}

function recordFetch(response: Response = jsonResponse({ ok: true })) {
  const requests: Request[] = [];
  const fetchImpl: typeof fetch = async (input, init) => {
    requests.push(new Request(input, init));
    return response.clone();
  };

  return { fetchImpl, requests };
}

async function requestBody(request: Request): Promise<unknown> {
  const text = await request.text();
  return text === "" ? undefined : JSON.parse(text);
}

describe("resource clients", () => {
  it("builds filesystem calls with auth, body, and supplied idempotency", async () => {
    const { fetchImpl, requests } = recordFetch(jsonResponse({ written: "docs/a.txt", size: 2 }));
    const client = new StratumClient({
      baseUrl: "https://stratum.example/api/",
      auth: { type: "bearer", token: "agent-token" },
      fetch: fetchImpl,
    });

    await client.fs.writeFile("/docs/../docs/a.txt", "hi", {
      mimeType: "text/plain",
      idempotencyKey: "retry-1",
    });

    expect(requests[0]?.method).toBe("PUT");
    expect(requests[0]?.url).toBe("https://stratum.example/api/fs/docs/a.txt");
    expect(requests[0]?.headers.get("Authorization")).toBe("Bearer agent-token");
    expect(requests[0]?.headers.get("X-Stratum-Mime-Type")).toBe("text/plain");
    expect(requests[0]?.headers.get("Idempotency-Key")).toBe("retry-1");
    expect(await requests[0]?.text()).toBe("hi");
  });

  it("rejects root stat because the server root fs route returns a listing", async () => {
    const { fetchImpl, requests } = recordFetch();
    const client = new StratumClient({
      baseUrl: "https://stratum.example/api/",
      auth: { type: "user", username: "alice" },
      fetch: fetchImpl,
    });

    await expect(client.fs.stat("/")).rejects.toThrow("workspace root");
    expect(requests).toHaveLength(0);
  });

  it("builds search calls and rejects semantic search", async () => {
    const { fetchImpl, requests } = recordFetch(jsonResponse({ results: [], count: 0 }));
    const client = new StratumClient({
      baseUrl: "https://stratum.example",
      auth: { type: "user", username: "alice" },
      fetch: fetchImpl,
    });

    await client.search.grep("TODO", { path: "docs", recursive: false });

    expect(requests[0]?.method).toBe("GET");
    expect(requests[0]?.url).toBe("https://stratum.example/search/grep?pattern=TODO&path=docs&recursive=false");
    expect(() => client.search.semantic("refund policy")).toThrow(UnsupportedFeatureError);
  });

  it("builds vcs refs with safely encoded slash-containing names and auto idempotency", async () => {
    const { fetchImpl, requests } = recordFetch(jsonResponse({ name: "agent/a/b", target: "b".repeat(64), version: 2 }));
    const client = new StratumClient({
      baseUrl: "https://stratum.example",
      auth: { type: "user", username: "root" },
      idempotencyKeyPrefix: "test-sdk",
      fetch: fetchImpl,
    });

    await client.vcs.updateRef("agent/legal bot/session/feature#1", {
      target: "b".repeat(64),
      expected_target: "a".repeat(64),
      expected_version: 1,
    });

    expect(requests[0]?.method).toBe("PATCH");
    expect(requests[0]?.url).toBe(
      "https://stratum.example/vcs/refs/agent/legal%20bot/session/feature%231",
    );
    expect(requests[0]?.headers.get("Idempotency-Key")).toMatch(/^test-sdk-/);
    expect(await requestBody(requests[0]!)).toEqual({
      target: "b".repeat(64),
      expected_target: "a".repeat(64),
      expected_version: 1,
    });
  });

  it("builds review mutation calls with supplied idempotency", async () => {
    const { fetchImpl, requests } = recordFetch();
    const client = new StratumClient({
      baseUrl: "https://stratum.example",
      auth: { type: "user", username: "root" },
      fetch: fetchImpl,
    });

    await client.reviews.createComment(
      "cr_1",
      { body: "Please update", kind: "changes_requested" },
      { idempotencyKey: "comment-1" },
    );

    expect(requests[0]?.method).toBe("POST");
    expect(requests[0]?.url).toBe("https://stratum.example/change-requests/cr_1/comments");
    expect(requests[0]?.headers.get("Idempotency-Key")).toBe("comment-1");
    expect(await requestBody(requests[0]!)).toEqual({ body: "Please update", kind: "changes_requested" });
  });

  it("builds run creation and raw stdout calls", async () => {
    const { fetchImpl, requests } = recordFetch(jsonResponse({ run_id: "run_1" }));
    const client = new StratumClient({
      baseUrl: "https://stratum.example",
      auth: { type: "workspace", workspaceId: "ws_1", workspaceToken: "secret" },
      fetch: fetchImpl,
    });

    await client.runs.create({ prompt: "Do work", command: "bun test" });

    expect(requests[0]?.method).toBe("POST");
    expect(requests[0]?.url).toBe("https://stratum.example/runs");
    expect(requests[0]?.headers.get("Authorization")).toBe("Bearer secret");
    expect(requests[0]?.headers.get("X-Stratum-Workspace")).toBe("ws_1");
    expect(requests[0]?.headers.get("Idempotency-Key")).toMatch(/^stratum-sdk-/);

    const stdoutRecorder = recordFetch(textResponse("ok"));
    const stdoutClient = new StratumClient({
      baseUrl: "https://stratum.example",
      auth: { type: "workspace", workspaceId: "ws_1", workspaceToken: "secret" },
      fetch: stdoutRecorder.fetchImpl,
    });
    await stdoutClient.runs.stdout("run_1");
    expect(stdoutRecorder.requests[0]?.url).toBe("https://stratum.example/runs/run_1/stdout");
  });

  it("builds workspace create with idempotency but token issuance without it", async () => {
    const { fetchImpl, requests } = recordFetch(jsonResponse({ id: "ws_1" }));
    const client = new StratumClient({
      baseUrl: "https://stratum.example",
      auth: { type: "user", username: "root" },
      fetch: fetchImpl,
    });

    await client.workspaces.create({ name: "incident", root_path: "/incidents/one" });
    await client.workspaces.issueToken("ws_1", {
      name: "ci",
      agent_token: "agent-token",
    });

    expect(requests[0]?.method).toBe("POST");
    expect(requests[0]?.url).toBe("https://stratum.example/workspaces");
    expect(requests[0]?.headers.get("Idempotency-Key")).toMatch(/^stratum-sdk-/);
    expect(requests[1]?.method).toBe("POST");
    expect(requests[1]?.url).toBe("https://stratum.example/workspaces/ws_1/tokens");
    expect(requests[1]?.headers.has("Idempotency-Key")).toBe(false);
  });

  it("keeps bash-compatible methods on StratumClient", async () => {
    const { fetchImpl, requests } = recordFetch(textResponse("On commit abc"));
    const client = new StratumClient({
      baseUrl: "https://stratum.example",
      workspaceId: "ws_1",
      workspaceToken: "secret",
      fetch: fetchImpl,
    });

    await client.status();

    expect(requests[0]?.url).toBe("https://stratum.example/vcs/status");
    expect(requests[0]?.headers.get("Authorization")).toBe("Bearer secret");
    expect(requests[0]?.headers.get("X-Stratum-Workspace")).toBe("ws_1");
  });

  it("does not expose idempotency options for workspace token issuance", () => {
    const valid: IssueWorkspaceTokenOptions = { name: "ci", agent_token: "token" };
    const invalid: IssueWorkspaceTokenOptions = {
      name: "ci",
      agent_token: "token",
      // @ts-expect-error Workspace token issuance responses include a raw secret, so idempotent replay is unsupported.
      idempotencyKey: "unsafe-replay",
    };

    expect(valid).toEqual({ name: "ci", agent_token: "token" });
    expect(invalid).toMatchObject({ idempotencyKey: "unsafe-replay" });
  });
});
