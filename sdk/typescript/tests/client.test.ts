import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  StratumClient,
  UnsupportedFeatureError,
  type ApprovalResponse,
  type CapabilityManifest,
  type ChangeRequestResponse,
  type IssueWorkspaceTokenOptions,
} from "../src/index.js";

const capabilitiesFixture = JSON.parse(
  readFileSync(fileURLToPath(new URL("../../contracts/capabilities.v1.json", import.meta.url)), "utf8"),
) as CapabilityManifest;
const durableCapabilitiesFixture = JSON.parse(
  readFileSync(
    fileURLToPath(new URL("../../contracts/capabilities.v1.durable-cloud.json", import.meta.url)),
    "utf8",
  ),
) as CapabilityManifest;

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
  it("loads the generated capability manifest contract fixture", () => {
    expect(capabilitiesFixture.revision).toBe("2026-05-17-2");
    expect(capabilitiesFixture.hints.banner).toBeNull();
    expect(capabilitiesFixture.routes.filesystem.write.idempotent).toBe(true);
    expect(capabilitiesFixture.routes.search.semantic.available).toBe(false);
    expect(capabilitiesFixture.routes.search.semantic.reason).toBe("not implemented");
    expect(capabilitiesFixture.routes.vcs.refs.list.available).toBe(true);
    expect(capabilitiesFixture.routes.vcs.refs.create.idempotent).toBe(true);
    expect(capabilitiesFixture.protection.ref_rules.require_all_files_viewed_default).toBe(true);
    expect(capabilitiesFixture.protection.path_rules.require_all_files_viewed_default).toBe(true);
    expect(capabilitiesFixture.routes.workspaces.issue_token.idempotent).toBe(false);
    expect(capabilitiesFixture.routes.workspaces.issue_token.reason).toBe("secret replay KMS is not configured");
    expect(capabilitiesFixture.routes.workspaces.revoke_token.idempotent).toBe(false);
    expect(capabilitiesFixture.diff.supported_fragment_kinds).toContain("text-unified");
    expect(capabilitiesFixture.idempotency.endpoints_supported).toContain("POST /workspaces");
  });

  it("loads the generated durable-cloud capability manifest contract fixture", () => {
    expect(durableCapabilitiesFixture.server.core_runtime).toBe("durable-cloud");
    expect(durableCapabilitiesFixture.hints.banner).toBeNull();
    expect(durableCapabilitiesFixture.auth.modes).toEqual(["workspace"]);
    expect(durableCapabilitiesFixture.routes.filesystem.read.available).toBe(true);
    expect(durableCapabilitiesFixture.routes.filesystem.write.available).toBe(true);
    expect(durableCapabilitiesFixture.routes.filesystem.write.requires).toEqual([
      "workspace-bearer",
      "durable-session-ref",
    ]);
    expect(durableCapabilitiesFixture.routes.filesystem.patch.available).toBe(true);
    expect(durableCapabilitiesFixture.routes.filesystem.delete.available).toBe(true);
    expect(durableCapabilitiesFixture.routes.filesystem.copy.available).toBe(true);
    expect(durableCapabilitiesFixture.routes.filesystem.move.available).toBe(true);
    expect(durableCapabilitiesFixture.routes.vcs.refs.list.available).toBe(true);
    expect(durableCapabilitiesFixture.routes.vcs.refs.create.available).toBe(true);
    expect(durableCapabilitiesFixture.routes.vcs.refs.create.requires).toEqual([
      "workspace-bearer",
      "durable-admin-principal",
      "repo-bound-principal",
    ]);
    expect(durableCapabilitiesFixture.routes.vcs.refs.update.available).toBe(true);
    expect(durableCapabilitiesFixture.routes.vcs.commit.available).toBe(true);
    expect(durableCapabilitiesFixture.routes.vcs.commit.requires).toContain("durable-session-ref");
    expect(durableCapabilitiesFixture.routes.vcs.revert.available).toBe(true);
    expect(durableCapabilitiesFixture.routes.review.change_requests.available).toBe(true);
    expect(durableCapabilitiesFixture.protection.ref_rules.available).toBe(true);
    expect(durableCapabilitiesFixture.protection.path_rules.available).toBe(true);
    expect(durableCapabilitiesFixture.protection.ref_rules.require_all_files_viewed_default).toBe(true);
    expect(durableCapabilitiesFixture.protection.path_rules.require_all_files_viewed_default).toBe(true);
    expect(durableCapabilitiesFixture.routes.audit.available).toBe(false);
    expect(durableCapabilitiesFixture.routes.workspaces.issue_token.reason).toBe(
      "durable-cloud route is not supported yet",
    );
    expect(durableCapabilitiesFixture.routes.workspaces.revoke_token.reason).toBe(
      "durable-cloud route is not supported yet",
    );
    expect(durableCapabilitiesFixture.recovery.scheduler_present).toBe(true);
  });

  it("fetches capabilities without sending configured auth", async () => {
    const { fetchImpl, requests } = recordFetch(jsonResponse(capabilitiesFixture));
    const client = new StratumClient({
      baseUrl: "https://stratum.example",
      auth: { type: "bearer", token: "agent-token" },
      fetch: fetchImpl,
    });

    const manifest = await client.getCapabilities();

    expect(manifest.revision).toBe(capabilitiesFixture.revision);
    expect(requests[0]?.method).toBe("GET");
    expect(requests[0]?.url).toBe("https://stratum.example/v1/capabilities");
    expect(requests[0]?.headers.has("Authorization")).toBe(false);
    expect(requests[0]?.headers.has("X-Stratum-Workspace")).toBe(false);
    expect(requests[0]?.headers.has("Idempotency-Key")).toBe(false);
  });

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

  it("builds vcs diff query refs while preserving path-only calls", async () => {
    const { fetchImpl, requests } = recordFetch(textResponse("diff --git"));
    const client = new StratumClient({
      baseUrl: "https://stratum.example",
      auth: { type: "user", username: "root" },
      fetch: fetchImpl,
    });

    await client.vcs.diff({ base: "main", head: "feature/change", path: "docs/readme.md" });
    await client.vcs.diff("docs/legacy.md");

    expect(requests[0]?.method).toBe("GET");
    expect(requests[0]?.url).toBe(
      "https://stratum.example/vcs/diff?base=main&head=feature%2Fchange&path=docs%2Freadme.md",
    );
    expect(requests[1]?.url).toBe("https://stratum.example/vcs/diff?path=docs%2Flegacy.md");
  });

  it("accepts change request responses with required file-view policy", () => {
    const response = {
      change_request: {
        id: "cr1",
        title: "Review docs",
        description: null,
        source_ref: "feature/docs",
        target_ref: "main",
        base_commit: "b".repeat(40),
        head_commit: "h".repeat(40),
        status: "open",
        created_by: 1,
        version: 1,
      },
      approval_state: {
        change_request_id: "cr1",
        required_approvals: 1,
        approval_count: 0,
        approved_by: [],
        required_reviewers: [],
        approved_required_reviewers: [],
        missing_required_reviewers: [],
        approved: false,
        matched_ref_rules: [],
        matched_path_rules: [],
        require_all_files_viewed: true,
      },
      require_all_files_viewed: true,
    } satisfies ChangeRequestResponse;

    expect(response.require_all_files_viewed).toBe(true);
    expect(response.approval_state.require_all_files_viewed).toBe(true);

    // @ts-expect-error server responses must carry resolved file-view policy.
    const missingPolicy: ChangeRequestResponse = {
      change_request: response.change_request,
      approval_state: response.approval_state,
    };
    expect(missingPolicy).toBeDefined();
  });

  it("accepts approval responses with required file-view policy", () => {
    const response = {
      approval: {
        id: "ap1",
        change_request_id: "cr1",
        head_commit: "h".repeat(40),
        approved_by: 1,
        comment: null,
        active: true,
        version: 1,
      },
      created: true,
      approval_state: {
        change_request_id: "cr1",
        required_approvals: 1,
        approval_count: 1,
        approved_by: [1],
        required_reviewers: [],
        approved_required_reviewers: [],
        missing_required_reviewers: [],
        approved: true,
        matched_ref_rules: [],
        matched_path_rules: [],
        require_all_files_viewed: true,
      },
      require_all_files_viewed: true,
    } satisfies ApprovalResponse;

    expect(response.require_all_files_viewed).toBe(true);
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

  it("forwards protected rule file-view flags", async () => {
    const { fetchImpl, requests } = recordFetch();
    const client = new StratumClient({
      baseUrl: "https://stratum.example",
      auth: { type: "user", username: "root" },
      fetch: fetchImpl,
    });

    await client.reviews.createProtectedRef({
      ref_name: "main",
      required_approvals: 1,
      require_all_files_viewed: false,
    });
    await client.reviews.createProtectedPath({
      path_prefix: "/legal",
      target_ref: "main",
      required_approvals: 2,
      require_all_files_viewed: false,
    });

    expect(await requestBody(requests[0]!)).toEqual({
      ref_name: "main",
      required_approvals: 1,
      require_all_files_viewed: false,
    });
    expect(await requestBody(requests[1]!)).toEqual({
      path_prefix: "/legal",
      target_ref: "main",
      required_approvals: 2,
      require_all_files_viewed: false,
    });
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

  it("builds workspace create with idempotency and token issuance with supplied idempotency", async () => {
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
      idempotencyKey: "issue-token-1",
    });

    expect(requests[0]?.method).toBe("POST");
    expect(requests[0]?.url).toBe("https://stratum.example/workspaces");
    expect(requests[0]?.headers.get("Idempotency-Key")).toMatch(/^stratum-sdk-/);
    expect(requests[1]?.method).toBe("POST");
    expect(requests[1]?.url).toBe("https://stratum.example/workspaces/ws_1/tokens");
    expect(requests[1]?.headers.get("Idempotency-Key")).toBe("issue-token-1");
    expect(await requestBody(requests[1]!)).toEqual({
      name: "ci",
      agent_token: "agent-token",
    });
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

  it("exposes explicit idempotency options for workspace token issuance", () => {
    const valid: IssueWorkspaceTokenOptions = {
      name: "ci",
      agent_token: "token",
      idempotencyKey: "secret-replay",
    };

    expect(valid).toEqual({ name: "ci", agent_token: "token", idempotencyKey: "secret-replay" });
  });
});
