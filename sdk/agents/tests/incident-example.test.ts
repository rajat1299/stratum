import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import type { CapabilityManifest } from "@stratum/sdk";
import {
  createFileDiff,
  loadIncidentExampleConfig,
  runIncidentChangeRequestExample,
} from "../examples/incident-change-request.js";

const capabilities = JSON.parse(
  readFileSync(
    fileURLToPath(new URL("../../contracts/capabilities.v1.json", import.meta.url)),
    "utf8",
  ),
) as CapabilityManifest;

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function textResponse(body: string, status = 200): Response {
  return new Response(body, { status });
}

function dirStatResponse(): Response {
  return jsonResponse({
    inode_id: 2,
    kind: "directory",
    size: 0,
    mode: "40755",
    uid: 0,
    gid: 0,
    created: 1,
    modified: 1,
    mime_type: null,
    content_hash: null,
    custom_attrs: {},
  });
}

function mainRef(target: string, version: number) {
  return { name: "main", target, version };
}

describe("incident adapter example", () => {
  it("loads seed-demo env without exposing the workspace token in the returned public config", () => {
    const config = loadIncidentExampleConfig({
      STRATUM_URL: "http://127.0.0.1:3000",
      STRATUM_WORKSPACE_ID: "11111111-1111-1111-1111-111111111111",
      STRATUM_WORKSPACE_TOKEN: "workspace-secret",
      STRATUM_REPO: "tenant-a",
    });

    expect(config.baseUrl).toBe("http://127.0.0.1:3000");
    expect(config.workspaceId).toBe("11111111-1111-1111-1111-111111111111");
    expect(config.repoId).toBe("tenant-a");
    expect(JSON.stringify(config.publicConfig)).not.toContain("workspace-secret");
  });

  it("renders create-file diffs for OpenAI Agents editor operations", () => {
    expect(createFileDiff("hello\nworld\n")).toBe("+hello\n+world\n");
    expect(createFileDiff("hello")).toBe("+hello\n");
  });

  it("redacts the workspace token from thrown public errors", async () => {
    const fetchImpl: typeof fetch = async () => textResponse("upstream echoed workspace-secret", 500);

    let caught: unknown;
    try {
      await runIncidentChangeRequestExample({
        env: {
          STRATUM_URL: "http://stratum.example",
          STRATUM_WORKSPACE_ID: "11111111-1111-1111-1111-111111111111",
          STRATUM_WORKSPACE_TOKEN: "workspace-secret",
        },
        fetch: fetchImpl,
      });
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(Error);
    const message = (caught as Error).message;
    expect(message).toContain("[redacted]");
    expect(message).not.toContain("workspace-secret");
    expect((caught as Error).stack ?? "").not.toContain("workspace-secret");
  });

  it("reads, writes, commits, resets main, creates a source ref, and opens a CR without token leakage", async () => {
    const baseline = "a".repeat(64);
    const update = "b".repeat(64);
    const requests: Request[] = [];
    let refsCall = 0;

    const fetchImpl: typeof fetch = async (input, init) => {
      const request = new Request(input, init);
      requests.push(request.clone());
      const url = new URL(request.url);
      const path = url.pathname;

      if (path === "/v1/capabilities") return jsonResponse(capabilities);

      if (path === "/vcs/commit") {
        return jsonResponse({ hash: refsCall === 0 ? "aaaaaaa" : "bbbbbbb", message: "ok", author: "root" });
      }

      if (path === "/vcs/refs" && request.method === "GET") {
        refsCall += 1;
        return jsonResponse({ refs: [refsCall === 1 ? mainRef(baseline, 1) : mainRef(update, 2)] });
      }

      if (path === "/vcs/refs/main" && request.method === "PATCH") {
        expect(await request.clone().json()).toEqual({
          expected_target: update,
          expected_version: 2,
          target: baseline,
        });
        return jsonResponse(mainRef(baseline, 3));
      }

      if (path === "/vcs/refs" && request.method === "POST") {
        const body = await request.clone().json();
        expect(body).toMatchObject({ target: update });
        expect(body.name).toMatch(/^agent\/incident-demo\/task7-/);
        return jsonResponse({ name: body.name, target: update, version: 1 }, 201);
      }

      if (path === "/change-requests" && request.method === "POST") {
        const body = await request.clone().json();
        expect(body).toMatchObject({ source_ref: expect.stringMatching(/^agent\/incident-demo\/task7-/), target_ref: "main" });
        return jsonResponse({
          change_request: {
            id: "cr-1",
            title: body.title,
            description: body.description,
            source_ref: body.source_ref,
            target_ref: "main",
            base_commit: baseline,
            head_commit: update,
            status: "open",
            created_by: 0,
            version: 1,
          },
          approval_state: {
            change_request_id: "cr-1",
            required_approvals: 0,
            approval_count: 0,
            approved_by: [],
            required_reviewers: [],
            approved_required_reviewers: [],
            missing_required_reviewers: [],
            approved: true,
            matched_ref_rules: [],
            matched_path_rules: [],
            require_all_files_viewed: false,
          },
          require_all_files_viewed: false,
        }, 201);
      }

      if (path === "/vcs/diff") return textResponse("diff --git a/root-cause.md b/root-cause.md\n");

      if (url.searchParams.get("stat") === "true") return dirStatResponse();

      if (path.endsWith("/evidence.md")) return textResponse("payment_service_timeout_rate: 7.4%\ncheckout_retry_rate: 3.1x\n");
      if (path.endsWith("/hypotheses.md")) return textResponse("Payment-service timeout regression\n");
      if (path.endsWith("/payment-service.md")) return textResponse("Inspect timeout and retry changes first.\n");
      if (path.endsWith("/researcher.md")) return textResponse("Payment-service deploys have previously introduced timeout regressions.\n");

      if (request.method === "PUT" && path.endsWith(".md")) {
        return jsonResponse({ written: path.replace(/^\/fs/, ""), size: (await request.clone().text()).length });
      }

      return jsonResponse({ error: `unexpected ${request.method} ${path}` }, 500);
    };

    const result = await runIncidentChangeRequestExample({
      env: {
        STRATUM_URL: "http://stratum.example",
        STRATUM_WORKSPACE_ID: "11111111-1111-1111-1111-111111111111",
        STRATUM_WORKSPACE_TOKEN: "workspace-secret",
        STRATUM_REPO: "tenant-a",
        STRATUM_WORKSPACE_ROOT: "/demo/incident-workspace",
      },
      fetch: fetchImpl,
      now: () => new Date("2026-06-10T00:00:00Z"),
    });

    expect(result.changeRequestId).toBe("cr-1");
    expect(result.targetRef).toBe("main");
    expect(result.sourceRef).toBe("agent/incident-demo/task7-20260610T000000000Z");
    expect(result.filesRead).toContain("/incidents/checkout-latency/evidence.md");
    expect(result.filesWritten).toEqual([
      "/incidents/checkout-latency/root-cause.md",
      "/incidents/checkout-latency/remediation.md",
    ]);
    expect(result.reviewPaths).toEqual([
      "/demo/incident-workspace/incidents/checkout-latency/root-cause.md",
      "/demo/incident-workspace/incidents/checkout-latency/remediation.md",
    ]);
    expect(JSON.stringify(result)).not.toContain("workspace-secret");

    const workspaceRequests = requests.filter((request) => request.headers.get("X-Stratum-Workspace"));
    expect(workspaceRequests.map((request) => new URL(request.url).pathname)).toEqual(
      expect.arrayContaining([
        "/fs/incidents/checkout-latency/evidence.md",
        "/fs/incidents/checkout-latency/root-cause.md",
        "/fs/incidents/checkout-latency/remediation.md",
      ]),
    );
    expect(workspaceRequests.every((request) => request.headers.get("Authorization") === "Bearer workspace-secret")).toBe(true);
    expect(workspaceRequests.every((request) => request.headers.get("X-Stratum-Repo") === "tenant-a")).toBe(true);

    const adminRequests = requests.filter((request) => new URL(request.url).pathname.startsWith("/vcs") || new URL(request.url).pathname.startsWith("/change-requests"));
    expect(adminRequests.every((request) => request.headers.get("Authorization") === "User root")).toBe(true);
    expect(adminRequests.every((request) => request.headers.get("X-Stratum-Repo") === "tenant-a")).toBe(true);

    const diffRequest = requests.find((request) => new URL(request.url).pathname === "/vcs/diff");
    expect(diffRequest).toBeDefined();
    expect(new URL(diffRequest!.url).searchParams.get("base")).toBe(baseline);
    expect(new URL(diffRequest!.url).searchParams.get("head")).toBe(update);

    for (const request of requests) {
      expect(request.url).not.toContain("workspace-secret");
    }
  });
});
