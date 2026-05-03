import { describe, expect, it } from "vitest";
import { StratumClient, UnsupportedFeatureError } from "../src/index.js";
import { createLiveWorkspace, liveConfigOrSkip } from "./live-helpers.js";

function assertWorkspaceRelativePath(path: string, workspaceRoot: string): void {
  expect(path).not.toContain(workspaceRoot);
}

describe("@stratum/sdk live smoke", () => {
  it(
    "exercises filesystem, search, vcs, and runs with workspace bearer auth",
    async (ctx) => {
      const config = liveConfigOrSkip(ctx);

      const admin = new StratumClient({
        baseUrl: config.baseUrl,
        auth: { type: "user", username: config.adminUser },
      });

      const { workspace, workspaceToken } = await createLiveWorkspace(admin, config);

      const client = new StratumClient({
        baseUrl: config.baseUrl,
        auth: {
          type: "workspace",
          workspaceId: workspace.id,
          workspaceToken,
        },
      });

      const readme = "hello from live smoke";

      await client.fs.mkdir("/docs");
      const written = await client.fs.writeFile("/docs/README.md", readme, { mimeType: "text/markdown" });
      assertWorkspaceRelativePath(written.written, config.workspaceRoot);
      expect(written.written).toMatch(/README\.md$/);

      const text = await client.fs.readFile("/docs/README.md");
      expect(text).toBe(readme);

      const st = await client.fs.stat("/docs/README.md");
      expect(st.kind).toBe("file");
      expect(st.size).toBeGreaterThan(0);

      const listing = await client.fs.listDirectory("/docs");
      assertWorkspaceRelativePath(listing.path, config.workspaceRoot);
      expect(listing.entries.some((e) => e.name === "README.md")).toBe(true);

      const grep = await client.search.grep("live smoke", { path: "/docs", recursive: true });
      expect(grep.count).toBeGreaterThan(0);
      for (const m of grep.results) {
        assertWorkspaceRelativePath(m.file, config.workspaceRoot);
        expect(m.line).toContain("live smoke");
      }

      const found = await client.search.find("README.md", { path: "/docs" });
      expect(found.count).toBeGreaterThan(0);
      for (const p of found.results) {
        assertWorkspaceRelativePath(p, config.workspaceRoot);
      }

      const treeOut = await client.search.tree("/");
      expect(treeOut).toContain("docs");
      expect(treeOut).not.toContain(config.workspaceRoot);

      const statusOut = await admin.vcs.status();
      expect(typeof statusOut).toBe("string");
      expect(statusOut.length).toBeGreaterThan(0);

      const diffOut = await admin.vcs.diff(`${config.workspaceRoot}/docs/README.md`);
      expect(typeof diffOut).toBe("string");

      expect(() => client.search.semantic("anything")).toThrow(UnsupportedFeatureError);

      const runId = `live-smoke-${Date.now()}`;
      await client.runs.create({
        run_id: runId,
        prompt: "live smoke",
        command: "echo hello",
        stdout: "hello\n",
        stderr: "",
        status: "succeeded",
        exit_code: 0,
      });

      const record = await client.runs.get(runId);
      expect(record.run_id).toBe(runId);

      const out = await client.runs.stdout(runId);
      expect(out).toContain("hello");

      const err = await client.runs.stderr(runId);
      expect(typeof err).toBe("string");
    },
    60_000,
  );
});
