import { describe, expect, it } from "vitest";
import { StratumClient } from "@stratum/sdk";
import { createBash } from "../src/index.js";
import { createLiveWorkspace, liveConfigOrSkip } from "../../typescript/tests/live-helpers.js";

async function expectAdminBoundedText(call: () => Promise<string>): Promise<void> {
  try {
    const out = await call();
    expect(out).not.toBe("");
  } catch (error) {
    expect(error instanceof Error ? error.message : String(error)).toMatch(
      /admin operation|permission|forbidden|403/i,
    );
  }
}

function expectAdminBoundedCommand(result: {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}): void {
  expect([0, 1]).toContain(result.exitCode);
  if (result.exitCode === 0) {
    expect(result.stdout).not.toBe("");
  } else {
    expect(result.stderr).toMatch(/admin operation|permission|forbidden|403/i);
  }
}

describe("@stratum/bash and StratumVolume live smoke", () => {
  it(
    "exercises mount cache, volume tools, and virtual bash against a live server",
    async (ctx) => {
      const config = liveConfigOrSkip(ctx);

      const admin = new StratumClient({
        baseUrl: config.baseUrl,
        auth: { type: "user", username: config.adminUser },
      });

      const { workspace, workspaceToken } = await createLiveWorkspace(admin, config);

      let fsGetCount = 0;
      const countingFetch: typeof fetch = async (input, init) => {
        const req = new Request(input, init);
        if (req.method === "GET" && new URL(req.url).pathname.includes("/fs/")) {
          fsGetCount += 1;
        }
        return globalThis.fetch(req);
      };

      const wsClient = new StratumClient({
        baseUrl: config.baseUrl,
        auth: { type: "workspace", workspaceId: workspace.id, workspaceToken },
        fetch: countingFetch,
      });

      const volume = wsClient.mount({ cwd: "/" });
      const body = "hello from live smoke";

      await volume.mkdir("/docs");
      await volume.writeFile("/docs/README.md", body);

      volume.cache.clear();
      fsGetCount = 0;
      expect(await volume.readFile("/docs/README.md")).toBe(body);
      expect(await volume.readFile("/docs/README.md")).toBe(body);
      expect(fsGetCount).toBe(1);

      expect(await volume.cd("/docs")).toBe("/docs");
      const listing = await volume.ls(".");
      expect(listing.entries.some((e) => e.name === "README.md")).toBe(true);

      const grepVol = await volume.grep("live", ".", true);
      expect(grepVol.count).toBeGreaterThan(0);
      expect(grepVol.results.some((m) => m.line.includes("live"))).toBe(true);

      const findVol = await volume.find("README.md", ".");
      expect(findVol.count).toBeGreaterThan(0);

      const treeVol = await volume.tree(".");
      expect(treeVol).toMatch(/README/);
      expect(treeVol).not.toContain(config.workspaceRoot);
      await expectAdminBoundedText(() => volume.status());
      await expectAdminBoundedText(() => volume.diff("README.md"));

      const { bash, refresh } = await createBash({
        baseUrl: config.baseUrl,
        workspaceId: workspace.id,
        workspaceToken,
      });

      await refresh();

      let r = await bash.exec("pwd");
      expect(r.exitCode).toBe(0);
      expect(r.stdout.trim()).toBe("/");

      r = await bash.exec("cat /docs/README.md");
      expect(r.exitCode).toBe(0);
      expect(r.stdout).toContain("live smoke");

      r = await bash.exec("grep live /docs");
      expect(r.exitCode).toBe(0);
      expect(r.stdout).toContain("README.md");

      r = await bash.exec("status");
      expectAdminBoundedCommand(r);

      r = await bash.exec("diff /docs/README.md");
      expectAdminBoundedCommand(r);

      r = await bash.exec("sgrep anything");
      expect(r.exitCode).toBe(2);
      expect(r.stderr).toContain("semantic search is not available");
    },
    60_000,
  );
});
