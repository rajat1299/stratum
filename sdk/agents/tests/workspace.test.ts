import { describe, expect, it } from "vitest";
import { StratumHttpError, UnsupportedFeatureError, type StratumClient, type StratumVolume } from "@stratum/sdk";
import { StratumAgentWorkspace } from "../src/index.js";
import { createFakeWorkspace, fakeCapabilities } from "./helpers.js";

describe("StratumAgentWorkspace", () => {
  it("reads text and bytes through the mounted volume", async () => {
    const { workspace } = createFakeWorkspace({ files: { "/notes.md": "hello" } });
    expect(await workspace.readFileText("/notes.md")).toBe("hello");
    expect(Array.from(await workspace.readFileBytes("/notes.md"))).toEqual(
      Array.from(new TextEncoder().encode("hello")),
    );
  });

  it("writes files and creates missing parent directories", async () => {
    const { workspace, has, read } = createFakeWorkspace();
    await workspace.writeFile("/deep/nested/file.txt", "content");
    expect(has("/deep/nested/file.txt")).toBe(true);
    expect(read("/deep/nested/file.txt")).toBe("content");
  });

  it("exists returns false for a 404 and rethrows other failures", async () => {
    const present = createFakeWorkspace({ files: { "/a.txt": "x" } });
    expect(await present.workspace.exists("/a.txt")).toBe(true);
    expect(await present.workspace.exists("/missing.txt")).toBe(false);

    const failing = new StratumAgentWorkspace({
      client: {} as StratumClient,
      capabilities: fakeCapabilities(false),
      volume: {
        stat: () => Promise.reject(new StratumHttpError(500, "boom")),
      } as unknown as StratumVolume,
    });
    await expect(failing.exists("/a.txt")).rejects.toBeInstanceOf(StratumHttpError);
  });

  it("editFile rejects missing files, missing strings, and non-unique strings unless replaceAll", async () => {
    const { workspace, read } = createFakeWorkspace({
      files: { "/dup.txt": "a a a", "/one.txt": "hello world" },
    });

    await expect(workspace.editFile("/missing.txt", "x", "y")).rejects.toThrow();
    await expect(workspace.editFile("/one.txt", "absent", "y")).rejects.toThrow("string not found");
    await expect(workspace.editFile("/dup.txt", "a", "b")).rejects.toThrow("replaceAll");

    expect(await workspace.editFile("/one.txt", "world", "stratum")).toEqual({
      path: "/one.txt",
      occurrences: 1,
    });
    expect(read("/one.txt")).toBe("hello stratum");

    expect(await workspace.editFile("/dup.txt", "a", "b", true)).toEqual({
      path: "/dup.txt",
      occurrences: 3,
    });
    expect(read("/dup.txt")).toBe("b b b");
  });

  it("listFiles returns workspace paths plus is_dir flags", async () => {
    const { workspace } = createFakeWorkspace({
      files: { "/dir/a.txt": "1", "/dir/sub/b.txt": "2" },
    });
    const entries = await workspace.listFiles("/dir");
    expect(entries).toContainEqual({ path: "/dir/a.txt", is_dir: false });
    expect(entries).toContainEqual({ path: "/dir/sub", is_dir: true });
  });

  it("glob recursively walks the workspace using picomatch and not shell execution", async () => {
    const { workspace } = createFakeWorkspace({
      files: {
        "/src/a.ts": "1",
        "/src/nested/b.ts": "2",
        "/src/'odd'/c.ts": "3",
        "/README.md": "4",
      },
    });
    const matches = [...(await workspace.glob("**/*.ts"))].sort();
    expect(matches).toEqual(["/src/'odd'/c.ts", "/src/a.ts", "/src/nested/b.ts"]);
    expect(matches).not.toContain("/README.md");
  });

  it("grep delegates to the mounted volume grep", async () => {
    const { workspace } = createFakeWorkspace({ files: { "/a.txt": "alpha\nbeta\n" } });
    const result = await workspace.grep("beta", "/", true);
    expect(result.count).toBe(1);
    expect(result.results[0]?.line).toBe("beta");
  });

  it("execute throws UnsupportedFeatureError when execution is unavailable", async () => {
    const { workspace, executeCalls } = createFakeWorkspace({ executeAvailable: false });
    await expect(workspace.execute("rm -rf /", "danger")).rejects.toBeInstanceOf(UnsupportedFeatureError);
    expect(executeCalls).toHaveLength(0);
  });

  it("execute runs through the SDK execute route when execution is available", async () => {
    const { workspace, executeCalls } = createFakeWorkspace({
      executeAvailable: true,
      execute: () => ({ stdout: "done\n", stderr: "", exitCode: 0 }),
    });
    const result = await workspace.execute("bun test", "run tests");
    expect(result).toMatchObject({ stdout: "done\n", stderr: "", exitCode: 0, status: "succeeded" });
    expect(executeCalls).toEqual([{ command: "bun test", prompt: "run tests" }]);
  });

  it("does not leak the raw command in unsupported execution errors", async () => {
    const { workspace } = createFakeWorkspace({ executeAvailable: false });
    const secretCommand = "deploy --token=SUPER_SECRET_TOKEN_VALUE";
    let message = "";
    try {
      await workspace.execute(secretCommand);
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).not.toContain("SUPER_SECRET_TOKEN_VALUE");
    expect(message).not.toContain("deploy");
  });
});
