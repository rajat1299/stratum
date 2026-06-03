import { describe, expect, it } from "vitest";
import { stratumTools } from "../src/mastra/index.js";
import { createFakeWorkspace } from "./helpers.js";

async function runTool<T>(
  t: { execute?: (input: any, context: any) => Promise<unknown> },
  input: unknown,
): Promise<T> {
  if (t.execute === undefined) throw new Error("tool has no execute");
  return (await t.execute(input, {} as never)) as T;
}

describe("mastra stratumTools", () => {
  it("execute returns stdout/stderr/exitCode when execution is available", async () => {
    const { workspace } = createFakeWorkspace({
      executeAvailable: true,
      execute: () => ({ stdout: "hi\n", stderr: "", exitCode: 0 }),
    });
    const tools = stratumTools(workspace);
    expect(await runTool(tools.execute, { command: "echo hi" })).toEqual({
      stdout: "hi\n",
      stderr: "",
      exitCode: 0,
    });
  });

  it("execute has the stable id stratum-execute", () => {
    const { workspace } = createFakeWorkspace();
    expect(stratumTools(workspace).execute.id).toBe("stratum-execute");
  });

  it("execute returns an explicit unsupported error object without the command", async () => {
    const { workspace, executeCalls } = createFakeWorkspace({ executeAvailable: false });
    const tools = stratumTools(workspace);
    const result = await runTool<{ error: string }>(tools.execute, { command: "deploy --token=SECRET_ABC" });
    expect(result).toEqual({ error: "Stratum execution is unavailable for this workspace." });
    expect(JSON.stringify(result)).not.toContain("SECRET_ABC");
    expect(executeCalls).toHaveLength(0);
  });

  it("readFile reads text and returns an error for missing files", async () => {
    const { workspace } = createFakeWorkspace({ files: { "/notes.md": "hello" } });
    const tools = stratumTools(workspace);
    expect(await runTool(tools.readFile, { path: "/notes.md" })).toEqual({ content: "hello" });
    const missing = await runTool<{ error: string }>(tools.readFile, { path: "/missing.md" });
    expect(missing.error).toContain("not found");
  });

  it("writeFile creates files and missing parents", async () => {
    const { workspace, has, read } = createFakeWorkspace();
    const tools = stratumTools(workspace);
    expect(await runTool(tools.writeFile, { path: "/a/b/c.txt", content: "data" })).toEqual({ path: "/a/b/c.txt" });
    expect(has("/a/b/c.txt")).toBe(true);
    expect(read("/a/b/c.txt")).toBe("data");
  });

  it("editFile follows exact-string replacement rules", async () => {
    const { workspace, read } = createFakeWorkspace({ files: { "/one.txt": "a b a", "/uniq.txt": "x y z" } });
    const tools = stratumTools(workspace);

    expect(await runTool(tools.editFile, { path: "/uniq.txt", oldString: "y", newString: "Y" })).toEqual({
      path: "/uniq.txt",
      occurrences: 1,
    });
    const rejected = await runTool<{ error: string }>(tools.editFile, { path: "/one.txt", oldString: "a", newString: "Z" });
    expect(rejected.error).toContain("replaceAll");
    expect(
      await runTool(tools.editFile, { path: "/one.txt", oldString: "a", newString: "Z", replaceAll: true }),
    ).toEqual({ path: "/one.txt", occurrences: 2 });
    expect(read("/one.txt")).toBe("Z b Z");
  });

  it("ls lists entries with is_dir flags", async () => {
    const { workspace } = createFakeWorkspace({ files: { "/dir/a.txt": "1", "/dir/sub/b.txt": "2" } });
    const tools = stratumTools(workspace);
    const result = await runTool<{ files: { path: string; is_dir: boolean }[] }>(tools.ls, { path: "/dir" });
    expect(result.files).toContainEqual({ path: "/dir/a.txt", is_dir: false });
    expect(result.files).toContainEqual({ path: "/dir/sub", is_dir: true });
  });
});
