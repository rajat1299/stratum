import { describe, expect, it } from "vitest";
import type { ToolCallOptions } from "ai";
import { StratumHttpError } from "@stratum/sdk";
import { stratumTools } from "../src/vercel/index.js";
import { createFakeWorkspace } from "./helpers.js";

const toolOptions = { toolCallId: "test-call", messages: [] } as unknown as ToolCallOptions;

// Test-only loose tool shape: `any` input keeps the helper assignable from every
// concrete tool's strongly-typed execute signature.
type AnyExecuteTool = { execute?: (input: any, options: ToolCallOptions) => unknown };

async function runTool<T>(t: AnyExecuteTool, input: unknown): Promise<T> {
  if (t.execute === undefined) throw new Error("tool has no execute");
  return (await t.execute(input, toolOptions)) as T;
}

const PNG_BYTES = new Uint8Array([0x89, 0x50, 0x4e, 0x47]);

describe("vercel stratumTools", () => {
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

  it("execute returns a stable error object without the command when unavailable", async () => {
    const { workspace, executeCalls } = createFakeWorkspace({ executeAvailable: false });
    const tools = stratumTools(workspace);
    const result = await runTool<{ error: string }>(tools.execute, {
      command: "deploy --token=SECRET_TOKEN_XYZ",
    });
    expect(result).toEqual({ error: "Stratum execution is unavailable for this workspace." });
    expect(JSON.stringify(result)).not.toContain("SECRET_TOKEN_XYZ");
    expect(executeCalls).toHaveLength(0);
  });

  it("readFile returns text for text files", async () => {
    const { workspace } = createFakeWorkspace({ files: { "/notes.md": "# Title" } });
    const tools = stratumTools(workspace);
    const result = await runTool<{ kind: string; content: string }>(tools.readFile, { path: "/notes.md" });
    expect(result.kind).toBe("text");
    expect(result.content).toBe("# Title");
  });

  it("readFile returns base64 media for images and PDFs", async () => {
    const { workspace } = createFakeWorkspace({ files: { "/img.png": PNG_BYTES, "/doc.pdf": "%PDF-1.4" } });
    const tools = stratumTools(workspace);
    const png = await runTool<{ kind: string; mimeType: string; base64: string }>(tools.readFile, { path: "/img.png" });
    expect(png.kind).toBe("media");
    expect(png.mimeType).toBe("image/png");
    expect(png.base64).toBe(Buffer.from(PNG_BYTES).toString("base64"));
    const pdf = await runTool<{ kind: string; mimeType: string }>(tools.readFile, { path: "/doc.pdf" });
    expect(pdf.kind).toBe("media");
    expect(pdf.mimeType).toBe("application/pdf");
  });

  it("readFile returns a binary metadata stub for unsupported binaries", async () => {
    const { workspace } = createFakeWorkspace({ files: { "/blob.bin": new Uint8Array([1, 2, 3]) } });
    const tools = stratumTools(workspace);
    const result = await runTool<{ kind: string; note: string }>(tools.readFile, { path: "/blob.bin" });
    expect(result.kind).toBe("binary");
    expect(result.note).toContain("Binary file /blob.bin");
  });

  it("readFile reports unavailable stat and read capabilities explicitly", async () => {
    const statUnavailable = createFakeWorkspace({
      files: { "/notes.md": "hello" },
      capabilityOverrides: (manifest) => ({
        ...manifest,
        routes: {
          ...manifest.routes,
          filesystem: {
            ...manifest.routes.filesystem,
            stat: { ...manifest.routes.filesystem.stat, available: false },
          },
        },
      }),
    });
    const statResult = await runTool<{ error: string }>(stratumTools(statUnavailable.workspace).readFile, {
      path: "/notes.md",
    });
    expect(statResult.error).toBe("filesystem.stat is unavailable for this Stratum workspace.");

    const readUnavailable = createFakeWorkspace({
      files: { "/notes.md": "hello" },
      capabilityOverrides: (manifest) => ({
        ...manifest,
        routes: {
          ...manifest.routes,
          filesystem: {
            ...manifest.routes.filesystem,
            read: { ...manifest.routes.filesystem.read, available: false },
          },
        },
      }),
    });
    const readResult = await runTool<{ error: string }>(stratumTools(readUnavailable.workspace).readFile, {
      path: "/notes.md",
    });
    expect(readResult.error).toBe("filesystem.read is unavailable for this Stratum workspace.");
  });

  it("readFile.toModelOutput maps text/content/error outputs", async () => {
    const { workspace } = createFakeWorkspace({ files: { "/notes.md": "hello", "/img.png": PNG_BYTES } });
    const tools = stratumTools(workspace);
    const readFile = tools.readFile;
    if (readFile.execute === undefined || readFile.toModelOutput === undefined) {
      throw new Error("readFile must define execute and toModelOutput");
    }
    const { execute, toModelOutput } = readFile;
    type ReadOutput = Parameters<typeof toModelOutput>[0]["output"];

    const textResult = (await execute({ path: "/notes.md" }, toolOptions)) as ReadOutput;
    expect(await toModelOutput({ toolCallId: "t", input: { path: "/notes.md" }, output: textResult })).toEqual({
      type: "text",
      value: "hello",
    });

    const mediaResult = (await execute({ path: "/img.png" }, toolOptions)) as ReadOutput;
    const mediaOut = await toModelOutput({ toolCallId: "t", input: { path: "/img.png" }, output: mediaResult });
    expect(mediaOut.type).toBe("content");

    const errorOut = await toModelOutput({
      toolCallId: "t",
      input: { path: "/missing" },
      output: { error: "File not found: /missing" },
    });
    expect(errorOut).toEqual({ type: "error-text", value: "File not found: /missing" });
  });

  it("writeFile creates files and missing parents", async () => {
    const { workspace, has, read } = createFakeWorkspace();
    const tools = stratumTools(workspace);
    expect(await runTool(tools.writeFile, { path: "/a/b/c.txt", content: "data" })).toEqual({ path: "/a/b/c.txt" });
    expect(has("/a/b/c.txt")).toBe(true);
    expect(read("/a/b/c.txt")).toBe("data");
  });

  it("editFile replaces one occurrence, rejects multiple, and supports replaceAll", async () => {
    const { workspace, read } = createFakeWorkspace({ files: { "/one.txt": "a b a", "/uniq.txt": "x y z" } });
    const tools = stratumTools(workspace);

    expect(await runTool(tools.editFile, { path: "/uniq.txt", oldString: "y", newString: "Y" })).toEqual({
      path: "/uniq.txt",
      occurrences: 1,
    });
    expect(read("/uniq.txt")).toBe("x Y z");

    const rejected = await runTool<{ error: string }>(tools.editFile, {
      path: "/one.txt",
      oldString: "a",
      newString: "Z",
    });
    expect(rejected.error).toContain("replaceAll");

    expect(
      await runTool(tools.editFile, { path: "/one.txt", oldString: "a", newString: "Z", replaceAll: true }),
    ).toEqual({ path: "/one.txt", occurrences: 2 });
    expect(read("/one.txt")).toBe("Z b Z");
  });

  it("editFile masks raw SDK errors", async () => {
    const { workspace } = createFakeWorkspace({
      files: { "/one.txt": "x y z" },
      writeError: new StratumHttpError(500, "deploy --token=SECRET stdout /Users/raj/backing"),
    });
    const result = await runTool<{ error: string }>(stratumTools(workspace).editFile, {
      path: "/one.txt",
      oldString: "y",
      newString: "Y",
    });
    expect(result.error).toBe("Stratum edit failed.");
    expect(result.error).not.toContain("SECRET");
    expect(result.error).not.toContain("stdout");
  });

  it("ls lists entries with is_dir flags", async () => {
    const { workspace } = createFakeWorkspace({ files: { "/dir/a.txt": "1", "/dir/sub/b.txt": "2" } });
    const tools = stratumTools(workspace);
    const result = await runTool<{ files: { path: string; is_dir: boolean }[] }>(tools.ls, { path: "/dir" });
    expect(result.files).toContainEqual({ path: "/dir/a.txt", is_dir: false });
    expect(result.files).toContainEqual({ path: "/dir/sub", is_dir: true });
  });
});
