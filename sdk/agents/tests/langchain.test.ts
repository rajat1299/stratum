import { describe, expect, it } from "vitest";
import { StratumLangChainWorkspace, extractText } from "../src/langchain/index.js";
import { createFakeWorkspace } from "./helpers.js";

const PNG_BYTES = new Uint8Array([0x89, 0x50, 0x4e, 0x47]);

describe("StratumLangChainWorkspace", () => {
  it("id defaults to stratum and accepts a sandboxId", () => {
    const { workspace } = createFakeWorkspace();
    expect(new StratumLangChainWorkspace(workspace).id).toBe("stratum");
    expect(new StratumLangChainWorkspace(workspace, { sandboxId: "case-42" }).id).toBe("case-42");
  });

  it("execute maps Stratum execution and fails explicitly when unavailable", async () => {
    const ok = createFakeWorkspace({
      executeAvailable: true,
      execute: () => ({ stdout: "out", stderr: "err", exitCode: 0 }),
    });
    const okBackend = new StratumLangChainWorkspace(ok.workspace);
    expect(await okBackend.execute("ls")).toEqual({ output: "out\nerr", exitCode: 0, truncated: false });

    const off = createFakeWorkspace({ executeAvailable: false });
    const offBackend = new StratumLangChainWorkspace(off.workspace);
    const result = await offBackend.execute("deploy --token=SECRET_X");
    expect(result).toEqual({
      output: "Stratum execution is unavailable for this workspace.",
      exitCode: null,
      truncated: false,
    });
    expect(JSON.stringify(result)).not.toContain("SECRET_X");
    expect(off.executeCalls).toHaveLength(0);
  });

  it("write creates new files, rejects existing paths, and creates missing parents", async () => {
    const { workspace, has } = createFakeWorkspace({ files: { "/exists.txt": "x" } });
    const backend = new StratumLangChainWorkspace(workspace);

    expect(await backend.write("/deep/new.txt", "data")).toEqual({ path: "/deep/new.txt" });
    expect(has("/deep/new.txt")).toBe(true);

    const rejected = await backend.write("/exists.txt", "y");
    expect(rejected.error).toContain("already exists");
  });

  it("read supports offset/limit for text files", async () => {
    const { workspace } = createFakeWorkspace({ files: { "/lines.txt": "l0\nl1\nl2\nl3\nl4" } });
    const backend = new StratumLangChainWorkspace(workspace);
    const result = await backend.read("/lines.txt", 1, 2);
    expect(result.content).toBe("l1\nl2");
    expect(result.mimeType).toBe("text/plain");
  });

  it("read returns Uint8Array for PDF and supported image mimes", async () => {
    const { workspace } = createFakeWorkspace({ files: { "/img.png": PNG_BYTES, "/doc.pdf": "%PDF" } });
    const backend = new StratumLangChainWorkspace(workspace);
    const png = await backend.read("/img.png");
    expect(png.content).toBeInstanceOf(Uint8Array);
    expect(png.mimeType).toBe("image/png");
    const pdf = await backend.read("/doc.pdf");
    expect(pdf.content).toBeInstanceOf(Uint8Array);
    expect(pdf.mimeType).toBe("application/pdf");
  });

  it("read rejects unsupported binary mimes with an explicit binary message", async () => {
    const { workspace } = createFakeWorkspace({ files: { "/blob.bin": new Uint8Array([1, 2, 3]) } });
    const backend = new StratumLangChainWorkspace(workspace);
    const result = await backend.read("/blob.bin");
    expect(result.content).toBeUndefined();
    expect(result.error).toContain("Binary file '/blob.bin'");
  });

  it("edit follows exact-string replacement rules", async () => {
    const { workspace, read } = createFakeWorkspace({ files: { "/one.txt": "a b a" } });
    const backend = new StratumLangChainWorkspace(workspace);

    const rejected = await backend.edit("/one.txt", "a", "Z");
    expect(rejected.error).toContain("replaceAll");

    const replaced = await backend.edit("/one.txt", "a", "Z", true);
    expect(replaced).toEqual({ path: "/one.txt", occurrences: 2 });
    expect(read("/one.txt")).toBe("Z b Z");

    const missing = await backend.edit("/missing.txt", "a", "b");
    expect(missing.error).toContain("not found");
  });

  it("ls returns file info with is_dir", async () => {
    const { workspace } = createFakeWorkspace({ files: { "/dir/a.txt": "1", "/dir/sub/b.txt": "2" } });
    const backend = new StratumLangChainWorkspace(workspace);
    const result = await backend.ls("/dir");
    expect(result.files).toContainEqual({ path: "/dir/a.txt", is_dir: false });
    expect(result.files).toContainEqual({ path: "/dir/sub", is_dir: true });
  });

  it("glob uses recursive SDK listing plus picomatch, including single-quote paths", async () => {
    const { workspace } = createFakeWorkspace({
      files: { "/src/a.ts": "1", "/src/'odd'/b.ts": "2", "/src/readme.md": "3" },
    });
    const backend = new StratumLangChainWorkspace(workspace);
    const result = await backend.glob("**/*.ts");
    const paths = (result.files ?? []).map((file) => file.path).sort();
    expect(paths).toEqual(["/src/'odd'/b.ts", "/src/a.ts"]);
  });

  it("grep maps SDK grep results into deepagents GrepMatch[] with no shell fallback", async () => {
    const { workspace, executeCalls } = createFakeWorkspace({ files: { "/a.txt": "alpha\nbeta\n" } });
    const backend = new StratumLangChainWorkspace(workspace);
    const result = await backend.grep("beta");
    expect(result.matches).toEqual([{ path: "/a.txt", line: 2, text: "beta" }]);
    expect(executeCalls).toHaveLength(0);
  });

  it("uploadFiles and downloadFiles use SDK write/read", async () => {
    const { workspace, read } = createFakeWorkspace();
    const backend = new StratumLangChainWorkspace(workspace);

    const uploaded = await backend.uploadFiles([["/up/a.txt", new TextEncoder().encode("hi")]]);
    expect(uploaded).toEqual([{ path: "/up/a.txt", error: null }]);
    expect(read("/up/a.txt")).toBe("hi");

    const downloaded = await backend.downloadFiles(["/up/a.txt", "/missing.txt"]);
    expect(downloaded[0]?.error).toBeNull();
    expect(downloaded[0]?.content).toBeInstanceOf(Uint8Array);
    expect(downloaded[1]).toEqual({ path: "/missing.txt", content: null, error: "file_not_found" });
  });

  it("readRaw returns content, mime type, and ISO timestamps", async () => {
    const { workspace } = createFakeWorkspace({ files: { "/notes.md": "hello" } });
    const backend = new StratumLangChainWorkspace(workspace);
    const result = await backend.readRaw("/notes.md");
    const data = result.data as { content: unknown; mimeType: string; modified_at: string } | undefined;
    expect(data?.content).toBe("hello");
    expect(data?.mimeType).toBe("text/markdown");
    expect(data?.modified_at).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });
});

describe("extractText", () => {
  it("handles plain string and array content blocks", () => {
    const texts = extractText([
      { content: "plain message" },
      { content: [{ type: "text", text: "block one" }, { type: "image", text: "ignored" }] },
      { content: "   " },
      {},
    ]);
    expect(texts).toEqual(["plain message", "block one"]);
  });
});
