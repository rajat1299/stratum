import { describe, expect, it, vi } from "vitest";
import type {
  StratumCommitResult,
  StratumCopyResult,
  StratumDeleteResult,
  StratumDirectoryListing,
  StratumFindResult,
  StratumGrepResult,
  StratumMkdirResult,
  StratumMoveResult,
  StratumRequestBody,
  StratumStat,
  StratumWriteResult,
} from "../src/client.js";
import { StratumHttpError } from "../src/client.js";
import { FsError, toFsError } from "../src/errors.js";
import { StratumFs } from "../src/stratum-fs.js";
import { StratumVolume } from "../src/volume.js";

function fileStat(path: string, size = 5): StratumStat {
  return {
    inode_id: path.length,
    kind: "file",
    size,
    mode: "0644",
    uid: 501,
    gid: 20,
    created: 1_777_744_700,
    modified: 1_777_744_800,
    mime_type: "text/plain",
    content_hash: "sha256:abc",
    custom_attrs: {},
  };
}

function dirStat(path = "dir"): StratumStat {
  return { ...fileStat(path, 0), kind: "directory", mode: "0755", mime_type: null, content_hash: null };
}

function createClient() {
  return {
    readFile: vi.fn<(_: string) => Promise<string>>(),
    readFileBuffer: vi.fn<(_: string) => Promise<Uint8Array>>(),
    writeFile: vi.fn<(_: string, __: StratumRequestBody, ___?: unknown) => Promise<StratumWriteResult>>(),
    mkdir: vi.fn<(_: string, __?: unknown) => Promise<StratumMkdirResult>>(),
    listDirectory: vi.fn<(_: string) => Promise<StratumDirectoryListing>>(),
    stat: vi.fn<(_: string) => Promise<StratumStat>>(),
    deletePath: vi.fn<(_: string, __?: boolean, ___?: unknown) => Promise<StratumDeleteResult>>(),
    copyPath: vi.fn<(_: string, __: string, ___?: unknown) => Promise<StratumCopyResult>>(),
    movePath: vi.fn<(_: string, __: string, ___?: unknown) => Promise<StratumMoveResult>>(),
    grep: vi.fn<(_: string, __?: string, ___?: boolean) => Promise<StratumGrepResult>>(),
    find: vi.fn<(_: string, __?: string) => Promise<StratumFindResult>>(),
    tree: vi.fn<(_: string) => Promise<string>>(),
    status: vi.fn<() => Promise<string>>(),
    diff: vi.fn<(_: string | undefined) => Promise<string>>(),
    commit: vi.fn<(_: string, __?: unknown) => Promise<StratumCommitResult>>(),
  };
}

function createFs() {
  const client = createClient();
  const volume = new StratumVolume(client);
  return { client, fs: new StratumFs(volume), volume };
}

describe("StratumFs", () => {
  it("implements file reads, writes, append, and buffer reads over StratumVolume", async () => {
    const { client, fs } = createFs();
    client.stat.mockResolvedValue(fileStat("docs/a.txt", 5));
    client.readFile.mockResolvedValue("hello");
    client.readFileBuffer.mockResolvedValue(new TextEncoder().encode("new"));
    client.writeFile.mockResolvedValue({ written: "docs/a.txt", size: 11 });

    await expect(fs.readFile("/docs/a.txt")).resolves.toBe("hello");
    await expect(fs.readFileBuffer("/docs/a.txt")).resolves.toEqual(new TextEncoder().encode("hello"));
    await fs.writeFile("/docs/a.txt", new TextEncoder().encode("new"));
    await fs.appendFile("/docs/a.txt", " world");

    expect(client.readFile).toHaveBeenCalledWith("docs/a.txt");
    expect(client.writeFile).toHaveBeenNthCalledWith(1, "docs/a.txt", new TextEncoder().encode("new"), undefined);
    expect(client.writeFile).toHaveBeenNthCalledWith(
      2,
      "docs/a.txt",
      new TextEncoder().encode("new world"),
      undefined,
    );
  });

  it("preserves non-UTF-8 bytes for buffer reads, binary writes, and appends", async () => {
    const { client, fs } = createFs();
    const original = new Uint8Array([0xff, 0x00]);
    const tail = new Uint8Array([0x80]);
    client.stat.mockResolvedValue(fileStat("bin/data", 2));
    client.readFileBuffer.mockResolvedValue(original);
    client.writeFile.mockResolvedValue({ written: "bin/data", size: 3 });

    await expect(fs.readFileBuffer("/bin/data")).resolves.toEqual(original);
    await fs.writeFile("/bin/data", original);
    await fs.appendFile("/bin/data", tail);

    expect(client.writeFile).toHaveBeenNthCalledWith(1, "bin/data", original, undefined);
    expect(client.writeFile).toHaveBeenNthCalledWith(2, "bin/data", new Uint8Array([0xff, 0x00, 0x80]), undefined);
  });

  it("checks remote stat before file and directory operations that need POSIX shape", async () => {
    const { client, fs } = createFs();
    client.stat.mockImplementation(async (path) => {
      if (path === "docs") return dirStat(path);
      if (path === "docs/a.txt") return fileStat(path, 9);
      if (path === "docs/link") return { ...fileStat(path, 7), kind: "symlink" };
      throw new Error("not found");
    });
    client.listDirectory.mockResolvedValue({ path: "docs", entries: [] });
    client.mkdir.mockResolvedValue({ created: "docs/new", type: "directory" });
    client.copyPath.mockResolvedValue({ copied: "docs/a.txt", to: "docs/b.txt" });

    await expect(fs.readFile("/docs")).rejects.toMatchObject({ code: "EISDIR" });
    await expect(fs.readdir("/docs/a.txt")).rejects.toMatchObject({ code: "ENOTDIR" });
    await expect(fs.stat("/docs/link")).resolves.toMatchObject({
      isFile: false,
      isDirectory: false,
      isSymbolicLink: true,
    });
    await expect(fs.mkdir("/docs")).rejects.toMatchObject({ code: "EEXIST" });
    await fs.mkdir("/docs/new");
    await expect(fs.cp("/docs", "/copy")).rejects.toMatchObject({ code: "EISDIR" });
    await fs.cp("/docs/a.txt", "/docs/b.txt");

    expect(client.mkdir).toHaveBeenCalledWith("docs/new", undefined);
    expect(client.copyPath).toHaveBeenCalledWith("docs/a.txt", "docs/b.txt", undefined);
  });

  it("maps stat, readdir, exists, rm, cp, and mv to POSIX-like filesystem behavior", async () => {
    const { client, fs } = createFs();
    client.stat.mockImplementation(async (path) => (path === "docs" ? dirStat(path) : fileStat(path, 9)));
    client.listDirectory.mockResolvedValue({
      path: "docs",
      entries: [
        {
          name: "a.txt",
          is_dir: false,
          is_symlink: false,
          size: 9,
          mode: "0644",
          uid: 501,
          gid: 20,
          modified: 1_777_744_800,
        },
        {
          name: "subdir",
          is_dir: true,
          is_symlink: false,
          size: 0,
          mode: "0755",
          uid: 501,
          gid: 20,
          modified: 1_777_744_800,
        },
      ],
    });
    client.deletePath.mockResolvedValue({ deleted: "docs/a.txt" });
    client.copyPath.mockResolvedValue({ copied: "docs/a.txt", to: "docs/b.txt" });
    client.movePath.mockResolvedValue({ moved: "docs/b.txt", to: "docs/c.txt" });

    await expect(fs.stat("/docs")).resolves.toMatchObject({ isDirectory: true, isFile: false, mode: 0o755 });
    await expect(fs.stat("/docs/a.txt")).resolves.toMatchObject({ isDirectory: false, isFile: true, size: 9 });
    await expect(fs.exists("/docs/a.txt")).resolves.toBe(true);
    await expect(fs.readdir("/docs")).resolves.toEqual(["a.txt", "subdir"]);
    await expect(fs.readdirWithFileTypes("/docs")).resolves.toEqual([
      { name: "a.txt", isFile: true, isDirectory: false, isSymbolicLink: false },
      { name: "subdir", isFile: false, isDirectory: true, isSymbolicLink: false },
    ]);

    await fs.rm("/docs/a.txt");
    await fs.cp("/docs/a.txt", "/docs/b.txt");
    await fs.mv("/docs/b.txt", "/docs/c.txt");

    expect(client.deletePath).toHaveBeenCalledWith("docs/a.txt", false, undefined);
    expect(client.copyPath).toHaveBeenCalledWith("docs/a.txt", "docs/b.txt", undefined);
    expect(client.movePath).toHaveBeenCalledWith("docs/b.txt", "docs/c.txt", undefined);
  });

  it("supports recursive mkdir by creating each missing path segment", async () => {
    const { client, fs } = createFs();
    client.stat.mockRejectedValue(new Error("not found"));
    client.mkdir.mockResolvedValue({ created: "docs", type: "directory" });

    await fs.mkdir("/docs/nested", { recursive: true });

    expect(client.mkdir).toHaveBeenNthCalledWith(1, "docs", undefined);
    expect(client.mkdir).toHaveBeenNthCalledWith(2, "docs/nested", undefined);
  });

  it("rejects non-recursive mkdir when the parent directory is missing", async () => {
    const { client, fs } = createFs();
    client.stat.mockRejectedValue(new Error("not found"));

    await expect(fs.mkdir("/missing/child")).rejects.toMatchObject({ code: "ENOENT" });

    expect(client.mkdir).not.toHaveBeenCalled();
  });

  it("returns clear ENOSYS errors for unsupported metadata and link APIs", async () => {
    const { fs } = createFs();

    await expect(fs.chmod("/x", 0o644)).rejects.toMatchObject({ code: "ENOSYS" });
    await expect(fs.symlink("/x", "/y")).rejects.toMatchObject({ code: "ENOSYS" });
    await expect(fs.link("/x", "/y")).rejects.toMatchObject({ code: "ENOSYS" });
    await expect(fs.readlink("/x")).rejects.toMatchObject({ code: "ENOSYS" });
    await expect(fs.utimes("/x", new Date(), new Date())).rejects.toMatchObject({ code: "ENOSYS" });
  });

  it("normalizes remote not-found failures into FsError for filesystem callers", async () => {
    const { client, fs } = createFs();
    client.stat.mockRejectedValue(new Error("not found"));

    await expect(fs.stat("/missing")).rejects.toBeInstanceOf(FsError);
    await expect(fs.stat("/missing")).rejects.toMatchObject({ code: "ENOENT" });
    await expect(fs.exists("/missing")).resolves.toBe(false);
  });

  it("preserves semantic filesystem errors from bad request responses", () => {
    const error = new StratumHttpError(400, '{"error":"is a directory"}', "is a directory");

    expect(toFsError(error, "/docs", "rm")).toMatchObject({ code: "EISDIR" });
  });
});
