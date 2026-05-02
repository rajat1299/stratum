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
import { StratumVolume } from "../src/volume.js";

function fileStat(path: string): StratumStat {
  return {
    inode_id: path.length,
    kind: "file",
    size: 5,
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

function dirStat(): StratumStat {
  return { ...fileStat("dir"), kind: "directory", size: 0, mime_type: null, content_hash: null };
}

function symlinkStat(): StratumStat {
  return { ...fileStat("link"), kind: "symlink", size: 7 };
}

function listing(path: string): StratumDirectoryListing {
  return {
    path,
    entries: [
      {
        name: "README",
        is_dir: false,
        is_symlink: false,
        size: 5,
        mode: "0644",
        uid: 501,
        gid: 20,
        modified: 1_777_744_800,
      },
    ],
  };
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

describe("StratumVolume", () => {
  it("tracks cwd and normalizes cd targets through stat", async () => {
    const client = createClient();
    client.stat.mockResolvedValue(dirStat());
    const volume = new StratumVolume(client);

    expect(volume.pwd()).toBe("/");

    await volume.cd("workspace/./src");
    expect(volume.pwd()).toBe("/workspace/src");
    expect(client.stat).toHaveBeenCalledWith("workspace/src");

    await volume.cd("..");
    expect(volume.pwd()).toBe("/workspace");
  });

  it("synthesizes root stat because the HTTP root fs route returns a listing", async () => {
    const client = createClient();
    const volume = new StratumVolume(client, { cwd: "/workspace" });

    await expect(volume.stat("/")).resolves.toMatchObject({ kind: "directory", mode: "0755" });
    await expect(volume.cd("/")).resolves.toBe("/");

    expect(client.stat).not.toHaveBeenCalled();
  });

  it("caches directory listings and file reads by normalized path", async () => {
    const client = createClient();
    client.listDirectory.mockResolvedValue(listing("/docs"));
    client.readFile.mockResolvedValue("hello");
    const volume = new StratumVolume(client);

    await expect(volume.ls("/docs")).resolves.toEqual(listing("/docs"));
    await expect(volume.ls("docs")).resolves.toEqual(listing("/docs"));
    await expect(volume.cat("/docs/README")).resolves.toBe("hello");
    await expect(volume.cat("docs/README")).resolves.toBe("hello");

    expect(client.listDirectory).toHaveBeenCalledTimes(1);
    expect(client.listDirectory).toHaveBeenCalledWith("docs");
    expect(client.readFile).toHaveBeenCalledTimes(1);
    expect(client.readFile).toHaveBeenCalledWith("docs/README");
  });

  it("preserves binary reads and writes through the volume cache", async () => {
    const bytes = new Uint8Array([0xff, 0x00, 0x61]);
    const client = createClient();
    client.readFileBuffer.mockResolvedValue(bytes);
    client.writeFile.mockResolvedValue({ written: "/bin/out", size: bytes.byteLength });
    const volume = new StratumVolume(client);

    await expect(volume.readFileBuffer("/bin/data")).resolves.toEqual(bytes);
    await expect(volume.readFileBuffer("/bin/data")).resolves.toEqual(bytes);
    await expect(volume.cat("/bin/data")).resolves.toBe(new TextDecoder().decode(bytes));
    await volume.writeFile("/bin/out", bytes);
    await expect(volume.readFileBuffer("/bin/out")).resolves.toEqual(bytes);

    expect(client.readFileBuffer).toHaveBeenCalledTimes(1);
    expect(client.writeFile).toHaveBeenCalledWith("bin/out", bytes, undefined);
  });

  it("defensively copies binary write content before caching", async () => {
    const bytes = new Uint8Array([0xff, 0x00, 0x61]);
    const client = createClient();
    client.writeFile.mockResolvedValue({ written: "/bin/out", size: bytes.byteLength });
    const volume = new StratumVolume(client);

    await volume.writeFile("/bin/out", bytes);
    bytes[0] = 0x00;

    await expect(volume.readFileBuffer("/bin/out")).resolves.toEqual(new Uint8Array([0xff, 0x00, 0x61]));
  });

  it("serves reads from cache after write and invalidates parent listings", async () => {
    const client = createClient();
    client.listDirectory.mockResolvedValueOnce(listing("/docs")).mockResolvedValueOnce({
      path: "/docs",
      entries: [
        ...listing("/docs").entries,
        {
          name: "new.txt",
          is_dir: false,
          is_symlink: false,
          size: 3,
          mode: "0644",
          uid: 501,
          gid: 20,
          modified: 1_777_744_900,
        },
      ],
    });
    client.writeFile.mockResolvedValue({ written: "/docs/new.txt", size: 3 });
    const volume = new StratumVolume(client);

    await volume.ls("/docs");
    await expect(volume.writeFile("/docs/new.txt", "new", { idempotencyKey: "write-1" })).resolves.toEqual({
      written: "/docs/new.txt",
      size: 3,
    });

    await expect(volume.cat("/docs/new.txt")).resolves.toBe("new");
    await volume.ls("/docs");

    expect(client.writeFile).toHaveBeenCalledWith("docs/new.txt", "new", { idempotencyKey: "write-1" });
    expect(client.readFile).not.toHaveBeenCalled();
    expect(client.listDirectory).toHaveBeenCalledTimes(2);
  });

  it("invalidates every ancestor listing for mkdir-p style mutations", async () => {
    const client = createClient();
    client.listDirectory
      .mockResolvedValueOnce(listing("/"))
      .mockResolvedValueOnce(listing("/workspace"))
      .mockResolvedValueOnce(listing("/workspace/docs"))
      .mockResolvedValueOnce(listing("/"))
      .mockResolvedValueOnce(listing("/workspace"))
      .mockResolvedValueOnce(listing("/workspace/docs"));
    client.mkdir.mockResolvedValue({ created: "/workspace/docs/new/deep", type: "directory" });
    const volume = new StratumVolume(client);

    await volume.ls("/");
    await volume.ls("/workspace");
    await volume.ls("/workspace/docs");
    await volume.mkdir("/workspace/docs/new/deep");
    await volume.ls("/");
    await volume.ls("/workspace");
    await volume.ls("/workspace/docs");

    expect(client.listDirectory).toHaveBeenCalledTimes(6);
  });

  it("records UTF-8 byte sizes and symlink stats in the path index", async () => {
    const client = createClient();
    client.readFile.mockResolvedValue("é");
    client.writeFile.mockResolvedValue({ written: "/docs/unicode.txt", size: 2 });
    client.stat.mockResolvedValue(symlinkStat());
    const volume = new StratumVolume(client);

    await volume.cat("/docs/readme.txt");
    await volume.writeFile("/docs/unicode.txt", "é");
    await volume.stat("/docs/link");

    expect(volume.pathIndex.entry("/docs/readme.txt")).toMatchObject({ size: 2, is_symlink: false });
    expect(volume.pathIndex.entry("/docs/unicode.txt")).toMatchObject({ size: 2, is_symlink: false });
    expect(volume.pathIndex.entry("/docs/link")).toMatchObject({ size: 7, is_symlink: true });
  });

  it("delegates mutations with client paths and invalidates affected cached paths", async () => {
    const client = createClient();
    client.readFile.mockResolvedValueOnce("old").mockResolvedValueOnce("fresh");
    client.mkdir.mockResolvedValue({ created: "/docs/new", type: "directory" });
    client.copyPath.mockResolvedValue({ copied: "/docs/a.txt", to: "/docs/b.txt" });
    client.movePath.mockResolvedValue({ moved: "/docs/b.txt", to: "/archive/b.txt" });
    client.deletePath.mockResolvedValue({ deleted: "/archive/b.txt" });
    const volume = new StratumVolume(client);

    await expect(volume.cat("/docs/a.txt")).resolves.toBe("old");
    await volume.mkdir("/docs/new", { idempotencyKey: "mkdir-1" });
    await volume.cp("/docs/a.txt", "/docs/b.txt", { idempotencyKey: "copy-1" });
    await volume.mv("/docs/b.txt", "/archive/b.txt", { idempotencyKey: "move-1" });
    await volume.rm("/archive/b.txt", false, { idempotencyKey: "rm-1" });
    await expect(volume.cat("/docs/a.txt")).resolves.toBe("old");

    expect(client.mkdir).toHaveBeenCalledWith("docs/new", { idempotencyKey: "mkdir-1" });
    expect(client.copyPath).toHaveBeenCalledWith("docs/a.txt", "docs/b.txt", { idempotencyKey: "copy-1" });
    expect(client.movePath).toHaveBeenCalledWith("docs/b.txt", "archive/b.txt", { idempotencyKey: "move-1" });
    expect(client.deletePath).toHaveBeenCalledWith("archive/b.txt", false, { idempotencyKey: "rm-1" });
    expect(client.readFile).toHaveBeenCalledTimes(1);
  });

  it("delegates search, tree, and VCS methods using cwd-aware paths", async () => {
    const client = createClient();
    const grepResult = { results: [{ file: "docs/README", line_num: 1, line: "hello" }], count: 1 };
    const findResult = { results: ["docs/README"], count: 1 };
    const commitResult = { hash: "abc123", message: "save", author: "Agent" };
    client.grep.mockResolvedValue(grepResult);
    client.find.mockResolvedValue(findResult);
    client.tree.mockResolvedValue("docs\n  README");
    client.status.mockResolvedValue("clean");
    client.diff.mockResolvedValue("diff --git");
    client.commit.mockResolvedValue(commitResult);
    const volume = new StratumVolume(client, { cwd: "/workspace" });

    await expect(volume.grep("hello", "docs", false)).resolves.toEqual(grepResult);
    await expect(volume.find("README", "docs")).resolves.toEqual(findResult);
    await expect(volume.tree("docs")).resolves.toBe("docs\n  README");
    await expect(volume.status()).resolves.toBe("clean");
    await expect(volume.diff("docs/README")).resolves.toBe("diff --git");
    await expect(volume.commit("save", { idempotencyKey: "commit-1" })).resolves.toEqual(commitResult);

    expect(client.grep).toHaveBeenCalledWith("hello", "workspace/docs", false);
    expect(client.find).toHaveBeenCalledWith("README", "workspace/docs");
    expect(client.tree).toHaveBeenCalledWith("workspace/docs");
    expect(client.status).toHaveBeenCalledWith();
    expect(client.diff).toHaveBeenCalledWith("workspace/docs/README");
    expect(client.commit).toHaveBeenCalledWith("save", { idempotencyKey: "commit-1" });
  });
});
