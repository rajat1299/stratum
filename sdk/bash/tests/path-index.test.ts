import { describe, expect, it } from "vitest";
import type { StratumDirectoryListing } from "../src/client.js";
import { PathIndex, normalizePath, toClientPath } from "../src/path-index.js";

function listing(path: string): StratumDirectoryListing {
  return {
    path,
    entries: [
      {
        name: "README",
        is_dir: false,
        is_symlink: false,
        size: 12,
        mode: "0644",
        uid: 501,
        gid: 20,
        modified: 1_777_744_800,
      },
      {
        name: "images",
        is_dir: true,
        is_symlink: false,
        size: 0,
        mode: "0755",
        uid: 501,
        gid: 20,
        modified: 1_777_744_860,
      },
    ],
  };
}

describe("normalizePath", () => {
  it("normalizes absolute and relative paths against cwd", () => {
    expect(normalizePath("/docs/./guide/../README", "/workspace")).toBe("/docs/README");
    expect(normalizePath("notes/../today.txt", "/workspace/sub")).toBe("/workspace/sub/today.txt");
    expect(normalizePath("../outside.txt", "/workspace/sub")).toBe("/workspace/outside.txt");
    expect(normalizePath("../../root.txt", "/workspace/sub")).toBe("/root.txt");
  });

  it("keeps absolute paths stable and preserves unrestricted Stratum names", () => {
    expect(normalizePath("/", "/workspace")).toBe("/");
    expect(normalizePath(".", "/workspace")).toBe("/workspace");
    expect(normalizePath("./file with spaces", "/workspace")).toBe("/workspace/file with spaces");
    expect(normalizePath("README", "/workspace")).toBe("/workspace/README");
    expect(normalizePath("archive.v1.final", "/workspace")).toBe("/workspace/archive.v1.final");
  });

  it("converts normalized SDK paths to workspace-relative client paths", () => {
    expect(toClientPath("/")).toBe("");
    expect(toClientPath("/docs/README")).toBe("docs/README");
    expect(toClientPath("docs/README", "/workspace")).toBe("workspace/docs/README");
  });
});

describe("PathIndex", () => {
  it("tracks files and directories from directory listings", () => {
    const index = new PathIndex();

    index.recordListing(listing("/docs"));

    expect(index.isDirectory("/docs")).toBe(true);
    expect(index.isFile("/docs/README")).toBe(true);
    expect(index.isDirectory("/docs/images")).toBe(true);
    expect(index.entry("/docs/README")).toMatchObject({ name: "README", is_dir: false, size: 12 });
    expect(index.entry("/docs/images")).toMatchObject({ name: "images", is_dir: true });
  });

  it("invalidates a path subtree", () => {
    const index = new PathIndex();
    index.recordListing(listing("/docs"));
    index.recordListing({
      path: "/docs/images",
      entries: [
        {
          name: "logo.png",
          is_dir: false,
          is_symlink: false,
          size: 99,
          mode: "0644",
          uid: 501,
          gid: 20,
          modified: 1_777_744_900,
        },
      ],
    });

    index.invalidateSubtree("/docs/images");

    expect(index.isDirectory("/docs")).toBe(true);
    expect(index.isDirectory("/docs/images")).toBe(false);
    expect(index.isFile("/docs/images/logo.png")).toBe(false);
    expect(index.paths()).toEqual(["/docs", "/docs/README"]);
  });

  it("treats root invalidation as the whole workspace subtree", () => {
    const index = new PathIndex();
    index.recordListing(listing("/docs"));

    index.invalidateSubtree("/");

    expect(index.paths()).toEqual([]);
  });
});
