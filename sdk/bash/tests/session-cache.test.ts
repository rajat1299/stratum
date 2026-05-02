import { describe, expect, it } from "vitest";
import type { StratumDirectoryListing, StratumStat } from "../src/client.js";
import { SessionCache } from "../src/session-cache.js";

const stat: StratumStat = {
  inode_id: 1,
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

const listing: StratumDirectoryListing = {
  path: "/docs",
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

describe("SessionCache", () => {
  it("stores stat, list, and read entries until TTL expiry", () => {
    let now = 1_000;
    const cache = new SessionCache({ ttlMs: 100, now: () => now });

    cache.setRead("/docs/README", "hello");
    cache.setStat("/docs/README", stat);
    cache.setList("/docs", listing);

    expect(cache.getRead("/docs/README")).toBe("hello");
    expect(cache.getStat("/docs/README")).toEqual(stat);
    expect(cache.getList("/docs")).toEqual(listing);

    now = 1_100;

    expect(cache.getRead("/docs/README")).toBeNull();
    expect(cache.getStat("/docs/README")).toBeNull();
    expect(cache.getList("/docs")).toBeNull();
  });

  it("invalidates every cache entry in a path subtree", () => {
    const cache = new SessionCache({ ttlMs: null });
    cache.setRead("/docs/README", "hello");
    cache.setStat("/docs/README", stat);
    cache.setList("/docs", listing);
    cache.setRead("/other/file.txt", "kept");

    cache.invalidatePath("/docs");

    expect(cache.getRead("/docs/README")).toBeNull();
    expect(cache.getStat("/docs/README")).toBeNull();
    expect(cache.getList("/docs")).toBeNull();
    expect(cache.getRead("/other/file.txt")).toBe("kept");
  });

  it("can invalidate exact parent list entries without clearing child reads", () => {
    const cache = new SessionCache({ ttlMs: null });
    cache.setRead("/docs/README", "hello");
    cache.setStat("/docs/README", stat);
    cache.setList("/docs", listing);

    cache.invalidateExact("/docs", ["list", "stat"]);

    expect(cache.getRead("/docs/README")).toBe("hello");
    expect(cache.getStat("/docs/README")).toEqual(stat);
    expect(cache.getList("/docs")).toBeNull();
  });

  it("treats root invalidation as the whole workspace subtree", () => {
    const cache = new SessionCache({ ttlMs: null });
    cache.setRead("/docs/README", "hello");
    cache.setRead("/other/file.txt", "kept");

    cache.invalidatePath("/");

    expect(cache.size()).toBe(0);
    expect(cache.getRead("/docs/README")).toBeNull();
    expect(cache.getRead("/other/file.txt")).toBeNull();
  });

  it("evicts least-recently-used entries when max bytes is exceeded", () => {
    const cache = new SessionCache({ ttlMs: null, maxBytes: 10 });
    cache.setRead("/a.txt", "12345");
    cache.setRead("/b.txt", "12345");
    cache.getRead("/a.txt");

    cache.setRead("/c.txt", "1");

    expect(cache.getRead("/a.txt")).toBe("12345");
    expect(cache.getRead("/b.txt")).toBeNull();
    expect(cache.getRead("/c.txt")).toBe("1");
  });

  it("clears all entries", () => {
    const cache = new SessionCache({ ttlMs: null });
    cache.setRead("/docs/README", "hello");
    cache.setStat("/docs/README", stat);
    cache.setList("/docs", listing);

    cache.clear();

    expect(cache.size()).toBe(0);
    expect(cache.getRead("/docs/README")).toBeNull();
    expect(cache.getStat("/docs/README")).toBeNull();
    expect(cache.getList("/docs")).toBeNull();
  });
});
