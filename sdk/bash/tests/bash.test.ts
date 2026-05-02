import { describe, expect, it } from "vitest";
import { createBash, TOOL_DESCRIPTION } from "../src/index.js";

interface Entry {
  type: "file" | "directory";
  content?: string;
}

function createWorkspaceFetch(initial: Record<string, Entry> = {}) {
  const entries = new Map<string, Entry>([
    ["", { type: "directory" }],
    ...Object.entries(initial).map(([path, entry]) => [normalize(path), entry] as const),
  ]);
  const calls: string[] = [];

  const fetchImpl: typeof fetch = async (input, init) => {
    const url = new URL(String(input));
    calls.push(`${init?.method ?? "GET"} ${url.pathname}${url.search}`);
    const parts = url.pathname.replace(/^\/+/, "").split("/");
    const prefix = parts[0] ?? "";
    const path = decodeURIComponent(parts.slice(1).join("/"));

    if (prefix === "fs") {
      return handleFs(entries, path, url, init);
    }
    if (prefix === "search" && parts[1] === "grep") {
      const pattern = url.searchParams.get("pattern") ?? "";
      const root = normalize(url.searchParams.get("path") ?? "");
      const results = Array.from(entries.entries())
        .filter(([entryPath, entry]) => entry.type === "file" && isUnder(entryPath, root))
        .flatMap(([file, entry]) =>
          (entry.content ?? "").split("\n").flatMap((line, index) =>
            line.includes(pattern) ? [{ file, line_num: index + 1, line }] : [],
          ),
        );
      return json({ results, count: results.length });
    }
    if (prefix === "vcs" && parts[1] === "status") return text("M docs/a.txt\n");
    if (prefix === "vcs" && parts[1] === "diff") return text("diff --git a/docs/a.txt b/docs/a.txt\n");
    if (prefix === "vcs" && parts[1] === "commit") {
      const body = JSON.parse(String(init?.body ?? "{}")) as { message?: string };
      return json({ hash: "abc123", message: body.message ?? "", author: "Stratum" });
    }

    return text("not found", 404);
  };

  return { calls, entries, fetchImpl };
}

describe("createBash", () => {
  it("creates a just-bash wrapper that reads and mutates the Stratum workspace", async () => {
    const workspace = createWorkspaceFetch({
      docs: { type: "directory" },
      "docs/a.txt": { type: "file", content: "hello\n" },
    });
    const result = await createBash({
      baseUrl: "https://stratum.example",
      workspaceId: "workspace",
      workspaceToken: "token",
      fetch: workspace.fetchImpl,
    });

    await expect(result.bash.exec("cat /docs/a.txt")).resolves.toMatchObject({ stdout: "hello\n", exitCode: 0 });
    await expect(result.bash.exec("echo world > /docs/b.txt")).resolves.toMatchObject({ exitCode: 0 });
    await expect(result.bash.exec("echo again >> /docs/b.txt")).resolves.toMatchObject({ exitCode: 0 });
    await expect(result.bash.exec("mkdir -p /docs/nested")).resolves.toMatchObject({ exitCode: 0 });
    await expect(result.bash.exec("cp /docs/b.txt /docs/c.txt")).resolves.toMatchObject({ exitCode: 0 });
    await expect(result.bash.exec("mv /docs/c.txt /docs/d.txt")).resolves.toMatchObject({ exitCode: 0 });
    await expect(result.bash.exec("rm /docs/d.txt")).resolves.toMatchObject({ exitCode: 0 });
    await expect(result.bash.exec("ls /docs")).resolves.toMatchObject({
      stdout: expect.stringContaining("a.txt"),
      exitCode: 0,
    });

    expect(result.fs).toBeDefined();
    expect(result.volume).toBeDefined();
    expect(result.toolDescription).toBe(TOOL_DESCRIPTION);
    expect(workspace.entries.get("docs/b.txt")?.content).toBe("world\nagain\n");
    expect(workspace.entries.has("docs/nested")).toBe(true);
    expect(workspace.entries.has("docs/d.txt")).toBe(false);
  });

  it("registers deterministic Stratum grep and VCS commands", async () => {
    const workspace = createWorkspaceFetch({
      docs: { type: "directory" },
      "docs/a.txt": { type: "file", content: "alpha\nneedle one\n" },
      "docs/b.txt": { type: "file", content: "needle two\nomega\n" },
    });
    const { bash } = await createBash({
      baseUrl: "https://stratum.example",
      workspaceId: "workspace",
      workspaceToken: "token",
      fetch: workspace.fetchImpl,
    });

    await expect(bash.exec("grep needle /docs")).resolves.toMatchObject({
      stdout: "docs/a.txt:2:needle one\ndocs/b.txt:1:needle two\n",
      exitCode: 0,
    });
    await expect(bash.exec("cd /docs && grep needle .")).resolves.toMatchObject({
      stdout: "docs/a.txt:2:needle one\ndocs/b.txt:1:needle two\n",
      exitCode: 0,
    });
    await expect(bash.exec("status")).resolves.toMatchObject({ stdout: "M docs/a.txt\n", exitCode: 0 });
    await expect(bash.exec("cd /docs && diff a.txt")).resolves.toMatchObject({
      stdout: "diff --git a/docs/a.txt b/docs/a.txt\n",
      exitCode: 0,
    });
    await expect(bash.exec("diff /docs/a.txt")).resolves.toMatchObject({
      stdout: "diff --git a/docs/a.txt b/docs/a.txt\n",
      exitCode: 0,
    });
    await expect(bash.exec("commit save workspace")).resolves.toMatchObject({
      stdout: "abc123 save workspace\n",
      exitCode: 0,
    });
  });

  it("describes the actual Stratum shell and VCS surface without semantic search claims", () => {
    expect(TOOL_DESCRIPTION).toContain("Stratum workspace");
    expect(TOOL_DESCRIPTION).toContain("status");
    expect(TOOL_DESCRIPTION).toContain("diff [path]");
    expect(TOOL_DESCRIPTION).toContain("commit <message>");
    expect(TOOL_DESCRIPTION).toMatch(/semantic search .*not supported/);
  });
});

function handleFs(entries: Map<string, Entry>, path: string, url: URL, init?: RequestInit): Response {
  const key = normalize(path);
  const method = init?.method ?? "GET";
  const stat = url.searchParams.get("stat") === "true";

  if (method === "GET" && stat) {
    const entry = entries.get(key);
    if (!entry) return text("missing", 404);
    return json(toStat(key, entry));
  }
  if (method === "GET") {
    const entry = entries.get(key);
    if (!entry) return text("missing", 404);
    if (entry.type === "file") return text(entry.content ?? "");
    return json({
      path: key,
      entries: Array.from(children(entries, key)).map(([name, child]) => ({
        name,
        is_dir: child.type === "directory",
        is_symlink: false,
        size: child.content?.length ?? 0,
        mode: child.type === "directory" ? "0755" : "0644",
        uid: 501,
        gid: 20,
        modified: 1_777_744_800,
      })),
    });
  }
  if (method === "PUT") {
    if (new Headers(init?.headers).get("X-Stratum-Type") === "directory") {
      entries.set(key, { type: "directory" });
      return json({ created: key, type: "directory" });
    }
    const content = bodyToString(init?.body);
    entries.set(key, { type: "file", content });
    return json({ written: key, size: new TextEncoder().encode(content).byteLength });
  }
  if (method === "DELETE") {
    entries.delete(key);
    return json({ deleted: key });
  }
  if (method === "POST" && url.searchParams.get("op") === "copy") {
    const dst = normalize(url.searchParams.get("dst") ?? "");
    const entry = entries.get(key);
    if (!entry) return text("missing", 404);
    entries.set(dst, { ...entry });
    return json({ copied: key, to: dst });
  }
  if (method === "POST" && url.searchParams.get("op") === "move") {
    const dst = normalize(url.searchParams.get("dst") ?? "");
    const entry = entries.get(key);
    if (!entry) return text("missing", 404);
    entries.set(dst, { ...entry });
    entries.delete(key);
    return json({ moved: key, to: dst });
  }

  return text("unsupported", 405);
}

function* children(entries: Map<string, Entry>, path: string): Generator<[string, Entry]> {
  const prefix = path === "" ? "" : `${path}/`;
  for (const [entryPath, entry] of entries) {
    if (entryPath === path || !entryPath.startsWith(prefix)) continue;
    const rest = entryPath.slice(prefix.length);
    if (!rest.includes("/")) yield [rest, entry];
  }
}

function toStat(path: string, entry: Entry) {
  return {
    inode_id: path.length,
    kind: entry.type,
    size: entry.content?.length ?? 0,
    mode: entry.type === "directory" ? "0755" : "0644",
    uid: 501,
    gid: 20,
    created: 1_777_744_700,
    modified: 1_777_744_800,
    mime_type: entry.type === "file" ? "text/plain" : null,
    content_hash: null,
    custom_attrs: {},
  };
}

function normalize(path: string): string {
  return path.replace(/^\/+/, "").replace(/\/+$/, "");
}

function isUnder(path: string, root: string): boolean {
  return root === "" || path === root || path.startsWith(`${root}/`);
}

function bodyToString(body: BodyInit | null | undefined): string {
  if (body === undefined || body === null) return "";
  if (typeof body === "string") return body;
  if (body instanceof ArrayBuffer) return new TextDecoder().decode(body);
  if (body instanceof Uint8Array) return new TextDecoder().decode(body);
  return String(body);
}

function text(body: string, status = 200): Response {
  return new Response(body, { status });
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
