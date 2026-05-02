import { describe, expect, it, vi } from "vitest";
import { StratumClient, StratumHttpError } from "../src/client.js";

type FetchCall = {
  readonly url: string;
  readonly init: RequestInit | undefined;
};

function makeFetch(responses: Response[] | ((call: FetchCall, index: number) => Response | Promise<Response>)) {
  const calls: FetchCall[] = [];
  const fakeFetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const call = { url: String(input), init };
    calls.push(call);

    if (Array.isArray(responses)) {
      const response = responses[calls.length - 1];
      if (!response) {
        throw new Error(`Unexpected fetch call ${calls.length}`);
      }
      return response;
    }

    return responses(call, calls.length - 1);
  }) as unknown as typeof fetch;

  return { calls, fakeFetch };
}

function jsonResponse(body: unknown, init?: ResponseInit) {
  return new Response(JSON.stringify(body), {
    headers: { "content-type": "application/json" },
    ...init,
  });
}

function createClient(fakeFetch: typeof fetch) {
  return new StratumClient({
    baseUrl: "https://stratum.example/api/",
    workspaceId: "workspace-123",
    workspaceToken: "token-abc",
    fetch: fakeFetch,
  });
}

function headersFor(call: FetchCall) {
  return new Headers(call.init?.headers);
}

describe("StratumClient", () => {
  it("sends workspace bearer auth headers on requests", async () => {
    const { calls, fakeFetch } = makeFetch([new Response("hello")]);
    const client = createClient(fakeFetch);

    await client.readFile("notes/today.txt");

    const headers = headersFor(calls[0]);
    expect(headers.get("Authorization")).toBe("Bearer token-abc");
    expect(headers.get("X-Stratum-Workspace")).toBe("workspace-123");
  });

  it("readFile calls GET /fs/<path> and returns text", async () => {
    const { calls, fakeFetch } = makeFetch([new Response("file contents")]);
    const client = createClient(fakeFetch);

    await expect(client.readFile("docs/readme.md")).resolves.toBe("file contents");

    expect(calls[0].url).toBe("https://stratum.example/api/fs/docs/readme.md");
    expect(calls[0].init?.method).toBe("GET");
  });

  it("writeFile calls PUT /fs/<path> with the body and an idempotency key", async () => {
    const { calls, fakeFetch } = makeFetch([jsonResponse({ written: "/docs/readme.md", size: 5 })]);
    const client = createClient(fakeFetch);

    await expect(client.writeFile("docs/readme.md", "hello")).resolves.toEqual({
      written: "/docs/readme.md",
      size: 5,
    });

    expect(calls[0].url).toBe("https://stratum.example/api/fs/docs/readme.md");
    expect(calls[0].init?.method).toBe("PUT");
    expect(calls[0].init?.body).toBe("hello");
    expect(headersFor(calls[0]).get("Idempotency-Key")).toMatch(/^stratum-bash-[A-Za-z0-9_-]+$/);
  });

  it("writeFile preserves a supplied idempotency key and mime type", async () => {
    const { calls, fakeFetch } = makeFetch([jsonResponse({ written: "/data.json", size: 2 })]);
    const client = createClient(fakeFetch);

    await client.writeFile("data.json", "{}", {
      idempotencyKey: "caller-key-1",
      mimeType: "application/json",
    });

    const headers = headersFor(calls[0]);
    expect(headers.get("Idempotency-Key")).toBe("caller-key-1");
    expect(headers.get("X-Stratum-Mime-Type")).toBe("application/json");
  });

  it("mkdir uses the directory header and parses the projected path", async () => {
    const { calls, fakeFetch } = makeFetch([jsonResponse({ created: "/docs", type: "directory" })]);
    const client = createClient(fakeFetch);

    await expect(client.mkdir("docs")).resolves.toEqual({ created: "/docs", type: "directory" });

    expect(calls[0].url).toBe("https://stratum.example/api/fs/docs");
    expect(calls[0].init?.method).toBe("PUT");
    const headers = headersFor(calls[0]);
    expect(headers.get("X-Stratum-Type")).toBe("directory");
    expect(headers.get("Idempotency-Key")).toMatch(/^stratum-bash-[A-Za-z0-9_-]+$/);
  });

  it("listDirectory parses directory entries using the actual is_dir shape", async () => {
    const listing = {
      path: "docs",
      entries: [
        {
          name: "guide.md",
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
    const { calls, fakeFetch } = makeFetch([jsonResponse(listing)]);
    const client = createClient(fakeFetch);

    await expect(client.listDirectory("docs")).resolves.toEqual(listing);

    expect(calls[0].url).toBe("https://stratum.example/api/fs/docs");
    expect(calls[0].init?.method).toBe("GET");
  });

  it("stat calls /fs/<path>?stat=true", async () => {
    const stat = {
      inode_id: 1,
      kind: "file",
      size: 4,
      mode: "0644",
      uid: 501,
      gid: 20,
      created: 1_777_744_800,
      modified: 1_777_744_860,
      mime_type: "text/plain",
      content_hash: "sha256:abc",
      custom_attrs: { purpose: "test" },
    };
    const { calls, fakeFetch } = makeFetch([jsonResponse(stat)]);
    const client = createClient(fakeFetch);

    await expect(client.stat("docs/readme.md")).resolves.toEqual(stat);

    expect(calls[0].url).toBe("https://stratum.example/api/fs/docs/readme.md?stat=true");
    expect(calls[0].init?.method).toBe("GET");
  });

  it("deletePath uses DELETE with recursive query and an idempotency key", async () => {
    const { calls, fakeFetch } = makeFetch([jsonResponse({ deleted: "/tmp" })]);
    const client = createClient(fakeFetch);

    await expect(client.deletePath("tmp", true)).resolves.toEqual({ deleted: "/tmp" });

    expect(calls[0].url).toBe("https://stratum.example/api/fs/tmp?recursive=true");
    expect(calls[0].init?.method).toBe("DELETE");
    expect(headersFor(calls[0]).get("Idempotency-Key")).toMatch(/^stratum-bash-[A-Za-z0-9_-]+$/);
  });

  it("copyPath uses POST with copy query, destination query, and an idempotency key", async () => {
    const { calls, fakeFetch } = makeFetch([jsonResponse({ copied: "/src/file.txt", to: "/dst/file.txt" })]);
    const client = createClient(fakeFetch);

    await expect(client.copyPath("src/file.txt", "dst/file.txt")).resolves.toEqual({
      copied: "/src/file.txt",
      to: "/dst/file.txt",
    });

    expect(calls[0].url).toBe("https://stratum.example/api/fs/src/file.txt?op=copy&dst=dst%2Ffile.txt");
    expect(calls[0].init?.method).toBe("POST");
    expect(headersFor(calls[0]).get("Idempotency-Key")).toMatch(/^stratum-bash-[A-Za-z0-9_-]+$/);
  });

  it("movePath uses POST with move query, destination query, and an idempotency key", async () => {
    const { calls, fakeFetch } = makeFetch([jsonResponse({ moved: "/src/file.txt", to: "/dst/file.txt" })]);
    const client = createClient(fakeFetch);

    await expect(client.movePath("src/file.txt", "dst/file.txt")).resolves.toEqual({
      moved: "/src/file.txt",
      to: "/dst/file.txt",
    });

    expect(calls[0].url).toBe("https://stratum.example/api/fs/src/file.txt?op=move&dst=dst%2Ffile.txt");
    expect(calls[0].init?.method).toBe("POST");
    expect(headersFor(calls[0]).get("Idempotency-Key")).toMatch(/^stratum-bash-[A-Za-z0-9_-]+$/);
  });

  it("grep calls the grep search route and parses results", async () => {
    const body = { results: [{ file: "docs/readme.md", line_num: 2, line: "hello" }], count: 1 };
    const { calls, fakeFetch } = makeFetch([jsonResponse(body)]);
    const client = createClient(fakeFetch);

    await expect(client.grep("hel+", "docs", false)).resolves.toEqual(body);

    expect(calls[0].url).toBe(
      "https://stratum.example/api/search/grep?pattern=hel%2B&path=docs&recursive=false",
    );
    expect(calls[0].init?.method).toBe("GET");
  });

  it("find calls the find search route and parses results", async () => {
    const body = { results: ["docs/readme.md"], count: 1 };
    const { calls, fakeFetch } = makeFetch([jsonResponse(body)]);
    const client = createClient(fakeFetch);

    await expect(client.find("*.md", "docs")).resolves.toEqual(body);

    expect(calls[0].url).toBe("https://stratum.example/api/search/find?path=docs&name=*.md");
    expect(calls[0].init?.method).toBe("GET");
  });

  it("tree calls /tree/<path> and returns text", async () => {
    const { calls, fakeFetch } = makeFetch([new Response("docs\n  readme.md")]);
    const client = createClient(fakeFetch);

    await expect(client.tree("docs")).resolves.toBe("docs\n  readme.md");

    expect(calls[0].url).toBe("https://stratum.example/api/tree/docs");
    expect(calls[0].init?.method).toBe("GET");
  });

  it("status calls /vcs/status and returns text", async () => {
    const { calls, fakeFetch } = makeFetch([new Response("clean")]);
    const client = createClient(fakeFetch);

    await expect(client.status()).resolves.toBe("clean");

    expect(calls[0].url).toBe("https://stratum.example/api/vcs/status");
    expect(calls[0].init?.method).toBe("GET");
  });

  it("diff calls /vcs/diff with an optional path and returns text", async () => {
    const { calls, fakeFetch } = makeFetch([new Response("diff --git")]);
    const client = createClient(fakeFetch);

    await expect(client.diff("docs/readme.md")).resolves.toBe("diff --git");

    expect(calls[0].url).toBe("https://stratum.example/api/vcs/diff?path=docs%2Freadme.md");
    expect(calls[0].init?.method).toBe("GET");
  });

  it("commit posts a JSON message with an idempotency key and parses the commit response", async () => {
    const body = { hash: "abc123", message: "Initial commit", author: "Agent" };
    const { calls, fakeFetch } = makeFetch([jsonResponse(body)]);
    const client = createClient(fakeFetch);

    await expect(client.commit("Initial commit")).resolves.toEqual(body);

    expect(calls[0].url).toBe("https://stratum.example/api/vcs/commit");
    expect(calls[0].init?.method).toBe("POST");
    expect(calls[0].init?.body).toBe(JSON.stringify({ message: "Initial commit" }));
    const headers = headersFor(calls[0]);
    expect(headers.get("Content-Type")).toBe("application/json");
    expect(headers.get("Idempotency-Key")).toMatch(/^stratum-bash-[A-Za-z0-9_-]+$/);
  });

  it("throws StratumHttpError with status, raw body, and parsed error message on non-2xx responses", async () => {
    const { fakeFetch } = makeFetch([
      jsonResponse({ error: "workspace token is not authorized" }, { status: 403 }),
    ]);
    const client = createClient(fakeFetch);

    const error = await client.status().catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(StratumHttpError);
    expect(error).toMatchObject({
      name: "StratumHttpError",
      status: 403,
      body: '{"error":"workspace token is not authorized"}',
      message: "workspace token is not authorized",
    });
  });
});
