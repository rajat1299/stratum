import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, memoryAuthStorage } from "../lib/auth.tsx";
import { RepositoryPlaceholder } from "./RepositoryPlaceholder.tsx";

function okJson(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

function okText(body: string): Response {
  return new Response(body, {
    status: 200,
    headers: { "content-type": "text/plain" },
  });
}

function renderRepository(fetchImpl: typeof fetch) {
  globalThis.fetch = fetchImpl;
  const storage = memoryAuthStorage({ type: "user", username: "alice" });
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });

  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <AuthProvider storage={storage}>
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
      </AuthProvider>
    );
  }

  return render(<RepositoryPlaceholder />, { wrapper: Wrapper });
}

let originalFetch: typeof fetch | undefined;
beforeEach(() => {
  originalFetch = globalThis.fetch;
});
afterEach(() => {
  if (originalFetch) globalThis.fetch = originalFetch;
});

describe("RepositoryPlaceholder", () => {
  it("renders live repository refs, tree, status, and recent commits", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async (input) => {
      const url = String(typeof input === "string" || input instanceof URL ? input : input.url);
      if (url.includes("/vcs/refs")) {
        return okJson({
          refs: [
            { name: "main", target: "abc12345" + "0".repeat(56), version: 4 },
            { name: "agent/redline/cr-1", target: "def67890" + "0".repeat(56), version: 2 },
          ],
        });
      }
      if (url.includes("/vcs/status")) return okText("clean\n");
      if (url.includes("/tree")) return okText("/\n  contracts/\n    acme.md\n");
      if (url.includes("/vcs/log")) {
        return okJson({
          commits: [
            {
              hash: "abc12345" + "0".repeat(56),
              message: "merge redline",
              author: "alice",
              timestamp: 1_715_000_000,
            },
          ],
        });
      }
      return okJson({});
    });

    renderRepository(fetchSpy);

    expect(await screen.findByRole("heading", { name: /^repository$/i })).toBeTruthy();
    expect(await screen.findByText("main")).toBeTruthy();
    expect(await screen.findByText("agent/redline/cr-1")).toBeTruthy();
    expect(await screen.findByText(/contracts/)).toBeTruthy();
    expect(await screen.findByText(/acme\.md/)).toBeTruthy();
    expect(await screen.findByText(/clean/)).toBeTruthy();
    expect(await screen.findByText(/merge redline/)).toBeTruthy();
    expect(screen.queryByText(/phase b/i)).toBeNull();
  });
});
