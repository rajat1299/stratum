import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, memoryAuthStorage } from "../lib/auth.tsx";
import { AuditPlaceholder } from "./AuditPlaceholder.tsx";

function okJson(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

function httpError(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function renderAudit(fetchImpl: typeof fetch) {
  globalThis.fetch = fetchImpl;
  const storage = memoryAuthStorage({ type: "user", username: "root" });
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

  return render(<AuditPlaceholder />, { wrapper: Wrapper });
}

let originalFetch: typeof fetch | undefined;
beforeEach(() => {
  originalFetch = globalThis.fetch;
});
afterEach(() => {
  if (originalFetch) globalThis.fetch = originalFetch;
});

describe("AuditPlaceholder", () => {
  it("renders recent audit events through the SDK", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () =>
      okJson({
        events: [
          {
            id: "event-2",
            sequence: 12,
            timestamp: "2026-06-06T19:00:00Z",
            actor: { uid: 0, username: "root", delegate: null },
            workspace: {
              id: "ws-1",
              root_path: "/contracts",
              base_ref: "main",
              session_ref: "agent/review",
            },
            action: "vcs_commit",
            resource: { kind: "commit", id: "abc12345" + "0".repeat(56), path: null },
            outcome: "success",
            details: { ref: "main" },
          },
          {
            id: "event-1",
            sequence: 11,
            timestamp: "2026-06-06T18:59:00Z",
            actor: { uid: 7, username: "alice", delegate: null },
            workspace: null,
            action: "policy_decision_deny",
            resource: { kind: "file", id: null, path: "/contracts/secret.md" },
            outcome: "partial",
            details: { rule: "protected path" },
          },
        ],
      }),
    );

    renderAudit(fetchSpy);

    expect(await screen.findByRole("heading", { name: "Audit" })).toBeTruthy();
    expect(await screen.findByText("Committed")).toBeTruthy();
    expect(screen.getByText("Blocked")).toBeTruthy();
    expect(screen.getByText("root")).toBeTruthy();
    expect(screen.getByText("alice")).toBeTruthy();
    expect(screen.getByText(/abc12345/)).toBeTruthy();
    expect(screen.getByText("/contracts/secret.md")).toBeTruthy();
    expect(screen.queryByText(/phase/i)).toBeNull();
    await waitFor(() => expect(String(fetchSpy.mock.calls[0]?.[0])).toContain("/audit?limit=50"));
  });

  it("renders an admin-only error without leaking roadmap copy", async () => {
    renderAudit(vi.fn<typeof fetch>(async () => httpError(403, { error: "permission denied: audit" })));

    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toMatch(/admin user session/i);
    expect(screen.queryByText(/phase/i)).toBeNull();
  });
});
