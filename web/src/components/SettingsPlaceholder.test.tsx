import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, memoryAuthStorage } from "../lib/auth.tsx";
import {
  loadDurableCloudFixture,
  loadLocalFixture,
  type SafeCapabilities,
} from "../lib/capabilities.ts";
import { SettingsPlaceholder } from "./SettingsPlaceholder.tsx";

interface RecordedPost {
  readonly url: string;
  readonly body: unknown;
}

const recordedPosts: RecordedPost[] = [];
const recordedRequests: string[] = [];

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

function renderSettings(fetchImpl: typeof fetch) {
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

  return render(<SettingsPlaceholder />, { wrapper: Wrapper });
}

function settingsFetch(capabilities: SafeCapabilities = loadLocalFixture()): typeof fetch {
  return vi.fn<typeof fetch>(async (input, init) => {
    const url = String(typeof input === "string" || input instanceof URL ? input : input.url);
    const method = init?.method ?? (input instanceof Request ? input.method : "GET");
    recordedRequests.push(`${method} ${url}`);

    if (url.endsWith("/v1/capabilities")) {
      return okJson(capabilities);
    }

    if (method === "POST") {
      recordedPosts.push({
        url,
        body: init?.body ? JSON.parse(String(init.body)) : {},
      });
    }

    if (url.endsWith("/workspaces") && method === "GET") {
      return okJson({
        workspaces: [
          {
            id: "ws-1",
            name: "Acme review",
            root_path: "/contracts",
            head_commit: "abc12345" + "0".repeat(56),
            version: 3,
            base_ref: "main",
            session_ref: "agent/acme",
          },
        ],
      });
    }
    if (url.endsWith("/protected/refs") && method === "GET") {
      return okJson({
        rules: [
          {
            id: "ref-main",
            ref_name: "main",
            required_approvals: 2,
            require_all_files_viewed: true,
            created_by: 1,
            active: true,
          },
        ],
      });
    }
    if (url.endsWith("/protected/paths") && method === "GET") {
      return okJson({
        rules: [
          {
            id: "path-contracts",
            path_prefix: "/contracts",
            target_ref: "main",
            required_approvals: 1,
            require_all_files_viewed: false,
            created_by: 1,
            active: true,
          },
        ],
      });
    }
    if (url.endsWith("/workspaces") && method === "POST") {
      return okJson({
        id: "ws-2",
        name: "Deal room",
        root_path: "/deal-room",
        head_commit: null,
        version: 1,
        base_ref: "main",
        session_ref: null,
      });
    }
    if (url.endsWith("/workspaces/ws-1/tokens") && method === "POST") {
      return okJson({
        workspace_id: "ws-1",
        token_id: "tok-1",
        name: "Review bot",
        workspace_token: "st_ws_secret",
        agent_uid: 101,
        principal_uid: 101,
        read_prefixes: ["/contracts"],
        write_prefixes: ["/contracts/redlines"],
        base_ref: "main",
        session_ref: "agent/acme",
      });
    }
    if (url.endsWith("/protected/refs") && method === "POST") {
      return okJson({
        id: "ref-main-new",
        ref_name: "release",
        required_approvals: 3,
        require_all_files_viewed: true,
        created_by: 1,
        active: true,
      });
    }
    if (url.endsWith("/protected/paths") && method === "POST") {
      return okJson({
        id: "path-legal-new",
        path_prefix: "/legal",
        target_ref: "release",
        required_approvals: 2,
        require_all_files_viewed: false,
        created_by: 1,
        active: true,
      });
    }

    return okJson({});
  });
}

let originalFetch: typeof fetch | undefined;
beforeEach(() => {
  originalFetch = globalThis.fetch;
  recordedPosts.length = 0;
  recordedRequests.length = 0;
});
afterEach(() => {
  if (originalFetch) globalThis.fetch = originalFetch;
});

describe("SettingsPlaceholder", () => {
  it("renders live workspaces and protection rules without roadmap copy", async () => {
    renderSettings(settingsFetch());

    expect(await screen.findByRole("heading", { name: "Settings" })).toBeTruthy();
    expect(await screen.findAllByText("Acme review")).toHaveLength(2);
    expect(screen.getByText("/contracts")).toBeTruthy();
    expect(screen.getAllByText("main")).toHaveLength(2);
    expect(screen.getByText("2 approvals")).toBeTruthy();
    expect(screen.queryByText(/phase/i)).toBeNull();
  });

  it("creates a workspace through the SDK", async () => {
    renderSettings(settingsFetch());

    fireEvent.change(await screen.findByLabelText("Workspace name"), {
      target: { value: "Deal room" },
    });
    fireEvent.change(screen.getByLabelText("Workspace path"), {
      target: { value: "/deal-room" },
    });
    fireEvent.change(screen.getByLabelText("Base branch"), {
      target: { value: "main" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Create workspace" }));

    expect(await screen.findByText("Workspace ready.")).toBeTruthy();
    await waitFor(() =>
      expect(recordedPosts).toContainEqual({
        url: "http://localhost:3000/workspaces",
        body: { name: "Deal room", root_path: "/deal-room", base_ref: "main" },
      }),
    );
  });

  it("issues workspace tokens and creates branch/path rules", async () => {
    renderSettings(settingsFetch());
    await screen.findAllByText("Acme review");

    fireEvent.change(screen.getByLabelText("Token name"), {
      target: { value: "Review bot" },
    });
    fireEvent.change(screen.getByLabelText("Agent token"), {
      target: { value: "agent-secret" },
    });
    fireEvent.change(screen.getByLabelText("Read access"), {
      target: { value: "/contracts" },
    });
    fireEvent.change(screen.getByLabelText("Write access"), {
      target: { value: "/contracts/redlines" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Issue token" }));
    expect(await screen.findByText("st_ws_secret")).toBeTruthy();

    fireEvent.change(screen.getByLabelText("Branch name"), {
      target: { value: "release" },
    });
    fireEvent.change(screen.getByLabelText("Branch approvals"), {
      target: { value: "3" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Protect branch" }));
    expect(await screen.findByText("Branch protected.")).toBeTruthy();

    fireEvent.change(screen.getByLabelText("Path prefix"), {
      target: { value: "/legal" },
    });
    fireEvent.change(screen.getByLabelText("Path branch"), {
      target: { value: "release" },
    });
    fireEvent.change(screen.getByLabelText("Path approvals"), {
      target: { value: "2" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Protect path" }));
    expect(await screen.findByText("Path protected.")).toBeTruthy();

    expect(recordedPosts).toContainEqual({
      url: "http://localhost:3000/workspaces/ws-1/tokens",
      body: {
        name: "Review bot",
        agent_token: "agent-secret",
        read_prefixes: ["/contracts"],
        write_prefixes: ["/contracts/redlines"],
      },
    });
    expect(recordedPosts).toContainEqual({
      url: "http://localhost:3000/protected/refs",
      body: { ref_name: "release", required_approvals: 3, require_all_files_viewed: true },
    });
    expect(recordedPosts).toContainEqual({
      url: "http://localhost:3000/protected/paths",
      body: {
        path_prefix: "/legal",
        target_ref: "release",
        required_approvals: 2,
        require_all_files_viewed: false,
      },
    });
  });

  it("issues hosted workspace tokens with an agent ID", async () => {
    renderSettings(settingsFetch(loadDurableCloudFixture()));
    await screen.findAllByText("Acme review");

    fireEvent.change(screen.getByLabelText("Token name"), {
      target: { value: "Review bot" },
    });
    fireEvent.change(screen.getByLabelText("Agent ID"), {
      target: { value: "501" },
    });
    fireEvent.change(screen.getByLabelText("Read access"), {
      target: { value: "/contracts" },
    });
    fireEvent.change(screen.getByLabelText("Write access"), {
      target: { value: "/contracts/redlines" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Issue token" }));

    expect(await screen.findByText("st_ws_secret")).toBeTruthy();
    expect(recordedPosts).toContainEqual({
      url: "http://localhost:3000/workspaces/ws-1/tokens",
      body: {
        name: "Review bot",
        principal_uid: 501,
        read_prefixes: ["/contracts"],
        write_prefixes: ["/contracts/redlines"],
      },
    });
  });

  it("explains unsupported protection groups without calling their endpoints", async () => {
    const capabilities: SafeCapabilities = {
      ...loadLocalFixture(),
      protection: {
        ref_rules: {
          ...loadLocalFixture().protection.ref_rules,
          available: false,
        },
        path_rules: {
          ...loadLocalFixture().protection.path_rules,
          available: false,
        },
      },
    };

    renderSettings(settingsFetch(capabilities));

    expect(
      await screen.findAllByText(/branch protection is not available here/i),
    ).not.toHaveLength(0);
    expect(
      screen.getAllByText(/path protection is not available here/i),
    ).not.toHaveLength(0);
    expect(screen.queryByRole("button", { name: "Protect branch" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Protect path" })).toBeNull();

    await waitFor(() => expect(recordedRequests).toContain("GET /v1/capabilities"));
    expect(recordedRequests.some((request) => request.includes("/protected/refs"))).toBe(false);
    expect(recordedRequests.some((request) => request.includes("/protected/paths"))).toBe(false);
  });

  it("keeps server-setting failures separate from hosted-preview limits", async () => {
    const originalDev = import.meta.env.DEV;
    (import.meta.env as { DEV: boolean }).DEV = false;
    const fetchSpy = vi.fn<typeof fetch>(async (input, init) => {
      const url = String(typeof input === "string" || input instanceof URL ? input : input.url);
      const method = init?.method ?? (input instanceof Request ? input.method : "GET");
      recordedRequests.push(`${method} ${url}`);
      if (url.endsWith("/v1/capabilities")) {
        return httpError(503, { error: "settings unavailable" });
      }
      return httpError(404, { error: "unexpected route" });
    });

    try {
      renderSettings(fetchSpy);

      const alert = await screen.findByRole("alert");
      expect(alert.textContent).toMatch(/\/v1\/capabilities 503/i);
      expect(await screen.findAllByText(/settings could not be loaded/i)).not.toHaveLength(0);
      expect(screen.queryByText(/hosted preview/i)).toBeNull();
      expect(recordedRequests.some((request) => request.includes("/workspaces"))).toBe(false);
      expect(recordedRequests.some((request) => request.includes("/protected"))).toBe(false);
    } finally {
      (import.meta.env as { DEV: boolean }).DEV = originalDev;
    }
  });
});
