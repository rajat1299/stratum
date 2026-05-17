import { QueryClient, useQuery } from "@tanstack/react-query";
import { act, render, renderHook, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { AuthProvider, memoryAuthStorage, useAuth } from "./auth.tsx";
import { isTerminalHttpError, makeQueryClient, QueryProvider } from "./query.tsx";

function wrap(storage = memoryAuthStorage(), client?: QueryClient) {
  return function W({ children }: { children: ReactNode }) {
    return (
      <AuthProvider storage={storage}>
        <QueryProvider {...(client ? { client } : {})}>{children}</QueryProvider>
      </AuthProvider>
    );
  };
}

describe("makeQueryClient — defaults", () => {
  it("sets a 60s staleTime", () => {
    const c = makeQueryClient();
    expect(c.getDefaultOptions().queries?.staleTime).toBe(60_000);
  });

  it("disables mutation retries (caller drives explicit retry)", () => {
    const c = makeQueryClient();
    expect(c.getDefaultOptions().mutations?.retry).toBe(0);
  });

  it("retries up to twice on transient errors", () => {
    const c = makeQueryClient();
    const retry = c.getDefaultOptions().queries?.retry;
    if (typeof retry !== "function") throw new Error("expected a function retry policy");
    expect(retry(0, new Error("boom"))).toBe(true);
    expect(retry(1, new Error("boom"))).toBe(true);
    expect(retry(2, new Error("boom"))).toBe(false);
  });

  it("never retries 4xx errors", () => {
    const c = makeQueryClient();
    const retry = c.getDefaultOptions().queries?.retry;
    if (typeof retry !== "function") throw new Error("expected a function retry policy");
    // The retry callback's `error` is typed as Error; in practice the SDK
    // throws StratumHttpError with a `status` field. We synthesize that
    // shape here.
    const err = (status: number): Error => Object.assign(new Error(`HTTP ${status}`), { status });
    expect(retry(0, err(403))).toBe(false);
    expect(retry(0, err(404))).toBe(false);
    expect(retry(0, err(409))).toBe(false);
  });
});

describe("isTerminalHttpError", () => {
  // Synthesize an HTTP-shaped error the way the SDK does in practice.
  const httpErr = (status: number): Error =>
    Object.assign(new Error(`HTTP ${status}`), { status });

  it("identifies 4xx as terminal", () => {
    expect(isTerminalHttpError(httpErr(400))).toBe(true);
    expect(isTerminalHttpError(httpErr(403))).toBe(true);
    expect(isTerminalHttpError(httpErr(499))).toBe(true);
  });

  it("does not treat 5xx as terminal", () => {
    expect(isTerminalHttpError(httpErr(500))).toBe(false);
    expect(isTerminalHttpError(httpErr(503))).toBe(false);
  });

  it("does not treat non-status errors as terminal", () => {
    expect(isTerminalHttpError(new Error("network down"))).toBe(false);
    expect(isTerminalHttpError(null)).toBe(false);
    expect(isTerminalHttpError("boom")).toBe(false);
  });
});

describe("QueryProvider — cache hygiene on sign-out", () => {
  it("clears the cache when auth flips to anon", async () => {
    const storage = memoryAuthStorage({ type: "user", username: "alice" });
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });

    // Seed a query so the cache has something to clear.
    function Inner() {
      useQuery({ queryKey: ["seed"], queryFn: () => "value" });
      return null;
    }
    render(<Inner />, { wrapper: wrap(storage, client) });
    await waitFor(() => expect(client.getQueryData(["seed"])).toBe("value"));

    // Now sign out via the auth API and assert the cache is empty.
    const { result } = renderHook(() => useAuth(), { wrapper: wrap(storage, client) });
    await waitFor(() => expect(result.current.state.status).toBe("authed"));
    act(() => result.current.signOut());
    await waitFor(() => expect(client.getQueryData(["seed"])).toBeUndefined());
  });

  it("does not clear on hydrate-to-authed (initial mount with a session)", async () => {
    const storage = memoryAuthStorage({ type: "user", username: "alice" });
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    client.setQueryData(["pre-existing"], 42);

    function Inner() {
      // Just touch useQuery so the provider wires up.
      useQuery({ queryKey: ["x"], queryFn: () => "ok" });
      return null;
    }
    render(<Inner />, { wrapper: wrap(storage, client) });
    // Wait a beat to let hydration effects flush.
    await waitFor(() => expect(client.getQueryData(["pre-existing"])).toBe(42));
  });
});

describe("QueryProvider — basic render", () => {
  it("provides a QueryClient to descendants", async () => {
    function Probe() {
      const q = useQuery({ queryKey: ["probe"], queryFn: () => "hi" });
      return <span>{q.data ?? "…"}</span>;
    }
    render(<Probe />, { wrapper: wrap() });
    await waitFor(() => expect(screen.getByText("hi")).toBeTruthy());
  });

  it("re-uses the same client across re-renders (referentially stable)", () => {
    const client = makeQueryClient();
    const spy = vi.spyOn(client, "clear");
    const wrapper = wrap(memoryAuthStorage(), client);
    const { rerender } = render(<div />, { wrapper });
    rerender(<div />);
    rerender(<div />);
    expect(spy).not.toHaveBeenCalled();
  });
});
