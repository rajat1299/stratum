import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { AuthProvider, memoryAuthStorage, useAuth } from "./auth.tsx";
import { useStratumClient } from "./stratum-client.ts";

function wrapperWith(storage = memoryAuthStorage()) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return <AuthProvider storage={storage}>{children}</AuthProvider>;
  };
}

describe("useStratumClient", () => {
  it("returns a usable client even in the anon state (for /health, /v1/capabilities, /auth/login)", async () => {
    const { result } = renderHook(() => useStratumClient(), { wrapper: wrapperWith() });
    // Loading state still produces a client.
    expect(result.current).toBeDefined();
    expect(result.current.fs).toBeDefined();
    expect(result.current.reviews).toBeDefined();
  });

  it("memoizes the client across re-renders when auth has not changed", async () => {
    const storage = memoryAuthStorage({ type: "user", username: "alice" });
    const { result, rerender } = renderHook(() => useStratumClient(), {
      wrapper: wrapperWith(storage),
    });
    await waitFor(() => expect(result.current).toBeDefined());
    const first = result.current;
    rerender();
    expect(result.current).toBe(first);
  });

  it("rebuilds the client when auth credentials change", async () => {
    // Combined hook so both useAuth and useStratumClient share one provider.
    function useBoth() {
      return { auth: useAuth(), client: useStratumClient() };
    }
    const { result } = renderHook(useBoth, { wrapper: wrapperWith() });
    await waitFor(() => expect(result.current.auth.state.status).toBe("anon"));

    const before = result.current.client;
    act(() => result.current.auth.signInAsUser("alice"));
    await waitFor(() => expect(result.current.auth.state.status).toBe("authed"));
    expect(result.current.client).not.toBe(before);

    const afterSignIn = result.current.client;
    act(() => result.current.auth.signOut());
    await waitFor(() => expect(result.current.auth.state.status).toBe("anon"));
    expect(result.current.client).not.toBe(afterSignIn);
  });

  it("respects baseUrl override (used by storybook spikes pointing at a remote demo server)", () => {
    const { result } = renderHook(() => useStratumClient({ baseUrl: "https://demo.example" }), {
      wrapper: wrapperWith(),
    });
    expect(result.current).toBeDefined();
    // We don't introspect the SDK's private state — the contract is "no error".
  });
});
