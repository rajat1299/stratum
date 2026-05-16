import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { AuthProvider, memoryAuthStorage, parseStoredAuth, useAuth } from "./auth.tsx";

function wrapperWith(storage = memoryAuthStorage()) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return <AuthProvider storage={storage}>{children}</AuthProvider>;
  };
}

describe("AuthProvider — hydration", () => {
  it("starts in loading and resolves to anon when storage is empty", async () => {
    const { result } = renderHook(() => useAuth(), { wrapper: wrapperWith() });
    expect(result.current.state.status).toBe("loading");
    await waitFor(() => expect(result.current.state.status).toBe("anon"));
  });

  it("resolves to authed when storage already holds a valid session", async () => {
    const storage = memoryAuthStorage({ type: "user", username: "alice" });
    const { result } = renderHook(() => useAuth(), { wrapper: wrapperWith(storage) });
    await waitFor(() => expect(result.current.state.status).toBe("authed"));
    if (result.current.state.status !== "authed") throw new Error("unreachable");
    expect(result.current.state.credentials).toEqual({ type: "user", username: "alice" });
  });
});

describe("AuthProvider — sign-in flows", () => {
  it("signInAsUser persists and flips to authed", async () => {
    const storage = memoryAuthStorage();
    const { result } = renderHook(() => useAuth(), { wrapper: wrapperWith(storage) });
    await waitFor(() => expect(result.current.state.status).toBe("anon"));
    act(() => result.current.signInAsUser("  alice  "));
    expect(result.current.state.status).toBe("authed");
    expect(storage.read()).toEqual({ type: "user", username: "alice" }); // trimmed
  });

  it("signInWithBearer accepts a token", async () => {
    const storage = memoryAuthStorage();
    const { result } = renderHook(() => useAuth(), { wrapper: wrapperWith(storage) });
    await waitFor(() => expect(result.current.state.status).toBe("anon"));
    act(() => result.current.signInWithBearer("sk_strat_abc"));
    if (result.current.state.status !== "authed") throw new Error("unreachable");
    expect(result.current.state.credentials).toEqual({ type: "bearer", token: "sk_strat_abc" });
  });

  it("signInWithWorkspace requires both id and token", async () => {
    const storage = memoryAuthStorage();
    const { result } = renderHook(() => useAuth(), { wrapper: wrapperWith(storage) });
    await waitFor(() => expect(result.current.state.status).toBe("anon"));
    act(() => result.current.signInWithWorkspace("ws-1", ""));
    expect(result.current.state.status).toBe("anon");
    act(() => result.current.signInWithWorkspace("ws-1", "secret"));
    expect(result.current.state.status).toBe("authed");
  });

  it("signOut clears storage and flips to anon", async () => {
    const storage = memoryAuthStorage({ type: "user", username: "alice" });
    const { result } = renderHook(() => useAuth(), { wrapper: wrapperWith(storage) });
    await waitFor(() => expect(result.current.state.status).toBe("authed"));
    act(() => result.current.signOut());
    expect(result.current.state.status).toBe("anon");
    expect(storage.read()).toBeNull();
  });

  it("rejects empty / whitespace usernames silently", async () => {
    const storage = memoryAuthStorage();
    const { result } = renderHook(() => useAuth(), { wrapper: wrapperWith(storage) });
    await waitFor(() => expect(result.current.state.status).toBe("anon"));
    act(() => result.current.signInAsUser("   "));
    expect(result.current.state.status).toBe("anon");
  });
});

describe("useAuth — error handling", () => {
  it("throws a developer-facing error when used outside the provider", () => {
    expect(() => renderHook(() => useAuth())).toThrowError(/AuthProvider/);
  });
});

describe("parseStoredAuth — defends against tampered storage", () => {
  it("accepts valid user shape", () => {
    expect(parseStoredAuth({ type: "user", username: "alice" })).toEqual({
      type: "user",
      username: "alice",
    });
  });

  it("accepts valid bearer shape", () => {
    expect(parseStoredAuth({ type: "bearer", token: "sk" })).toEqual({
      type: "bearer",
      token: "sk",
    });
  });

  it("accepts valid workspace shape", () => {
    expect(
      parseStoredAuth({ type: "workspace", workspaceId: "ws-1", workspaceToken: "tok" }),
    ).toEqual({ type: "workspace", workspaceId: "ws-1", workspaceToken: "tok" });
  });

  it("rejects unknown type", () => {
    expect(parseStoredAuth({ type: "oidc", username: "alice" })).toBeNull();
  });

  it("rejects missing fields", () => {
    expect(parseStoredAuth({ type: "user" })).toBeNull();
    expect(parseStoredAuth({ type: "bearer" })).toBeNull();
    expect(parseStoredAuth({ type: "workspace", workspaceId: "ws-1" })).toBeNull();
  });

  it("rejects empty fields (treats them as missing)", () => {
    expect(parseStoredAuth({ type: "user", username: "" })).toBeNull();
    expect(parseStoredAuth({ type: "bearer", token: "" })).toBeNull();
  });

  it("rejects scalars and arrays", () => {
    expect(parseStoredAuth(null)).toBeNull();
    expect(parseStoredAuth("alice")).toBeNull();
    expect(parseStoredAuth(42)).toBeNull();
    expect(parseStoredAuth([])).toBeNull();
  });
});
