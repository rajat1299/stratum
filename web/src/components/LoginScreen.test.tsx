import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { AuthProvider, memoryAuthStorage } from "../lib/auth.tsx";
import { LoginScreen } from "./LoginScreen.tsx";

function renderLogin(storage = memoryAuthStorage()) {
  function Wrapper({ children }: { children: ReactNode }) {
    return <AuthProvider storage={storage}>{children}</AuthProvider>;
  }
  return { storage, ...render(<LoginScreen />, { wrapper: Wrapper }) };
}

describe("LoginScreen — chrome", () => {
  it("renders the title and both visible tabs", async () => {
    renderLogin();
    expect(await screen.findByRole("heading", { name: /sign in to your workspace/i })).toBeTruthy();
    expect(screen.getByRole("tab", { name: "User" })).toBeTruthy();
    expect(screen.getByRole("tab", { name: "Bearer token" })).toBeTruthy();
  });

  it("defaults to the user tab", async () => {
    renderLogin();
    expect(await screen.findByLabelText("Username")).toBeTruthy();
    expect(screen.queryByLabelText("Agent token")).toBeNull();
  });

  it("switching to the bearer tab swaps the visible field", async () => {
    renderLogin();
    fireEvent.click(screen.getByRole("tab", { name: "Bearer token" }));
    expect(await screen.findByLabelText("Agent token")).toBeTruthy();
    expect(screen.queryByLabelText("Username")).toBeNull();
  });
});

describe("LoginScreen — submit gating", () => {
  it("disables Continue when the field is empty", async () => {
    renderLogin();
    const btn = await screen.findByRole("button", { name: "Continue" });
    expect((btn as HTMLButtonElement).disabled).toBe(true);
  });

  it("enables Continue once the user types", async () => {
    renderLogin();
    const input = (await screen.findByLabelText("Username")) as HTMLInputElement;
    fireEvent.change(input, { target: { value: "alice" } });
    const btn = await screen.findByRole("button", { name: "Continue" });
    expect((btn as HTMLButtonElement).disabled).toBe(false);
  });

  it("workspace mode requires both id and token", async () => {
    renderLogin();
    fireEvent.click(screen.getByRole("button", { name: /sign in with a workspace token/i }));
    const id = (await screen.findByLabelText("Workspace id")) as HTMLInputElement;
    fireEvent.change(id, { target: { value: "ws-1" } });
    const btn = await screen.findByRole("button", { name: "Continue" });
    expect((btn as HTMLButtonElement).disabled).toBe(true);
    fireEvent.change(screen.getByLabelText("Workspace token"), { target: { value: "tok" } });
    expect((btn as HTMLButtonElement).disabled).toBe(false);
  });
});

describe("LoginScreen — submit flows", () => {
  it("submitting the user form persists a user credential", async () => {
    const { storage } = renderLogin();
    fireEvent.change(await screen.findByLabelText("Username"), { target: { value: "alice" } });
    fireEvent.click(screen.getByRole("button", { name: "Continue" }));
    await waitFor(() => {
      expect(storage.read()).toEqual({ type: "user", username: "alice" });
    });
  });

  it("submitting the bearer form persists a bearer credential", async () => {
    const { storage } = renderLogin();
    fireEvent.click(screen.getByRole("tab", { name: "Bearer token" }));
    fireEvent.change(await screen.findByLabelText("Agent token"), { target: { value: "sk_strat_xyz" } });
    fireEvent.click(screen.getByRole("button", { name: "Continue" }));
    await waitFor(() => {
      expect(storage.read()).toEqual({ type: "bearer", token: "sk_strat_xyz" });
    });
  });

  it("submitting the workspace form persists a workspace credential", async () => {
    const { storage } = renderLogin();
    fireEvent.click(screen.getByRole("button", { name: /sign in with a workspace token/i }));
    fireEvent.change(await screen.findByLabelText("Workspace id"), { target: { value: "ws-1" } });
    fireEvent.change(screen.getByLabelText("Workspace token"), { target: { value: "tok" } });
    fireEvent.click(screen.getByRole("button", { name: "Continue" }));
    await waitFor(() => {
      expect(storage.read()).toEqual({
        type: "workspace",
        workspaceId: "ws-1",
        workspaceToken: "tok",
      });
    });
  });
});
