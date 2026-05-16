import { fireEvent, render, screen, within } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { AuthProvider, memoryAuthStorage } from "../lib/auth.tsx";
import { AppShell, type NavItem } from "./AppShell.tsx";

const NAV: NavItem[] = [
  { to: "/reviews", label: "Reviews", icon: <span data-testid="icon-reviews">i</span>, badge: 3 },
  { to: "/repository", label: "Repository", icon: <span data-testid="icon-repo">i</span> },
  { to: "/audit", label: "Audit", icon: <span data-testid="icon-audit">i</span> },
  { to: "/settings", label: "Settings", icon: <span data-testid="icon-settings">i</span> },
];

function renderShell(props: {
  readonly pathname?: string;
  readonly onOpenPalette?: () => void;
  readonly navigate?: (to: string) => void;
}) {
  const storage = memoryAuthStorage({ type: "user", username: "alice" });
  function Wrapper({ children }: { children: ReactNode }) {
    return <AuthProvider storage={storage}>{children}</AuthProvider>;
  }
  return render(
    <AppShell
      nav={NAV}
      pathname={props.pathname ?? "/reviews"}
      navigate={props.navigate ?? (() => undefined)}
      {...(props.onOpenPalette ? { onOpenPalette: props.onOpenPalette } : {})}
    >
      <div data-testid="main">main content</div>
    </AppShell>,
    { wrapper: Wrapper },
  );
}

describe("AppShell — chrome", () => {
  it("renders the brand mark, primary nav, main content, and user menu", () => {
    renderShell({});
    expect(screen.getByText("stratum")).toBeTruthy();
    expect(screen.getByRole("navigation", { name: "Primary" })).toBeTruthy();
    expect(screen.getByTestId("main")).toBeTruthy();
    expect(screen.getByRole("button", { name: /open command palette/i })).toBeTruthy();
  });

  it("renders every nav item with its label", () => {
    renderShell({});
    for (const item of NAV) {
      expect(screen.getByRole("button", { name: new RegExp(item.label, "i") })).toBeTruthy();
    }
  });

  it("renders the badge for items with a count", () => {
    renderShell({});
    expect(screen.getByText("3")).toBeTruthy();
  });

  it("renders the breadcrumb for the active route", () => {
    renderShell({ pathname: "/audit" });
    expect(screen.getByRole("navigation", { name: "Breadcrumb" }).textContent).toContain("Audit");
  });
});

describe("AppShell — active highlight", () => {
  it("marks the active nav item via aria-current", () => {
    renderShell({ pathname: "/reviews" });
    const active = screen.getByRole("button", { name: /reviews/i });
    expect(active.getAttribute("aria-current")).toBe("page");
    const inactive = screen.getByRole("button", { name: /audit/i });
    expect(inactive.getAttribute("aria-current")).toBeNull();
  });

  it("treats nested routes (/reviews/cr-42) as active for the /reviews item", () => {
    renderShell({ pathname: "/reviews/cr-42" });
    const active = screen.getByRole("button", { name: /reviews/i });
    expect(active.getAttribute("aria-current")).toBe("page");
  });
});

describe("AppShell — interactions", () => {
  it("calls navigate when a nav item is clicked", () => {
    const navigate = vi.fn();
    renderShell({ pathname: "/reviews", navigate });
    fireEvent.click(screen.getByRole("button", { name: /audit/i }));
    expect(navigate).toHaveBeenCalledWith("/audit");
  });

  it("calls onOpenPalette when the search button is clicked", () => {
    const onOpenPalette = vi.fn();
    renderShell({ onOpenPalette });
    fireEvent.click(screen.getByRole("button", { name: /open command palette/i }));
    expect(onOpenPalette).toHaveBeenCalledTimes(1);
  });

  it("disables the search button when no palette handler is wired", () => {
    renderShell({});
    const btn = screen.getByRole("button", { name: /open command palette/i }) as HTMLButtonElement;
    expect(btn.disabled).toBe(true);
  });
});

describe("AppShell — user menu", () => {
  it("shows the username in the menu after opening", () => {
    renderShell({});
    const triggers = screen.getAllByRole("button");
    const userTrigger = triggers.find((b) => b.getAttribute("aria-haspopup") === "menu");
    if (!userTrigger) throw new Error("expected a menu trigger");
    fireEvent.click(userTrigger);
    const menu = screen.getByRole("menu");
    expect(within(menu).getByText("alice")).toBeTruthy();
    expect(within(menu).getByRole("menuitem", { name: /sign out/i })).toBeTruthy();
  });

  it("sign out clears the session", () => {
    const storage = memoryAuthStorage({ type: "user", username: "alice" });
    function Wrapper({ children }: { children: ReactNode }) {
      return <AuthProvider storage={storage}>{children}</AuthProvider>;
    }
    render(
      <AppShell nav={NAV} pathname="/reviews" navigate={() => undefined}>
        <div>main</div>
      </AppShell>,
      { wrapper: Wrapper },
    );
    const triggers = screen.getAllByRole("button");
    const userTrigger = triggers.find((b) => b.getAttribute("aria-haspopup") === "menu");
    if (!userTrigger) throw new Error("expected a menu trigger");
    fireEvent.click(userTrigger);
    fireEvent.click(screen.getByRole("menuitem", { name: /sign out/i }));
    expect(storage.read()).toBeNull();
  });
});
