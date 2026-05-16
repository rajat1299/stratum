import { fireEvent, render, renderHook, screen } from "@testing-library/react";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { CommandPalette, type CommandItem, usePaletteShortcut } from "./CommandPalette.tsx";

const ITEMS: CommandItem[] = [
  { id: "reviews", label: "Go to Reviews", description: "/reviews", shortcut: "G R", run: vi.fn() },
  { id: "repo", label: "Go to Repository", description: "/repository", shortcut: "G P", run: vi.fn() },
  { id: "audit", label: "Go to Audit", description: "/audit", run: vi.fn() },
  { id: "settings", label: "Go to Settings", description: "/settings", run: vi.fn() },
  { id: "signout", label: "Sign out", run: vi.fn() },
];

function freshItems(): CommandItem[] {
  return ITEMS.map((item) => ({ ...item, run: vi.fn() }));
}

function renderOpen(items: readonly CommandItem[] = ITEMS, onClose = vi.fn()) {
  return { onClose, ...render(<CommandPalette open onClose={onClose} items={items} />) };
}

describe("CommandPalette — render gating", () => {
  it("renders nothing when closed", () => {
    const { container } = render(<CommandPalette open={false} onClose={vi.fn()} items={ITEMS} />);
    expect(container.querySelector("[role='dialog']")).toBeNull();
  });

  it("renders a dialog + listbox when open", () => {
    renderOpen();
    expect(screen.getByRole("dialog", { name: /command palette/i })).toBeTruthy();
    expect(screen.getByRole("listbox", { name: /commands/i })).toBeTruthy();
  });

  it("renders every item with its label by default", () => {
    renderOpen();
    for (const item of ITEMS) {
      expect(screen.getByRole("option", { name: new RegExp(item.label, "i") })).toBeTruthy();
    }
  });
});

describe("CommandPalette — filtering", () => {
  it("filters items by case-insensitive substring on label", () => {
    renderOpen();
    const input = screen.getByRole("combobox");
    fireEvent.change(input, { target: { value: "REPO" } });
    expect(screen.queryByRole("option", { name: /Go to Reviews/i })).toBeNull();
    expect(screen.getByRole("option", { name: /Go to Repository/i })).toBeTruthy();
  });

  it("matches against description text too", () => {
    renderOpen();
    const input = screen.getByRole("combobox");
    fireEvent.change(input, { target: { value: "/audit" } });
    expect(screen.getAllByRole("option")).toHaveLength(1);
    expect(screen.getByRole("option", { name: /Go to Audit/i })).toBeTruthy();
  });

  it("shows the no-matches state when nothing hits", () => {
    renderOpen();
    fireEvent.change(screen.getByRole("combobox"), { target: { value: "xyzzy" } });
    expect(screen.queryByRole("option")).toBeNull();
    expect(screen.getByText(/no matches/i)).toBeTruthy();
  });
});

describe("CommandPalette — keyboard nav", () => {
  it("selects the first item by default and exposes it as aria-activedescendant", () => {
    renderOpen();
    const input = screen.getByRole("combobox");
    expect(input.getAttribute("aria-activedescendant")).toContain("reviews");
  });

  it("ArrowDown moves the highlight and wraps", () => {
    renderOpen();
    const input = screen.getByRole("combobox");
    for (let i = 0; i < ITEMS.length; i++) fireEvent.keyDown(input, { key: "ArrowDown" });
    // After length presses we should be back on the first item.
    expect(input.getAttribute("aria-activedescendant")).toContain("reviews");
  });

  it("ArrowUp from the first item wraps to the last", () => {
    renderOpen();
    const input = screen.getByRole("combobox");
    fireEvent.keyDown(input, { key: "ArrowUp" });
    expect(input.getAttribute("aria-activedescendant")).toContain("signout");
  });

  it("Enter runs the highlighted item and closes", () => {
    const items = freshItems();
    const onClose = vi.fn();
    renderOpen(items, onClose);
    const input = screen.getByRole("combobox");
    fireEvent.keyDown(input, { key: "ArrowDown" }); // highlight repo
    fireEvent.keyDown(input, { key: "Enter" });
    expect(items[1]!.run).toHaveBeenCalledTimes(1);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("Escape closes without running", () => {
    const items = freshItems();
    const onClose = vi.fn();
    renderOpen(items, onClose);
    fireEvent.keyDown(screen.getByRole("combobox"), { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);
    for (const item of items) expect(item.run).not.toHaveBeenCalled();
  });
});

describe("CommandPalette — mouse", () => {
  it("clicking an item runs it and closes", () => {
    const items = freshItems();
    const onClose = vi.fn();
    renderOpen(items, onClose);
    fireEvent.click(screen.getByRole("option", { name: /sign out/i }));
    expect(items[4]!.run).toHaveBeenCalledTimes(1);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("clicking the backdrop closes; clicking inside the surface does not", () => {
    const onClose = vi.fn();
    renderOpen(ITEMS, onClose);
    const dialog = screen.getByRole("dialog");
    fireEvent.mouseDown(dialog);
    expect(onClose).not.toHaveBeenCalled();
    // The presentation element is the dialog's parent (the backdrop).
    const backdrop = dialog.parentElement;
    if (!backdrop) throw new Error("expected a backdrop");
    fireEvent.mouseDown(backdrop, { target: backdrop, currentTarget: backdrop });
    expect(onClose).toHaveBeenCalled();
  });
});

describe("usePaletteShortcut", () => {
  it("fires open() on ⌘K (metaKey)", () => {
    const open = vi.fn();
    renderHook(() => usePaletteShortcut(open));
    fireEvent.keyDown(window, { key: "k", metaKey: true });
    expect(open).toHaveBeenCalledTimes(1);
  });

  it("fires open() on Ctrl+K (ctrlKey)", () => {
    const open = vi.fn();
    renderHook(() => usePaletteShortcut(open));
    fireEvent.keyDown(window, { key: "k", ctrlKey: true });
    expect(open).toHaveBeenCalledTimes(1);
  });

  it("ignores plain k without modifier", () => {
    const open = vi.fn();
    renderHook(() => usePaletteShortcut(open));
    fireEvent.keyDown(window, { key: "k" });
    expect(open).not.toHaveBeenCalled();
  });

  it("toggles via state when wired round-trip", () => {
    function Host() {
      const [open, setOpen] = useState(false);
      usePaletteShortcut(() => setOpen(true));
      return open ? <span>opened</span> : <span>closed</span>;
    }
    render(<Host />);
    expect(screen.getByText("closed")).toBeTruthy();
    fireEvent.keyDown(window, { key: "k", metaKey: true });
    expect(screen.getByText("opened")).toBeTruthy();
  });
});
