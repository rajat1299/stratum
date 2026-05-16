/**
 * CommandPalette — ⌘K overlay (stub).
 *
 * Fully controlled: parent owns `open` state and the item list, palette
 * does the rendering + filtering + keyboard nav. Decoupled from the
 * router so it's testable in isolation.
 *
 *   Open      ⌘K / Ctrl-K   (registered via usePaletteShortcut)
 *   Close     Esc, click outside, item selected
 *   Filter    Case-insensitive substring match on label + description
 *   Move      ↑ / ↓, wraps at top and bottom
 *   Activate  Enter on the highlighted item
 *
 * A11y:
 *   - role="dialog" + aria-modal on the surface
 *   - role="listbox" on the items list, role="option" + aria-selected on each
 *   - input has aria-controls + aria-activedescendant pointing at the
 *     highlighted option so VoiceOver/NVDA announce the current selection
 *     without focus moving away from the input.
 *
 * This slice ships a static item list (navigation only). Phase D wires
 * dynamic items (open change-request id, jump to file, etc.).
 */

import { useEffect, useId, useMemo, useRef, useState } from "react";

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

export interface CommandItem {
  readonly id: string;
  readonly label: string;
  /** Optional secondary line — mono, dimmed. Folded into the search corpus. */
  readonly description?: string;
  /** Shortcut hint rendered on the right, e.g. "G R". Purely visual. */
  readonly shortcut?: string;
  /** Invoked on Enter / click. Palette closes after run(). */
  readonly run: () => void;
}

export interface CommandPaletteProps {
  readonly open: boolean;
  readonly onClose: () => void;
  readonly items: readonly CommandItem[];
  /** Placeholder text in the search input. */
  readonly placeholder?: string;
}

// ─────────────────────────────────────────────────────────────────────────────
// Hook — register ⌘K / Ctrl-K to open
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Bind ⌘K (Cmd+K on macOS / Ctrl+K elsewhere) to the given opener. Pass a
 * stable callback (e.g. wrapped in useCallback) so the listener doesn't
 * thrash on every render.
 */
export function usePaletteShortcut(open: () => void): void {
  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (e.key.toLowerCase() === "k" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        open();
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [open]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Component
// ─────────────────────────────────────────────────────────────────────────────

export function CommandPalette({ open, onClose, items, placeholder = "Type a command…" }: CommandPaletteProps) {
  const listboxId = useId();
  const inputRef = useRef<HTMLInputElement>(null);
  const [query, setQuery] = useState("");
  const [cursor, setCursor] = useState(0);

  const filtered = useMemo(() => filterItems(items, query), [items, query]);

  // Clamp cursor whenever the visible list shrinks.
  useEffect(() => {
    if (cursor >= filtered.length) setCursor(Math.max(0, filtered.length - 1));
  }, [filtered.length, cursor]);

  // Reset on open + focus the input.
  useEffect(() => {
    if (!open) return;
    setQuery("");
    setCursor(0);
    // Focus on the next tick so React has mounted the input.
    const id = window.setTimeout(() => inputRef.current?.focus(), 0);
    return () => window.clearTimeout(id);
  }, [open]);

  if (!open) return null;

  const activeId = filtered[cursor] ? `${listboxId}-${filtered[cursor].id}` : undefined;

  function onKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Escape") {
      e.preventDefault();
      onClose();
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      if (filtered.length > 0) setCursor((c) => (c + 1) % filtered.length);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      if (filtered.length > 0) setCursor((c) => (c - 1 + filtered.length) % filtered.length);
    } else if (e.key === "Enter") {
      e.preventDefault();
      const item = filtered[cursor];
      if (item) {
        item.run();
        onClose();
      }
    }
  }

  return (
    <div
      role="presentation"
      onMouseDown={(e) => {
        // Click on the backdrop closes; clicks inside the surface bubble up
        // but stopPropagation on the surface keeps them from reaching here.
        if (e.target === e.currentTarget) onClose();
      }}
      className="fixed inset-0 z-50 grid place-items-start justify-center bg-stone-950/30 px-4 pt-[15vh] backdrop-blur-sm"
    >
      <div
        role="dialog"
        aria-modal
        aria-label="Command palette"
        onMouseDown={(e) => e.stopPropagation()}
        className="w-full max-w-lg overflow-hidden rounded-xl border border-stone-200 bg-white shadow-2xl"
      >
        <div className="flex items-center gap-2 border-b border-stone-200 px-3 py-2.5">
          <span aria-hidden className="text-stone-400">
            <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
              <circle cx="7" cy="7" r="4.5" stroke="currentColor" strokeWidth="1.5" />
              <path d="M10.5 10.5 13.5 13.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
            </svg>
          </span>
          <input
            ref={inputRef}
            type="text"
            placeholder={placeholder}
            value={query}
            onChange={(e) => {
              setQuery(e.currentTarget.value);
              setCursor(0);
            }}
            onKeyDown={onKeyDown}
            role="combobox"
            aria-expanded
            aria-controls={listboxId}
            {...(activeId ? { "aria-activedescendant": activeId } : {})}
            className="w-full bg-transparent text-[14px] text-stone-900 placeholder:text-stone-400 focus:outline-none"
          />
          <kbd className="rounded border border-stone-300 px-1 font-mono text-[10px] text-stone-500">esc</kbd>
        </div>

        <ul
          id={listboxId}
          role="listbox"
          aria-label="Commands"
          className="max-h-[50vh] overflow-y-auto py-1"
        >
          {filtered.length === 0 ? (
            <li className="px-3 py-6 text-center font-serif text-[13px] italic text-stone-500">
              No matches.
            </li>
          ) : (
            filtered.map((item, i) => {
              const selected = i === cursor;
              return (
                <li
                  key={item.id}
                  id={`${listboxId}-${item.id}`}
                  role="option"
                  aria-selected={selected}
                  onMouseEnter={() => setCursor(i)}
                  onClick={() => {
                    item.run();
                    onClose();
                  }}
                  className={`flex cursor-pointer items-center gap-3 px-3 py-2 ${
                    selected ? "bg-orange-50 text-orange-900" : "text-stone-800 hover:bg-stone-50"
                  }`}
                >
                  <span className="flex-1 truncate text-[13.5px]">{item.label}</span>
                  {item.description && (
                    <span className="truncate font-mono text-[11px] text-stone-500">{item.description}</span>
                  )}
                  {item.shortcut && (
                    <kbd className="rounded border border-stone-300 px-1 font-mono text-[10px] text-stone-500">
                      {item.shortcut}
                    </kbd>
                  )}
                </li>
              );
            })
          )}
        </ul>

        <div className="flex items-center justify-end gap-2 border-t border-stone-200 bg-stone-50 px-3 py-1.5 font-mono text-[10px] text-stone-500">
          <span>
            <kbd className="rounded border border-stone-300 px-1">↑</kbd>{" "}
            <kbd className="rounded border border-stone-300 px-1">↓</kbd> move
          </span>
          <span>
            <kbd className="rounded border border-stone-300 px-1">↵</kbd> select
          </span>
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Filter
// ─────────────────────────────────────────────────────────────────────────────

function filterItems(items: readonly CommandItem[], query: string): readonly CommandItem[] {
  const q = query.trim().toLowerCase();
  if (q.length === 0) return items;
  return items.filter((item) => {
    const hay = `${item.label} ${item.description ?? ""}`.toLowerCase();
    return hay.includes(q);
  });
}
