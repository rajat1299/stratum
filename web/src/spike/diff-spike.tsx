/**
 * Diff-view spike — week-2 deliverable.
 *
 * Demonstrates: the diff parser handles every shape `src/vcs/diff.rs` can
 * emit, and the renderer makes each one legible without falling through to
 * a `<pre>`. The fixtures here are copied verbatim from the Rust test bodies
 * plus the durable structured-summary code path so this spike is a real
 * round-trip against the production text format.
 *
 * What this spike is for:
 *   - Showing a real reviewer that the diff feels good before we cut to a
 *     full app shell.
 *   - Catching parser regressions whenever the Rust text format changes
 *     (the snapshot tests in diff-parser.test.ts do that part).
 *   - Being the smallest piece of UI we could throw away if we needed to —
 *     keeping the parser and rebuilding the renderer against the eventual
 *     design tokens.
 *
 * What this spike is NOT:
 *   - The final design. Tailwind utility classes here will be replaced by
 *     shadcn primitives + design tokens in Phase A5.
 *   - Wired to a real server. Phase B4 ties it to `GET /vcs/diff`.
 */

import { useMemo, useState } from "react";
import { parseDiff } from "../lib/diff-parser.ts";
import { DiffViewer } from "../components/DiffViewer.tsx";

// ─────────────────────────────────────────────────────────────────────────────
// Fixtures — verbatim from `src/vcs/diff.rs` test bodies + durable code path
// ─────────────────────────────────────────────────────────────────────────────

const FIXTURES: Record<string, { label: string; subtitle: string; body: string }> = {
  multi: {
    label: "Real change request",
    subtitle: "Three files, three fragment kinds — the daily-driver shape.",
    body: [
      "diff -- /contracts/loi-acme-q2.docx",
      "reason: binary or non-UTF-8 content is not supported by text diff",
      "before: object=a4f9c1b2 size=8421 type=file mime=application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      "after: object=e8b2d017 size=8442 type=file mime=application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      "diff -- /runbooks/redline-policy.md",
      "--- a/runbooks/redline-policy.md",
      "+++ b/runbooks/redline-policy.md",
      "@@ -42,7 +42,9 @@",
      " ### §4.b — Indemnification carve-out",
      " ",
      " Counterparty drafts default to mutual indemnity. Narrow to:",
      "-1. Gross negligence",
      "-2. Willful misconduct",
      "+1. Gross negligence",
      "+2. Willful misconduct",
      "+3. Final, non-appealable judgment by a court of competent jurisdiction",
      " ",
      " See `/case-files/precedent-2025/` for two precedent matches.",
      "diff -- /memory/agents/redline.md",
      "metadata:",
      "- mode: 0600",
      "+ mode: 0640",
      "- custom_attrs.last_loaded: 2026-05-12T18:00:00Z",
      "+ custom_attrs.last_loaded: 2026-05-13T14:21:08Z",
      "",
    ].join("\n"),
  },

  grouped: {
    label: "Grouped text diff",
    subtitle: "From `grouped_text_diff_uses_unified_hunk_header_and_trims_distant_equal_lines`.",
    body: [
      "diff -- /a.md",
      "--- a/a.md",
      "+++ b/a.md",
      "@@ -5,7 +5,7 @@",
      " shared line 05",
      " shared line 06",
      " shared line 07",
      "-before",
      "+after",
      " shared line 09",
      " shared line 10",
      " shared line 11",
      "",
    ].join("\n"),
  },

  legacyText: {
    label: "Legacy single-hunk text",
    subtitle: "In-memory code path. Lines numbered by the parser since the @@ header is bare.",
    body: [
      "diff -- /a.md",
      "--- a/a.md",
      "+++ b/a.md",
      "@@",
      " one",
      "-two",
      "+2",
      " three",
      "",
    ].join("\n"),
  },

  metadata: {
    label: "Metadata-only",
    subtitle: "Mode + uid + mime + custom_attrs delta. No content change.",
    body: [
      "diff -- /config.json",
      "metadata:",
      "- mode: 0644",
      "+ mode: 0755",
      "- uid: 1",
      "+ uid: 7",
      "- mime_type: text/plain",
      "+ mime_type: application/json",
      "- custom_attrs.owner: alice",
      "+ custom_attrs.owner: bob",
      "",
    ].join("\n"),
  },

  binary: {
    label: "Binary (durable summary)",
    subtitle: "Production path emits a structured summary; we render before/after object + size + mime.",
    body: [
      "diff -- /assets/logo.png",
      "reason: binary or non-UTF-8 content is not supported by text diff",
      "before: object=a1b2c3d4 size=42119 type=file mime=image/png",
      "after: object=e5f6a7b8 size=47220 type=file mime=image/png",
      "",
    ].join("\n"),
  },

  tooLarge: {
    label: "Diff too large",
    subtitle: "Past the 512 KiB rendered-diff limit. Render before/after sizes + a path to inspect.",
    body: [
      "diff -- /case-files/transcript.txt",
      "reason: text diff is too large to render",
      "before: object=11111111 size=2097152 type=file mime=text/plain",
      "after: object=22222222 size=2098000 type=file mime=text/plain",
      "",
    ].join("\n"),
  },

  kindChanged: {
    label: "Path kind changed",
    subtitle: "A path that was a directory became a file (or vice-versa).",
    body: [
      "diff -- /policy",
      "reason: path kind changed; text diff is not available",
      "before: object=<none> size=0 type=directory mime=<unset>",
      "after: object=deadbeef size=42 type=file mime=text/markdown",
      "",
    ].join("\n"),
  },

  empty: {
    label: "No changes",
    subtitle: "The `No changes.` sentinel from Rust → an empty-state card.",
    body: "No changes.\n",
  },
};

type FixtureKey = keyof typeof FIXTURES;

// ─────────────────────────────────────────────────────────────────────────────
// App
// ─────────────────────────────────────────────────────────────────────────────

export function SpikeApp() {
  const [active, setActive] = useState<FixtureKey>("multi");
  const [pasted, setPasted] = useState<string>("");

  const body = pasted.trim().length > 0 ? pasted : FIXTURES[active]!.body;
  const parsed = useMemo(() => parseDiff(body), [body]);

  return (
    <div className="grid h-full grid-cols-[280px_1fr] bg-stone-50">
      {/* Sidebar */}
      <aside className="flex flex-col border-r border-stone-200 bg-white">
        <header className="border-b border-stone-200 px-5 py-4">
          <div className="flex items-center gap-2">
            <BrandMark />
            <span className="text-[14px] font-semibold tracking-tight text-stone-900">stratum</span>
          </div>
          <div className="mt-1 font-mono text-[10.5px] uppercase tracking-wider text-stone-500">
            Diff spike · week 2
          </div>
        </header>

        <div className="border-b border-stone-200 px-4 py-4">
          <div className="mb-2 font-mono text-[10px] uppercase tracking-wider text-stone-500">
            Fixtures
          </div>
          <ul className="flex flex-col gap-0.5">
            {(Object.keys(FIXTURES) as FixtureKey[]).map((key) => (
              <li key={key}>
                <button
                  type="button"
                  onClick={() => {
                    setActive(key);
                    setPasted("");
                  }}
                  className={`w-full rounded-sm px-2 py-1.5 text-left text-[12.5px] transition-colors ${
                    active === key && pasted.length === 0
                      ? "bg-stone-100 text-stone-900"
                      : "text-stone-700 hover:bg-stone-50 hover:text-stone-900"
                  }`}
                  aria-pressed={active === key && pasted.length === 0}
                >
                  {FIXTURES[key]!.label}
                </button>
              </li>
            ))}
          </ul>
        </div>

        <div className="flex flex-1 flex-col gap-2 overflow-hidden px-4 py-4">
          <label
            htmlFor="paste-diff"
            className="font-mono text-[10px] uppercase tracking-wider text-stone-500"
          >
            Paste a real diff
          </label>
          <textarea
            id="paste-diff"
            value={pasted}
            onChange={(e) => setPasted(e.target.value)}
            placeholder="diff -- /your/file.md…"
            spellCheck={false}
            className="flex-1 resize-none rounded-sm border border-stone-200 bg-stone-50 px-3 py-2 font-mono text-[11px] leading-snug text-stone-800 outline-none focus:border-stone-400 focus:bg-white"
          />
          <p className="font-serif text-[12px] italic text-stone-500">
            Or pipe one in:{" "}
            <code className="not-italic font-mono text-[11px] text-stone-700">
              curl /vcs/diff
            </code>
          </p>
        </div>

        <footer className="border-t border-stone-200 px-5 py-3 font-mono text-[10.5px] text-stone-500">
          <a
            href="https://github.com/anthropics/stratum"
            className="underline-offset-4 hover:text-stone-900 hover:underline"
          >
            src/vcs/diff.rs
          </a>
          {" · "}
          <a href="#" className="underline-offset-4 hover:text-stone-900 hover:underline">
            roadmap
          </a>
        </footer>
      </aside>

      {/* Main */}
      <main className="flex flex-col overflow-hidden">
        <header className="flex items-center gap-4 border-b border-stone-200 bg-white px-8 py-4">
          <div className="flex-1 min-w-0">
            <h1 className="text-[18px] font-medium tracking-tight text-stone-900">
              {pasted.trim().length > 0 ? "Pasted diff" : FIXTURES[active]!.label}
            </h1>
            <p className="font-serif text-[13px] italic text-stone-500">
              {pasted.trim().length > 0
                ? "Live-parsed from your textarea."
                : FIXTURES[active]!.subtitle}
            </p>
          </div>
          <DiffStats parsed={parsed} />
        </header>

        <section className="flex-1 overflow-y-auto bg-stone-50 px-8 py-6">
          <DiffViewer fragments={parsed.fragments} isEmpty={parsed.isEmpty} />
        </section>

        <footer className="border-t border-stone-200 bg-white px-8 py-2.5 font-mono text-[10.5px] text-stone-500">
          {parsed.fragments.length} file{parsed.fragments.length === 1 ? "" : "s"} ·{" "}
          {parsed.fragments.map((f) => f.kind).join(" · ") || "—"}
        </footer>
      </main>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Small atoms
// ─────────────────────────────────────────────────────────────────────────────

function BrandMark() {
  return (
    <span className="inline-grid h-[18px] w-[18px] grid-cols-2 gap-[1.5px]" aria-hidden>
      <span className="rounded-[1.5px] bg-stone-900" />
      <span className="rounded-[1.5px] bg-orange-600" />
      <span className="rounded-[1.5px] bg-stone-900" />
      <span className="rounded-[1.5px] bg-stone-900 opacity-40" />
    </span>
  );
}

function DiffStats({ parsed }: { parsed: ReturnType<typeof parseDiff> }) {
  const totals = parsed.fragments.reduce(
    (acc, f) => {
      if (f.kind === "text-unified") {
        acc.added += f.added;
        acc.removed += f.removed;
      }
      return acc;
    },
    { added: 0, removed: 0 },
  );
  return (
    <div className="flex items-center gap-4 font-mono text-[11.5px] tabular-nums text-stone-600">
      <span>
        <span className="text-emerald-700">+{totals.added}</span>{" "}
        <span className="text-rose-700">−{totals.removed}</span>
      </span>
      <span className="text-stone-400">·</span>
      <span>{parsed.fragments.length} path{parsed.fragments.length === 1 ? "" : "s"}</span>
    </div>
  );
}
