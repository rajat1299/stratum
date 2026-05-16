/**
 * DiffViewer — renders the structured fragments produced by `parseDiff`.
 *
 * Five renderers, one per fragment kind. Each one earns its slot because
 * the API can emit each one. None of them fall through to a `<pre>`.
 *
 * This is a spike: styling is hand-rolled with Tailwind utility classes so we
 * can land it without taking a shadcn dep in week 2. The shape of the
 * components — props, slots, ARIA — is the part we keep when we re-skin against
 * design tokens in Phase A5.
 */

import { useMemo, useState } from "react";
import type {
  BinaryFragment,
  ContentSummary,
  DiffFragment,
  DiffHunk,
  DiffLine,
  KindChangedFragment,
  MetadataOnlyFragment,
  TextUnifiedFragment,
  TooLargeFragment,
  UnknownFragment,
} from "../lib/diff-parser.ts";
import { fragmentTotals, summariseFragmentKind } from "../lib/diff-parser.ts";

// ─────────────────────────────────────────────────────────────────────────────
// Top-level
// ─────────────────────────────────────────────────────────────────────────────

export interface DiffViewerProps {
  readonly fragments: readonly DiffFragment[];
  readonly isEmpty: boolean;
}

export function DiffViewer({ fragments, isEmpty }: DiffViewerProps) {
  if (isEmpty) {
    return (
      <div className="rounded-md border border-dashed border-stone-300 bg-stone-50 px-6 py-12 text-center text-stone-500">
        <p className="font-serif italic">No changes between these refs.</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      {fragments.map((fragment) => (
        <FileCard key={fragment.path} fragment={fragment} />
      ))}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// File card — one per fragment, collapsible
// ─────────────────────────────────────────────────────────────────────────────

function FileCard({ fragment }: { fragment: DiffFragment }) {
  const [open, setOpen] = useState(true);
  const { added, removed } = fragmentTotals(fragment);
  const kindLabel = summariseFragmentKind(fragment.kind);

  return (
    <section
      aria-labelledby={`diff-${fragment.path}`}
      className="overflow-hidden rounded-md border border-stone-200 bg-white shadow-sm"
    >
      <header className="flex items-center gap-3 border-b border-stone-200 bg-stone-50 px-3 py-2">
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          aria-expanded={open}
          aria-controls={`diff-body-${fragment.path}`}
          className="rounded-sm px-1 py-0.5 text-stone-500 hover:bg-stone-200"
        >
          <svg width="10" height="10" viewBox="0 0 16 16" aria-hidden style={{ transform: open ? "rotate(90deg)" : "none", transition: "transform 160ms ease" }}>
            <path d="M6 3l5 5-5 5" stroke="currentColor" strokeWidth={1.5} fill="none" strokeLinecap="round" />
          </svg>
        </button>
        <FileTypeBadge kind={fragment.kind} />
        <h3 id={`diff-${fragment.path}`} className="flex-1 truncate font-mono text-[12.5px] text-stone-900">
          {fragment.path}
        </h3>
        {fragment.kind === "text-unified" && (
          <span className="font-mono text-[11px] tabular-nums">
            <span className="text-emerald-700">+{added}</span>{" "}
            <span className="text-rose-700">−{removed}</span>
          </span>
        )}
        <span className="font-mono text-[10px] uppercase tracking-wider text-stone-500">{kindLabel}</span>
      </header>

      {open && (
        <div id={`diff-body-${fragment.path}`}>
          {fragment.kind === "text-unified" && <TextUnifiedView fragment={fragment} />}
          {fragment.kind === "metadata-only" && <MetadataOnlyView fragment={fragment} />}
          {fragment.kind === "binary" && <BinaryView fragment={fragment} />}
          {fragment.kind === "too-large" && <TooLargeView fragment={fragment} />}
          {fragment.kind === "kind-changed" && <KindChangedView fragment={fragment} />}
          {fragment.kind === "unknown" && <UnknownView fragment={fragment} />}
        </div>
      )}
    </section>
  );
}

function FileTypeBadge({ kind }: { kind: DiffFragment["kind"] }) {
  const color =
    kind === "text-unified" ? "bg-stone-200 text-stone-700"
    : kind === "metadata-only" ? "bg-violet-100 text-violet-800"
    : kind === "binary" ? "bg-amber-100 text-amber-800"
    : kind === "too-large" ? "bg-amber-100 text-amber-800"
    : kind === "kind-changed" ? "bg-rose-100 text-rose-800"
    : "bg-stone-200 text-stone-600";
  return (
    <span aria-hidden className={`h-4 w-4 rounded-sm ${color}`} />
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Renderers
// ─────────────────────────────────────────────────────────────────────────────

function TextUnifiedView({ fragment }: { fragment: TextUnifiedFragment }) {
  return (
    <div className="font-mono text-[12.5px] leading-relaxed">
      {fragment.hunks.map((hunk, i) => (
        <HunkView key={i} hunk={hunk} hunkIndex={i} />
      ))}
    </div>
  );
}

function HunkView({ hunk, hunkIndex }: { hunk: DiffHunk; hunkIndex: number }) {
  // Compute per-line numbers as we go. Lines that don't consume on a side
  // render that side's gutter blank.
  const numbered = useMemo(() => computeLineNumbers(hunk), [hunk]);

  return (
    <div className="border-t border-stone-200 first:border-t-0">
      <div className="bg-stone-50 px-3 py-1 font-mono text-[10.5px] text-stone-500">
        @@ -{hunk.beforeStart},{hunk.beforeCount} +{hunk.afterStart},{hunk.afterCount} @@{" "}
        <span className="text-stone-400">hunk {hunkIndex + 1}</span>
      </div>
      <table className="w-full border-collapse">
        <colgroup>
          <col style={{ width: 44 }} />
          <col style={{ width: 44 }} />
          <col style={{ width: 18 }} />
          <col />
        </colgroup>
        <tbody>
          {hunk.lines.map((line, i) => {
            const meta = numbered[i]!;
            const bg =
              line.type === "add" ? "bg-emerald-50"
              : line.type === "remove" ? "bg-rose-50"
              : "";
            const sign = line.type === "add" ? "+" : line.type === "remove" ? "−" : "";
            const signColor =
              line.type === "add" ? "text-emerald-700"
              : line.type === "remove" ? "text-rose-700"
              : "text-stone-400";
            return (
              <tr key={i} className={bg}>
                <td className="select-none border-r border-stone-200 px-2 text-right font-mono text-[10.5px] tabular-nums text-stone-400">
                  {meta.beforeNum ?? ""}
                </td>
                <td className="select-none border-r border-stone-200 px-2 text-right font-mono text-[10.5px] tabular-nums text-stone-400">
                  {meta.afterNum ?? ""}
                </td>
                <td className={`px-1 text-center font-mono text-[11px] ${signColor}`}>{sign}</td>
                <td className="px-2 font-mono text-[12.5px] text-stone-800 whitespace-pre-wrap break-words">
                  {line.text}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function computeLineNumbers(hunk: DiffHunk): { beforeNum: number | null; afterNum: number | null }[] {
  const out: { beforeNum: number | null; afterNum: number | null }[] = [];
  let b = hunk.beforeStart;
  let a = hunk.afterStart;
  for (const ln of hunk.lines) {
    if (ln.type === "add") {
      out.push({ beforeNum: null, afterNum: a });
      a++;
    } else if (ln.type === "remove") {
      out.push({ beforeNum: b, afterNum: null });
      b++;
    } else {
      out.push({ beforeNum: b, afterNum: a });
      b++;
      a++;
    }
  }
  return out;
}

// ── metadata-only ────────────────────────────────────────────────────────────
function MetadataOnlyView({ fragment }: { fragment: MetadataOnlyFragment }) {
  return (
    <dl className="divide-y divide-stone-100">
      {fragment.changes.map((c) => (
        <div key={c.field} className="grid grid-cols-[180px_1fr_1fr] gap-4 px-4 py-2.5 text-[12.5px]">
          <dt className="font-mono text-[11.5px] text-stone-500">{c.field}</dt>
          <dd className="font-mono text-rose-700">
            <span className="text-stone-400">−</span> {c.before}
          </dd>
          <dd className="font-mono text-emerald-700">
            <span className="text-stone-400">+</span> {c.after}
          </dd>
        </div>
      ))}
    </dl>
  );
}

// ── binary + too-large + kind-changed share a "summary card" look ────────────
function SummaryCard({
  reason,
  summary,
}: {
  readonly reason: string;
  readonly summary: ContentSummary | null;
}) {
  return (
    <div className="p-4 text-[13px]">
      <p className="mb-3 text-stone-700">{reason}</p>
      {summary && (
        <div className="grid grid-cols-2 gap-3 rounded-sm border border-stone-200 bg-stone-50 p-3 font-mono text-[11.5px]">
          <SummarySide title="Before" side={summary.before} />
          <SummarySide title="After" side={summary.after} />
        </div>
      )}
    </div>
  );
}

function SummarySide({ title, side }: { title: string; side: ContentSummary["before"] }) {
  return (
    <div>
      <div className="mb-1 text-[10px] uppercase tracking-wider text-stone-500">{title}</div>
      <dl className="space-y-0.5 text-stone-800">
        <Row k="type" v={side.type} />
        <Row k="size" v={formatBytes(side.size)} />
        <Row k="object" v={side.object ?? "—"} mono />
        <Row k="mime" v={side.mime ?? "—"} />
      </dl>
    </div>
  );
}

function Row({ k, v, mono }: { k: string; v: string; mono?: boolean }) {
  return (
    <div className="flex gap-2">
      <span className="w-12 text-stone-500">{k}</span>
      <span className={mono ? "truncate text-stone-700" : "text-stone-800"} title={v}>
        {v}
      </span>
    </div>
  );
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / 1024 / 1024).toFixed(2)} MiB`;
}

function BinaryView({ fragment }: { fragment: BinaryFragment }) {
  return (
    <SummaryCard
      reason="This path holds binary or non-UTF-8 content. Stratum stores the bytes and tracks the hash; we don't try to render a text diff for it."
      summary={fragment.summary}
    />
  );
}

function TooLargeView({ fragment }: { fragment: TooLargeFragment }) {
  return (
    <SummaryCard
      reason="The change is larger than the 512 KiB rendered-diff limit. Open the file at each ref to inspect."
      summary={fragment.summary}
    />
  );
}

function KindChangedView({ fragment }: { fragment: KindChangedFragment }) {
  return (
    <SummaryCard
      reason="The path changed kind (file ↔ directory ↔ symlink). Text diff doesn't apply."
      summary={fragment.summary}
    />
  );
}

function UnknownView({ fragment }: { fragment: UnknownFragment }) {
  return (
    <pre className="overflow-auto bg-stone-50 p-4 font-mono text-[12px] text-stone-700">
      {fragment.raw}
    </pre>
  );
}
